#!/usr/bin/env python3
"""
SQLancer Subset Oracle — Bug Report Analyzer
用法: python3 analyze_bugs.py [log目录] [--save bugs.txt]
默认目录: ./logs/mysql
"""

import os
import re
import sys
import argparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

# ── 配置 ────────────────────────────────────────────────────────────────────
VIOLATION_PATTERN = re.compile(
    r'(COUNT|EXISTS|MAX|MIN|SELECT|COUNT_DISTINCT|IN_SUBQUERY) subset violation[^\n]*'
)
# 提取 Q1/Q2 所在的行
Q1_PATTERN = re.compile(r'Q1:\s*(.+)')
Q2_PATTERN = re.compile(r'Q2:\s*(.+)')
MISSING_PATTERN = re.compile(r'Missing rows:\s*\[([^\]]*)\]')
SEED_PATTERN = re.compile(r'seed value:\s*(\d+)')
DB_PATTERN = re.compile(r'Database:\s*(\S+)')

# ── 单文件处理（在子进程里跑）───────────────────────────────────────────────
def process_file(filepath):
    """返回该文件里发现的所有 bug，每个 bug 是一个 dict"""
    bugs = []
    try:
        with open(filepath, 'r', errors='replace') as f:
            content = f.read()

        if 'violation' not in content:
            return bugs  # 快速跳过无 bug 文件

        # 提取元信息
        seed_m = SEED_PATTERN.search(content)
        db_m = DB_PATTERN.search(content)
        seed = seed_m.group(1) if seed_m else 'unknown'
        db = db_m.group(1) if db_m else os.path.basename(filepath)

        # 找所有 violation 块（每个 AssertionError 是一个块）
        blocks = content.split('java.lang.AssertionError:')
        for block in blocks[1:]:  # 第0个是 header，跳过
            lines = block.split('\n')
            first_line = lines[0].strip()

            vtype_m = re.match(r'(COUNT|EXISTS|MAX|MIN|SELECT|COUNT_DISTINCT|IN_SUBQUERY)'
                               r' subset violation', first_line)
            if not vtype_m:
                continue
            vtype = vtype_m.group(1)

            q1_m = Q1_PATTERN.search(block)
            q2_m = Q2_PATTERN.search(block)
            missing_m = MISSING_PATTERN.search(block)

            bugs.append({
                'file': filepath,
                'db': db,
                'seed': seed,
                'type': vtype,
                'message': first_line[:200],
                'q1': q1_m.group(1).strip()[:300] if q1_m else '',
                'q2': q2_m.group(1).strip()[:300] if q2_m else '',
                'missing': missing_m.group(1).strip()[:100] if missing_m else '',
            })
    except Exception as e:
        pass  # 损坏文件直接跳过
    return bugs


# ── 并行扫描目录 ─────────────────────────────────────────────────────────────
def scan_directory(log_dir, workers=8):
    files = []
    for root, _, fnames in os.walk(log_dir):
        for fname in fnames:
            if fname.endswith('.log'):
                files.append(os.path.join(root, fname))

    total = len(files)
    print(f"共发现 {total:,} 个 .log 文件，开始用 {workers} 个进程扫描...\n")

    all_bugs = []
    done = 0
    with ProcessPoolExecutor(max_workers=workers) as exe:
        futures = {exe.submit(process_file, f): f for f in files}
        for future in as_completed(futures):
            bugs = future.result()
            all_bugs.extend(bugs)
            done += 1
            if done % 10000 == 0 or done == total:
                print(f"  进度: {done:,}/{total:,}  已发现 bug: {len(all_bugs):,}", flush=True)

    return all_bugs


# ── 报告输出 ─────────────────────────────────────────────────────────────────
def print_report(bugs, save_path=None):
    lines = []
    sep = '═' * 70

    lines.append(sep)
    lines.append(f'  SQLancer Subset Oracle — Bug 分析报告')
    lines.append(sep)
    lines.append(f'  总 bug 数: {len(bugs)}')
    lines.append('')

    # 按类型统计
    by_type = defaultdict(list)
    for b in bugs:
        by_type[b['type']].append(b)

    lines.append('── 按 violation 类型统计 ──')
    for vtype, blist in sorted(by_type.items(), key=lambda x: -len(x[1])):
        lines.append(f'  {vtype:<20} {len(blist):>6} 次')
    lines.append('')

    # 按 Q1 去重（同一个查询模式触发多次只算一个）
    seen_q1 = set()
    unique_bugs = []
    for b in bugs:
        key = b['q1'][:150]
        if key not in seen_q1:
            seen_q1.add(key)
            unique_bugs.append(b)

    lines.append(f'── 去重后独立 bug 数（按 Q1 去重）: {len(unique_bugs)} ──')
    lines.append('')

    # 每类各打印最多3个典型案例
    lines.append('── 典型案例（每类最多3个）──')
    for vtype, blist in sorted(by_type.items(), key=lambda x: -len(x[1])):
        lines.append(f'\n【{vtype}】共 {len(blist)} 次')
        shown = set()
        count = 0
        for b in blist:
            key = b['q1'][:100]
            if key in shown:
                continue
            shown.add(key)
            count += 1
            lines.append(f'  文件   : {b["file"]}')
            lines.append(f'  DB     : {b["db"]}  seed={b["seed"]}')
            lines.append(f'  消息   : {b["message"]}')
            if b['q1']:
                lines.append(f'  Q1     : {b["q1"]}')
            if b['q2']:
                lines.append(f'  Q2     : {b["q2"]}')
            if b['missing']:
                lines.append(f'  缺失行 : [{b["missing"]}]')
            lines.append('')
            if count >= 3:
                break

    lines.append(sep)

    output = '\n'.join(lines)
    print(output)

    if save_path:
        with open(save_path, 'w') as f:
            f.write(output)
        print(f'\n报告已保存到: {save_path}')


# ── 入口 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description='分析 SQLancer subset oracle 日志')
    parser.add_argument('log_dir', nargs='?', default='./logs/mysql',
                        help='日志目录（默认 ./logs/mysql）')
    parser.add_argument('--save', metavar='FILE',
                        help='把报告保存到文件')
    parser.add_argument('--workers', type=int, default=8,
                        help='并行进程数（默认 8）')
    args = parser.parse_args()

    if not os.path.isdir(args.log_dir):
        print(f'错误: 目录不存在: {args.log_dir}')
        sys.exit(1)

    bugs = scan_directory(args.log_dir, workers=args.workers)
    print_report(bugs, save_path=args.save)


if __name__ == '__main__':
    main()