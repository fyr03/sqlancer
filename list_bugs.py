#!/usr/bin/env python3
"""
SQLancer Subset Oracle — 详细 Bug 列表
用法:
  python3 list_bugs.py [log目录] [选项]

选项:
  --save FILE        保存到文件（默认只打印到终端）
  --workers N        并行进程数（默认 8）
  --type TYPE        只看某类 violation（COUNT/MAX/MIN/SELECT/EXISTS）
  --unique           去重，同一 Q1 只显示一次
  --limit N          最多显示 N 条（默认全部）
"""

import os
import re
import sys
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed

# ── 正则 ─────────────────────────────────────────────────────────────────────
RE_VIOLATION = re.compile(
    r'java\.lang\.AssertionError:\s*'
    r'(COUNT|EXISTS|MAX|MIN|SELECT|COUNT_DISTINCT|IN_SUBQUERY)'
    r' subset violation(.*?)(?=java\.lang\.|^\-\-\s*Time:|\Z)',
    re.DOTALL | re.MULTILINE
)
RE_Q1       = re.compile(r'Q1:\s*(.+)')
RE_Q2       = re.compile(r'Q2:\s*(.+)')
RE_MISSING  = re.compile(r'Missing rows:\s*(\[[^\]]*\])')
RE_SEED     = re.compile(r'seed value:\s*(\d+)')
RE_DB       = re.compile(r'-- Database:\s*(\S+)')
RE_TIME     = re.compile(r'-- Time:\s*(.+)')
RE_VERSION  = re.compile(r'-- Database version:\s*(.+)')
RE_DDL      = re.compile(r'(CREATE TABLE[^;]+;)', re.IGNORECASE)

# ── 单文件处理 ───────────────────────────────────────────────────────────────
def process_file(filepath):
    bugs = []
    try:
        with open(filepath, 'r', errors='replace') as f:
            content = f.read()
        if 'violation' not in content:
            return bugs

        seed    = (RE_SEED.search(content)    or type('', (), {'group': lambda s,x: 'unknown'})()).group(1)
        db      = (RE_DB.search(content)      or type('', (), {'group': lambda s,x: os.path.basename(filepath)})()).group(1)
        ts      = (RE_TIME.search(content)    or type('', (), {'group': lambda s,x: ''})()).group(1).strip()
        version = (RE_VERSION.search(content) or type('', (), {'group': lambda s,x: ''})()).group(1).strip()

        # 提取建表语句（复现时有用）
        ddl_stmts = RE_DDL.findall(content)
        ddl = '\n'.join(ddl_stmts[:6])  # 最多保留前6条，避免太长

        for m in RE_VIOLATION.finditer(content):
            vtype   = m.group(1)
            block   = m.group(0)

            q1_m      = RE_Q1.search(block)
            q2_m      = RE_Q2.search(block)
            missing_m = RE_MISSING.search(block)

            q1      = q1_m.group(1).strip()      if q1_m      else ''
            q2      = q2_m.group(1).strip()      if q2_m      else ''
            missing = missing_m.group(1).strip() if missing_m else ''

            # 提取第一行作为简短消息
            first_line = block.split('\n')[0].replace('java.lang.AssertionError:', '').strip()

            bugs.append({
                'file'    : filepath,
                'db'      : db,
                'seed'    : seed,
                'time'    : ts,
                'version' : version,
                'type'    : vtype,
                'message' : first_line,
                'q1'      : q1,
                'q2'      : q2,
                'missing' : missing,
                'ddl'     : ddl,
            })
    except Exception:
        pass
    return bugs


# ── 并行扫描 ─────────────────────────────────────────────────────────────────
def scan(log_dir, workers=8):
    files = []
    for root, _, fnames in os.walk(log_dir):
        for fname in fnames:
            if fname.endswith('.log'):
                files.append(os.path.join(root, fname))

    total = len(files)
    print(f'扫描目录: {log_dir}')
    print(f'共 {total:,} 个 .log 文件，使用 {workers} 个进程...\n', flush=True)

    all_bugs = []
    done = 0
    with ProcessPoolExecutor(max_workers=workers) as exe:
        futures = {exe.submit(process_file, f): f for f in files}
        for future in as_completed(futures):
            result = future.result()
            all_bugs.extend(result)
            done += 1
            if done % 10000 == 0 or done == total:
                print(f'  {done:,}/{total:,} 文件已处理  |  发现 bug: {len(all_bugs):,}',
                      flush=True)

    # 按发现时间排序（时间为空的排最后）
    all_bugs.sort(key=lambda b: b['time'] or 'zzzz')
    return all_bugs


# ── 格式化单条 bug ────────────────────────────────────────────────────────────
def format_bug(idx, b):
    div = '─' * 72
    lines = [
        '',
        f'┌─ Bug #{idx}  [{b["type"]}] ' + '─' * max(0, 55 - len(b["type"])),
        f'│  时间     : {b["time"]}',
        f'│  数据库   : {b["db"]}',
        f'│  Seed     : {b["seed"]}',
        f'│  版本     : {b["version"]}',
        f'│  文件     : {b["file"]}',
        f'├─ 违规信息',
        f'│  {b["message"]}',
    ]
    if b['missing']:
        lines.append(f'│  缺失行   : {b["missing"]}')
    lines += [
        f'├─ Q1（在 S1 上执行）',
        f'│  {b["q1"]}',
        f'├─ Q2（在 S2 上执行）',
        f'│  {b["q2"]}',
    ]
    if b['ddl']:
        lines.append(f'├─ 相关 DDL')
        for ddl_line in b['ddl'].split('\n'):
            lines.append(f'│  {ddl_line}')
    lines.append('└' + '─' * 71)
    return '\n'.join(lines)


# ── 主报告 ───────────────────────────────────────────────────────────────────
def report(bugs, args):
    # 过滤
    if args.type:
        bugs = [b for b in bugs if b['type'].upper() == args.type.upper()]

    # 去重
    if args.unique:
        seen, deduped = set(), []
        for b in bugs:
            key = b['q1'][:120]
            if key not in seen:
                seen.add(key)
                deduped.append(b)
        removed = len(bugs) - len(deduped)
        bugs = deduped
        print(f'去重后剩余 {len(bugs)} 条（去掉 {removed} 条重复）\n')

    # 限制条数
    total_found = len(bugs)
    if args.limit and len(bugs) > args.limit:
        bugs = bugs[:args.limit]
        print(f'共 {total_found} 条，只显示前 {args.limit} 条\n')

    # 汇总统计
    from collections import Counter
    type_counts = Counter(b['type'] for b in bugs)

    header_lines = [
        '═' * 72,
        '  SQLancer Subset Oracle — 完整 Bug 列表',
        '═' * 72,
        f'  总计: {total_found} 条 bug',
        '',
        '  按类型:',
    ]
    for vtype, cnt in type_counts.most_common():
        header_lines.append(f'    {vtype:<25} {cnt:>6} 条')
    header_lines += ['', '═' * 72]

    output_parts = ['\n'.join(header_lines)]

    # 逐条详情
    for idx, b in enumerate(bugs, 1):
        output_parts.append(format_bug(idx, b))

    output_parts.append(f'\n共列出 {len(bugs)} 条 bug。')
    full_output = '\n'.join(output_parts)

    if args.save:
        with open(args.save, 'w') as f:
            f.write(full_output)
        print(full_output)
        print(f'\n✓ 已保存到: {args.save}')
    else:
        print(full_output)


# ── 入口 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='列出 SQLancer subset oracle 的所有 bug 详情',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('log_dir', nargs='?', default='./logs/mysql',
                        help='日志目录（默认 ./logs/mysql）')
    parser.add_argument('--save', metavar='FILE',
                        help='保存报告到文件')
    parser.add_argument('--workers', type=int, default=8,
                        help='并行进程数（默认 8）')
    parser.add_argument('--type', metavar='TYPE',
                        help='只看某类：COUNT / MAX / MIN / SELECT / EXISTS')
    parser.add_argument('--unique', action='store_true',
                        help='按 Q1 去重，同一查询模式只显示一次')
    parser.add_argument('--limit', type=int, default=None,
                        help='最多显示 N 条')
    args = parser.parse_args()

    if not os.path.isdir(args.log_dir):
        print(f'错误: 目录不存在 → {args.log_dir}')
        sys.exit(1)

    bugs = scan(args.log_dir, workers=args.workers)

    if not bugs:
        print('未发现任何 subset violation，日志干净！')
        return

    report(bugs, args)


if __name__ == '__main__':
    main()