#!/usr/bin/env python3
"""
SQLancer Subset Oracle — 详细 Bug 列表
支持 Oracle1/Oracle2（旧格式）和 Oracle3（新格式）的日志混合扫描。

用法:
  python3 list_bugs.py [log目录] [选项]

选项:
  --save FILE        保存到文件（默认只打印到终端）
  --workers N        并行进程数（默认 8）
  --oracle N         只看某个 oracle（1 / 2 / 3，默认全部）
  --type TYPE        只看某类 violation（见下方说明）
  --unique           去重，同一查询只显示一次
  --limit N          最多显示 N 条（默认全部）
  --plan-change      只显示伴随计划切换的 Oracle3 bug

Oracle3 violation 类型（--type 参数可用值）:
  COUNT            COUNT(*) 单调性违反
  MAX              MAX 单调性违反
  MIN              MIN 单调性违反
  ROW_SET          行集子集关系违反
  COUNT_DISTINCT   COUNT DISTINCT 单调性违反
  GROUP_BY_COUNT   GROUP BY + COUNT 单调性违反
  GROUP_BY_MAX     GROUP BY + MAX 单调性违反
  ORDER_ASC        ORDER BY ASC LIMIT 1（等价 MIN）违反
  ORDER_DESC       ORDER BY DESC LIMIT 1（等价 MAX）违反

Oracle1/2 violation 类型:
  COUNT / EXISTS / MAX / MIN / SELECT / COUNT_DISTINCT / IN_SUBQUERY
"""

import os
import re
import sys
import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

# ═══════════════════════════════════════════════════════════════════════
#  正则：Oracle1 / Oracle2（旧格式）
# ═══════════════════════════════════════════════════════════════════════
# 匹配 "COUNT subset violation" / "MAX subset violation" 等
RE_OLD_VIOLATION = re.compile(
    r'java\.lang\.AssertionError:\s*'
    r'(COUNT|EXISTS|MAX|MIN|SELECT|COUNT_DISTINCT|IN_SUBQUERY)'
    r' subset violation(.*?)(?=java\.lang\.|^--\s*Time:|\Z)',
    re.DOTALL | re.MULTILINE
)
RE_OLD_Q1      = re.compile(r'\bQ1:\s*(.+)')
RE_OLD_Q2      = re.compile(r'\bQ2:\s*(.+)')
RE_OLD_MISSING = re.compile(r'Missing rows:\s*(\[[^\]]*\])')

# ═══════════════════════════════════════════════════════════════════════
#  正则：Oracle3（新格式）
# ═══════════════════════════════════════════════════════════════════════
# 匹配 "Oracle3 COUNT violation" / "Oracle3 GROUP BY MAX violation" 等
# 捕获组1：规范化后的类型标签（见 _O3_TYPE_MAP）
_O3_PATTERNS = [
    # (regex_pattern, normalized_type)
    (r'Oracle3 COUNT violation',               'COUNT'),
    (r'Oracle3 MAX violation on [^:]+',         'MAX'),
    (r'Oracle3 MIN violation on [^:]+',         'MIN'),
    (r'Oracle3 row-set subset violation',       'ROW_SET'),
    (r'Oracle3 COUNT DISTINCT violation',       'COUNT_DISTINCT'),
    (r'Oracle3 GROUP BY COUNT violation for key [^:]+', 'GROUP_BY_COUNT'),
    (r'Oracle3 GROUP BY MAX violation for key [^:]+',   'GROUP_BY_MAX'),
    (r'Oracle3 ORDER BY ASC LIMIT 1 violation', 'ORDER_ASC'),
    (r'Oracle3 ORDER BY DESC LIMIT 1 violation','ORDER_DESC'),
]

# 合并成一个 alternation，捕获整个 first-line 作为 group(1)
_O3_ALT = '|'.join(p for p, _ in _O3_PATTERNS)
RE_O3_VIOLATION = re.compile(
    r'java\.lang\.AssertionError:\s*'
    r'(' + _O3_ALT + r')'
    r'(.*?)(?=java\.lang\.|^--\s*Time:|\Z)',
    re.DOTALL | re.MULTILINE
)

# Oracle3 字段提取
RE_O3_QUERY   = re.compile(r'^\s*Query:\s*(.+)',  re.MULTILINE)
RE_O3_PLAN1   = re.compile(r'^\s*Plan1:\s*(.+)',  re.MULTILINE)
RE_O3_PLAN2   = re.compile(r'^\s*Plan2:\s*(.+)',  re.MULTILINE)
RE_O3_MISSING = re.compile(r'Missing row digests:\s*(\{[^}]*\}|\[[^\]]*\])')
RE_O3_KEY     = re.compile(r'for key\s+(\S+):')      # GROUP BY violation 的 group key
RE_O3_COL     = re.compile(r'violation on\s+(\S+):')  # MAX/MIN violation 的列名

# ═══════════════════════════════════════════════════════════════════════
#  公共正则：日志元数据
# ═══════════════════════════════════════════════════════════════════════
RE_SEED    = re.compile(r'seed value:\s*(\d+)')
RE_DB      = re.compile(r'^\s*Database:\s*(\S+)',         re.MULTILINE)
RE_TIME    = re.compile(r'^\s*Time:\s*(.+)',              re.MULTILINE)
RE_VERSION = re.compile(r'^\s*Database version:\s*(.+)', re.MULTILINE)
RE_DDL     = re.compile(r'(CREATE TABLE[^;]+;)', re.IGNORECASE | re.DOTALL)


def _normalize_o3_type(first_line: str) -> str:
    """根据 AssertionError 第一行内容返回规范类型标签。"""
    for pattern, label in _O3_PATTERNS:
        if re.search(pattern, first_line):
            return label
    return 'UNKNOWN'


def _plan_changed(plan1: str, plan2: str) -> bool:
    """简单判断 Plan1 和 Plan2 是否代表不同执行路径（key 或 type 不同）。"""
    if not plan1 or not plan2:
        return False
    # 提取 type= 和 key= 值
    def extract(s, field):
        m = re.search(field + r'=([^;]+)', s)
        return m.group(1).strip() if m else ''
    return (extract(plan1, 'type') != extract(plan2, 'type') or
            extract(plan1, 'key')  != extract(plan2, 'key'))


# ═══════════════════════════════════════════════════════════════════════
#  单文件处理
# ═══════════════════════════════════════════════════════════════════════
def process_file(filepath):
    bugs = []
    try:
        with open(filepath, 'r', errors='replace') as f:
            raw = f.read()
        content = re.sub(r'^--', '', raw, flags=re.MULTILINE)
        if 'violation' not in content:
            return bugs

        # ── 元数据 ──────────────────────────────────────────────────
        def get(rx, default=''):
            m = rx.search(content)
            return m.group(1).strip() if m else default

        seed    = get(RE_SEED,    'unknown')
        db      = get(RE_DB,      os.path.basename(filepath))
        ts      = get(RE_TIME,    '')
        version = get(RE_VERSION, '')

        ddl_stmts = RE_DDL.findall(content)
        ddl = '\n'.join(ddl_stmts[:6])

        # ── Oracle1 / Oracle2 违规 ───────────────────────────────────
        for m in RE_OLD_VIOLATION.finditer(content):
            vtype = m.group(1)
            block = m.group(0)
            first_line = block.split('\n')[0].replace('java.lang.AssertionError:', '').strip()

            q1_m = RE_OLD_Q1.search(block)
            q2_m = RE_OLD_Q2.search(block)
            miss_m = RE_OLD_MISSING.search(block)

            bugs.append({
                'oracle'       : 'Oracle1/2',
                'file'         : filepath,
                'db'           : db,
                'seed'         : seed,
                'time'         : ts,
                'version'      : version,
                'type'         : vtype,
                'message'      : first_line,
                # Oracle1/2 专用字段
                'q1'           : q1_m.group(1).strip() if q1_m else '',
                'q2'           : q2_m.group(1).strip() if q2_m else '',
                'missing'      : miss_m.group(1).strip() if miss_m else '',
                # Oracle3 专用字段（空）
                'query'        : '',
                'plan1'        : '',
                'plan2'        : '',
                'plan_changed' : False,
                'group_key'    : '',
                'agg_col'      : '',
                'ddl'          : ddl,
            })

        # ── Oracle3 违规 ─────────────────────────────────────────────
        for m in RE_O3_VIOLATION.finditer(content):
            first_line_raw = m.group(1)   # 完整的 AssertionError 首行（含类型描述）
            block          = m.group(0)

            vtype    = _normalize_o3_type(first_line_raw)
            full_msg = ('java.lang.AssertionError: ' + first_line_raw).strip()

            query_m = RE_O3_QUERY.search(block)
            plan1_m = RE_O3_PLAN1.search(block)
            plan2_m = RE_O3_PLAN2.search(block)
            miss_m  = RE_O3_MISSING.search(block)
            key_m   = RE_O3_KEY.search(first_line_raw)
            col_m   = RE_O3_COL.search(first_line_raw)

            plan1 = plan1_m.group(1).strip() if plan1_m else ''
            plan2 = plan2_m.group(1).strip() if plan2_m else ''

            bugs.append({
                'oracle'       : 'Oracle3',
                'file'         : filepath,
                'db'           : db,
                'seed'         : seed,
                'time'         : ts,
                'version'      : version,
                'type'         : vtype,
                'message'      : full_msg,
                # Oracle1/2 专用字段（空）
                'q1'           : '',
                'q2'           : '',
                'missing'      : miss_m.group(1).strip() if miss_m else '',
                # Oracle3 专用字段
                'query'        : query_m.group(1).strip() if query_m else '',
                'plan1'        : plan1,
                'plan2'        : plan2,
                'plan_changed' : _plan_changed(plan1, plan2),
                'group_key'    : key_m.group(1).strip() if key_m else '',
                'agg_col'      : col_m.group(1).strip() if col_m else '',
                'ddl'          : ddl,
            })

    except Exception:
        pass
    return bugs


# ═══════════════════════════════════════════════════════════════════════
#  并行扫描
# ═══════════════════════════════════════════════════════════════════════
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
            all_bugs.extend(future.result())
            done += 1
            if done % 10000 == 0 or done == total:
                print(f'  {done:,}/{total:,} 文件已处理  |  发现 bug: {len(all_bugs):,}',
                      flush=True)

    all_bugs.sort(key=lambda b: b['time'] or 'zzzz')
    return all_bugs


# ═══════════════════════════════════════════════════════════════════════
#  格式化单条 bug
# ═══════════════════════════════════════════════════════════════════════
def format_bug(idx, b):
    oracle_tag = b['oracle']
    type_tag   = b['type']
    plan_tag   = ' [PLAN-CHANGED]' if b.get('plan_changed') else ''

    lines = [
        '',
        f'┌─ Bug #{idx}  [{oracle_tag}][{type_tag}]{plan_tag} ' +
        '─' * max(0, 45 - len(oracle_tag) - len(type_tag) - len(plan_tag)),
        f'│  时间     : {b["time"]}',
        f'│  数据库   : {b["db"]}',
        f'│  Seed     : {b["seed"]}',
        f'│  版本     : {b["version"]}',
        f'│  文件     : {b["file"]}',
        f'├─ 违规信息',
        f'│  {b["message"]}',
    ]

    if b.get('missing'):
        lines.append(f'│  缺失行   : {b["missing"]}')
    if b.get('group_key'):
        lines.append(f'│  Group Key: {b["group_key"]}')
    if b.get('agg_col'):
        lines.append(f'│  聚合列   : {b["agg_col"]}')

    # Oracle1/2：显示 Q1 / Q2
    if b['oracle'] == 'Oracle1/2':
        lines += [
            f'├─ Q1（在 S1 上执行）',
            f'│  {b["q1"]}',
            f'├─ Q2（在 S2 上执行）',
            f'│  {b["q2"]}',
        ]
    # Oracle3：显示 Query / Plan1 / Plan2
    else:
        if b.get('query'):
            lines += [
                f'├─ Query（S1 和 S2 共用同一查询）',
                f'│  {b["query"]}',
            ]
        if b.get('plan1') or b.get('plan2'):
            lines.append(f'├─ 执行计划对比')
            if b.get('plan1'):
                lines.append(f'│  Plan1 (S1): {b["plan1"]}')
            if b.get('plan2'):
                lines.append(f'│  Plan2 (S2): {b["plan2"]}')

    if b.get('ddl'):
        lines.append(f'├─ 相关 DDL')
        for ddl_line in b['ddl'].split('\n'):
            lines.append(f'│  {ddl_line}')

    lines.append('└' + '─' * 71)
    return '\n'.join(lines)


# ═══════════════════════════════════════════════════════════════════════
#  主报告
# ═══════════════════════════════════════════════════════════════════════
def report(bugs, args):
    # ── 过滤 ────────────────────────────────────────────────────────
    if args.oracle:
        oracle_map = {'1': 'Oracle1/2', '2': 'Oracle1/2', '3': 'Oracle3'}
        target = oracle_map.get(str(args.oracle))
        if target:
            bugs = [b for b in bugs if b['oracle'] == target]

    if args.type:
        bugs = [b for b in bugs if b['type'].upper() == args.type.upper()]

    if args.plan_change:
        bugs = [b for b in bugs if b.get('plan_changed')]

    # ── 去重 ────────────────────────────────────────────────────────
    if args.unique:
        seen, deduped = set(), []
        for b in bugs:
            # Oracle3 用 query 去重，Oracle1/2 用 q1 去重
            key = (b['query'] or b['q1'])[:120]
            if key not in seen:
                seen.add(key)
                deduped.append(b)
        removed = len(bugs) - len(deduped)
        bugs = deduped
        print(f'去重后剩余 {len(bugs)} 条（去掉 {removed} 条重复）\n')

    # ── 限制条数 ─────────────────────────────────────────────────────
    total_found = len(bugs)
    if args.limit and len(bugs) > args.limit:
        bugs = bugs[:args.limit]

    # ── 汇总统计 ─────────────────────────────────────────────────────
    oracle_counts = Counter(b['oracle'] for b in bugs)
    type_counts   = Counter(f'[{b["oracle"]}] {b["type"]}' for b in bugs)
    plan_changed_count = sum(1 for b in bugs if b.get('plan_changed'))

    header_lines = [
        '═' * 72,
        '  SQLancer Subset Oracle — 完整 Bug 列表',
        '═' * 72,
        f'  总计: {total_found} 条 bug',
        '',
        '  按 Oracle:',
    ]
    for oracle, cnt in oracle_counts.most_common():
        header_lines.append(f'    {oracle:<20} {cnt:>6} 条')

    header_lines += ['', '  按类型:']
    for label, cnt in type_counts.most_common():
        header_lines.append(f'    {label:<40} {cnt:>6} 条')

    if plan_changed_count:
        header_lines += [
            '',
            f'  其中伴随计划切换: {plan_changed_count} 条'
            f'（占 Oracle3 bug 的 {plan_changed_count * 100 // max(1, oracle_counts.get("Oracle3", 1))}%）',
        ]

    if args.limit and total_found > args.limit:
        header_lines.append(f'\n  （共 {total_found} 条，只显示前 {args.limit} 条）')

    header_lines += ['', '═' * 72]

    output_parts = ['\n'.join(header_lines)]
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


# ═══════════════════════════════════════════════════════════════════════
#  入口
# ═══════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description='列出 SQLancer subset oracle（Oracle1/2/3）的所有 bug 详情',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('log_dir', nargs='?', default='./logs/mysql',
                        help='日志目录（默认 ./logs/mysql）')
    parser.add_argument('--save', metavar='FILE',
                        help='保存报告到文件')
    parser.add_argument('--workers', type=int, default=8,
                        help='并行进程数（默认 8）')
    parser.add_argument('--oracle', metavar='N', choices=['1', '2', '3'],
                        help='只看某个 oracle（1/2/3）')
    parser.add_argument('--type', metavar='TYPE',
                        help='只看某类 violation（大小写不敏感）')
    parser.add_argument('--unique', action='store_true',
                        help='按查询去重，同一查询模式只显示一次')
    parser.add_argument('--limit', type=int, default=None,
                        help='最多显示 N 条')
    parser.add_argument('--plan-change', dest='plan_change', action='store_true',
                        help='只显示伴随计划切换的 Oracle3 bug（需要 Plan1/Plan2 字段）')
    args = parser.parse_args()

    if not os.path.isdir(args.log_dir):
        print(f'错误: 目录不存在 → {args.log_dir}')
        sys.exit(1)

    bugs = scan(args.log_dir, workers=args.workers)

    if not bugs:
        print('未发现任何 violation，日志干净！')
        return

    report(bugs, args)


if __name__ == '__main__':
    main()