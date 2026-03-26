#!/usr/bin/env python3
"""
Extract Oracle3 bug reports from SQLancer log files.

Usage:
  python3 list_bugs_3.py [log_dir]

Examples:
  python3 list_bugs_3.py ./logs/mysql
  python3 list_bugs_3.py ./logs/mysql --type ROW_SET --unique --save oracle3_bugs.txt
"""

import argparse
import os
import re
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed


RE_ASSERTION_BLOCK = re.compile(
    r"java\.lang\.AssertionError:\s*"
    r"(Oracle3 (?:COUNT|MAX|MIN) violation.*?|Oracle3 row-set subset violation.*?)"
    r"(?=java\.lang\.|^\-\-\s*Time:|\Z)",
    re.DOTALL | re.MULTILINE,
)
RE_QUERY = re.compile(r"^\s*Query:\s*(.+)$", re.MULTILINE)
RE_PLAN1 = re.compile(r"^\s*Plan1:\s*(.+)$", re.MULTILINE)
RE_PLAN2 = re.compile(r"^\s*Plan2:\s*(.+)$", re.MULTILINE)
RE_MISSING = re.compile(r"^\s*Missing row digests:\s*(.+)$", re.MULTILINE)
RE_SEED = re.compile(r"seed value:\s*(\d+)")
RE_DB = re.compile(r"-- Database:\s*(\S+)")
RE_TIME = re.compile(r"-- Time:\s*(.+)")
RE_VERSION = re.compile(r"-- Database version:\s*(.+)")
RE_DDL = re.compile(r"(CREATE TABLE[^;]+;)", re.IGNORECASE)


def classify_violation(message: str) -> str:
    if message.startswith("Oracle3 COUNT violation:"):
        return "COUNT"
    if message.startswith("Oracle3 MAX violation on"):
        return "MAX"
    if message.startswith("Oracle3 MIN violation on"):
        return "MIN"
    if message.startswith("Oracle3 row-set subset violation:"):
        return "ROW_SET"
    return "UNKNOWN"


def process_file(filepath: str):
    bugs = []
    try:
        with open(filepath, "r", errors="replace") as f:
            content = f.read()
    except Exception:
        return bugs

    if "Oracle3 " not in content:
        return bugs

    seed_match = RE_SEED.search(content)
    db_match = RE_DB.search(content)
    time_match = RE_TIME.search(content)
    version_match = RE_VERSION.search(content)
    ddl_matches = RE_DDL.findall(content)

    seed = seed_match.group(1) if seed_match else "unknown"
    db = db_match.group(1) if db_match else os.path.basename(filepath)
    timestamp = time_match.group(1).strip() if time_match else ""
    version = version_match.group(1).strip() if version_match else ""
    ddl = "\n".join(ddl_matches[:6])

    for match in RE_ASSERTION_BLOCK.finditer(content):
        block = match.group(1).strip()
        first_line = block.splitlines()[0].strip()

        query_match = RE_QUERY.search(block)
        plan1_match = RE_PLAN1.search(block)
        plan2_match = RE_PLAN2.search(block)
        missing_match = RE_MISSING.search(block)

        bugs.append(
            {
                "file": filepath,
                "db": db,
                "seed": seed,
                "time": timestamp,
                "version": version,
                "type": classify_violation(first_line),
                "message": first_line,
                "query": query_match.group(1).strip() if query_match else "",
                "plan1": plan1_match.group(1).strip() if plan1_match else "",
                "plan2": plan2_match.group(1).strip() if plan2_match else "",
                "missing": missing_match.group(1).strip() if missing_match else "",
                "ddl": ddl,
            }
        )

    return bugs


def scan_logs(log_dir: str, workers: int):
    files = []
    for root, _, filenames in os.walk(log_dir):
        for name in filenames:
            if name.endswith(".log"):
                files.append(os.path.join(root, name))

    total = len(files)
    print(f"Scanning {total:,} .log files under {log_dir} with {workers} workers...\n", flush=True)

    all_bugs = []
    done = 0
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_file, path): path for path in files}
        for future in as_completed(futures):
            all_bugs.extend(future.result())
            done += 1
            if done % 10000 == 0 or done == total:
                print(f"  processed {done:,}/{total:,} files | bugs found: {len(all_bugs):,}", flush=True)

    all_bugs.sort(key=lambda bug: bug["time"] or "zzzz")
    return all_bugs


def dedupe_bugs(bugs):
    seen = set()
    unique_bugs = []
    for bug in bugs:
        key = (bug["type"], bug["query"][:200], bug["message"][:200])
        if key in seen:
            continue
        seen.add(key)
        unique_bugs.append(bug)
    return unique_bugs


def format_bug(index: int, bug):
    lines = [
        "",
        "=" * 72,
        f"Bug #{index} [{bug['type']}]",
        f"Time    : {bug['time']}",
        f"DB      : {bug['db']}",
        f"Seed    : {bug['seed']}",
        f"Version : {bug['version']}",
        f"File    : {bug['file']}",
        f"Message : {bug['message']}",
    ]
    if bug["missing"]:
        lines.append(f"Missing : {bug['missing']}")
    if bug["query"]:
        lines.append(f"Query   : {bug['query']}")
    if bug["plan1"]:
        lines.append(f"Plan1   : {bug['plan1']}")
    if bug["plan2"]:
        lines.append(f"Plan2   : {bug['plan2']}")
    if bug["ddl"]:
        lines.append("DDL:")
        for ddl_line in bug["ddl"].splitlines():
            lines.append(f"  {ddl_line}")
    return "\n".join(lines)


def build_report(bugs, args):
    if args.type:
        bugs = [bug for bug in bugs if bug["type"] == args.type.upper()]

    total_before_unique = len(bugs)
    if args.unique:
        bugs = dedupe_bugs(bugs)
        removed = total_before_unique - len(bugs)
        print(f"Unique filter kept {len(bugs)} bugs and removed {removed} duplicates.\n")

    if args.limit is not None and len(bugs) > args.limit:
        print(f"Limiting output from {len(bugs)} bugs to the first {args.limit}.\n")
        bugs = bugs[: args.limit]

    counts = Counter(bug["type"] for bug in bugs)
    report_parts = [
        "=" * 72,
        "SQLancer Oracle3 Bug Report Summary",
        "=" * 72,
        f"Total bugs: {len(bugs)}",
        "",
        "Counts by type:",
    ]

    for violation_type, count in counts.most_common():
        report_parts.append(f"  {violation_type:<12} {count:>6}")

    for index, bug in enumerate(bugs, 1):
        report_parts.append(format_bug(index, bug))

    report_parts.append("\n" + "=" * 72)
    return "\n".join(report_parts)


def main():
    parser = argparse.ArgumentParser(description="Extract Oracle3 bug reports from SQLancer log files.")
    parser.add_argument("log_dir", nargs="?", default="./logs/mysql", help="Directory containing SQLancer .log files.")
    parser.add_argument("--save", metavar="FILE", help="Write the final report to FILE.")
    parser.add_argument("--workers", type=int, default=8, help="Number of worker processes to use. Default: 8.")
    parser.add_argument(
        "--type",
        metavar="TYPE",
        choices=["COUNT", "MAX", "MIN", "ROW_SET"],
        help="Only show one Oracle3 violation type.",
    )
    parser.add_argument("--unique", action="store_true", help="Deduplicate similar bugs by type/message/query.")
    parser.add_argument("--limit", type=int, default=None, help="Only print the first N bugs after filtering.")
    args = parser.parse_args()

    if not os.path.isdir(args.log_dir):
        print(f"Error: log directory does not exist: {args.log_dir}", file=sys.stderr)
        sys.exit(1)

    bugs = scan_logs(args.log_dir, args.workers)
    if not bugs:
        print("No Oracle3 assertion failures were found.")
        return

    report = build_report(bugs, args)
    print(report)

    if args.save:
        with open(args.save, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\nSaved report to {args.save}")


if __name__ == "__main__":
    main()
