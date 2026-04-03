#!/usr/bin/env python3
"""
Replay a SQLancer SQL log against SQLite and write a structured execution log.

Example:
    python sqlite_replay.py \
        --input logs/sqlite3/database0-cur.log \
        --output replay_output.log \
        --database replay.sqlite
"""

import argparse
import sqlite3
import sys
from pathlib import Path


def split_statements(text: str) -> list[str]:
    """
    Split SQL text into statements on semicolons, while keeping simple quoted
    strings and comments intact.
    """
    statements = []
    current: list[str] = []
    in_single = False
    in_double = False
    i = 0

    while i < len(text):
        ch = text[i]

        if not in_single and not in_double and text[i:i + 2] == "--":
            end = text.find("\n", i)
            if end == -1:
                current.append(text[i:])
                break
            current.append(text[i:end + 1])
            i = end + 1
            continue

        if not in_single and not in_double and text[i:i + 2] == "/*":
            end = text.find("*/", i + 2)
            if end == -1:
                current.append(text[i:])
                break
            current.append(text[i:end + 2])
            i = end + 2
            continue

        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double

        if ch == ";" and not in_single and not in_double:
            current.append(ch)
            stmt = "".join(current).strip()
            if stmt and stmt != ";":
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    last = "".join(current).strip()
    if last:
        statements.append(last)

    return statements


class Logger:
    def __init__(self, log_path: str):
        self.path = Path(log_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "w", encoding="utf-8")

    def write(self, text: str) -> None:
        print(text, end="")
        self._fh.write(text)
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()

    def log_select(self, sql: str, rows: list[sqlite3.Row], cols: list[str]) -> None:
        self.write(sql.rstrip() + "\n")
        if not rows:
            self.write("(empty)\n")
            return

        widths = {c: len(str(c)) for c in cols}
        for row in rows:
            for c in cols:
                widths[c] = max(widths[c], len("NULL" if row[c] is None else str(row[c])))

        sep = "+" + "+".join("-" * (widths[c] + 2) for c in cols) + "+"
        header = "|" + "|".join(f" {str(c):<{widths[c]}} " for c in cols) + "|"
        self.write(sep + "\n")
        self.write(header + "\n")
        self.write(sep + "\n")
        for row in rows:
            line = "|" + "|".join(
                f" {'NULL' if row[c] is None else str(row[c]):<{widths[c]}} " for c in cols
            ) + "|"
            self.write(line + "\n")
        self.write(sep + "\n")

    def log_error(self, sql: str, exc: Exception) -> None:
        self.write(sql.rstrip() + "\n")
        self.write(f"ERROR: {exc}\n")

    def log_init_error(self, msg: str) -> None:
        self.write(f"[FATAL] {msg}\n")


def connect(database_path: Path, keep_existing: bool) -> sqlite3.Connection:
    if database_path.exists() and not keep_existing:
        database_path.unlink()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(database_path))
    conn.row_factory = sqlite3.Row
    return conn


def run(args) -> None:
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path}")
        sys.exit(1)

    raw_text = input_path.read_text(encoding="utf-8", errors="replace")
    statements = split_statements(raw_text)
    if not statements:
        print("[WARN] No SQL statements were parsed from the input file.")
        sys.exit(0)

    logger = Logger(args.output)

    try:
        conn = connect(Path(args.database), args.keep_existing)
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys = OFF")
        conn.commit()
    except sqlite3.Error as e:
        logger.log_init_error(f"Failed to open SQLite database: {e}")
        logger.close()
        sys.exit(1)

    try:
        for sql in statements:
            try:
                cur.execute(sql)
                if cur.description:
                    rows = cur.fetchall()
                    cols = [desc[0] for desc in cur.description]
                    logger.log_select(sql, rows, cols)
                else:
                    conn.commit()
                    logger.write(sql.rstrip() + "\n")
            except sqlite3.Error as e:
                logger.log_error(sql, e)
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
            except Exception as e:
                logger.log_error(sql, e)
    finally:
        cur.close()
        conn.close()
        logger.close()

    print(f"\nLog written to: {Path(args.output).resolve()}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replay a SQL file against SQLite and write a structured log."
    )
    parser.add_argument("--input", required=True, help="Input SQL txt/log file path")
    parser.add_argument("--output", required=True, help="Output log file path")
    parser.add_argument(
        "--database",
        default="replay.sqlite",
        help="SQLite database file path (default: replay.sqlite)",
    )
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Reuse the existing SQLite database file instead of recreating it",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
