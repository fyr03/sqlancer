#!/usr/bin/env python3
"""
replay_sql.py
读取包含 SQL 语句序列的 txt 文件，在 MySQL 中逐条执行，
并将「执行的 SQL + 返回结果」输出到指定 log 文件。

用法：
    python replay_sql.py \
        --input  logs/mysql/database3.log \
        --output replay_output.log \
        --host   127.0.0.1 \
        --port   3306 \
        --user   root \
        --password yourpassword \
        --database database3

依赖：
    pip install mysql-connector-python
"""

import argparse
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    print("缺少依赖，请先执行：pip install mysql-connector-python")
    sys.exit(1)


# ──────────────────────────────────────────────
# SQL 解析：把文件内容切分成独立语句
# ──────────────────────────────────────────────

def split_statements(text: str) -> list[str]:
    """
    按分号切分 SQL 文本，跳过空语句。
    简单处理注释行（-- 和 /**/）以及引号内的分号。
    """
    statements = []
    current: list[str] = []
    in_single = False   # 是否在单引号字符串内
    in_double = False   # 是否在双引号字符串内
    i = 0

    while i < len(text):
        ch = text[i]

        # 跳过行注释 --
        if not in_single and not in_double and ch == '-' and text[i:i+2] == '--':
            end = text.find('\n', i)
            if end == -1:
                break
            current.append(text[i:end+1])
            i = end + 1
            continue

        # 跳过块注释 /* */
        if not in_single and not in_double and ch == '/' and text[i:i+2] == '/*':
            end = text.find('*/', i+2)
            if end == -1:
                break
            current.append(text[i:end+2])
            i = end + 2
            continue

        # 引号状态切换（简单处理，不处理转义序列 \' 等）
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double

        # 语句结束
        if ch == ';' and not in_single and not in_double:
            current.append(ch)
            stmt = ''.join(current).strip()
            if stmt and stmt != ';':
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    # 处理文件末尾没有分号的语句
    last = ''.join(current).strip()
    if last:
        statements.append(last)

    return statements


# ──────────────────────────────────────────────
# 日志写入
# ──────────────────────────────────────────────

class Logger:
    def __init__(self, log_path: str):
        self.path = Path(log_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, 'w', encoding='utf-8')

    def write(self, text: str):
        print(text, end='')
        self._fh.write(text)
        self._fh.flush()

    def close(self):
        self._fh.close()

    def log_select(self, sql: str, rows: list, cols: list):
        """SELECT 结果：SQL 一行 + ASCII 表格"""
        self.write(sql.rstrip() + '\n')
        if not rows:
            self.write("(empty)\n")
            return
        # 计算每列宽度（列名与数据取最大值）
        widths = {c: len(str(c)) for c in cols}
        for row in rows:
            for c in cols:
                widths[c] = max(widths[c], len('NULL' if row[c] is None else str(row[c])))
        sep = '+' + '+'.join('-' * (widths[c] + 2) for c in cols) + '+'
        header = '|' + '|'.join(f" {str(c):<{widths[c]}} " for c in cols) + '|'
        self.write(sep + '\n')
        self.write(header + '\n')
        self.write(sep + '\n')
        for row in rows:
            line = '|' + '|'.join(
                f" {'NULL' if row[c] is None else str(row[c]):<{widths[c]}} "
                for c in cols
            ) + '|'
            self.write(line + '\n')
        self.write(sep + '\n')

    def log_error(self, sql: str, exc: Exception):
        """只有出错才输出：SQL 一行 + 错误信息一行"""
        self.write(sql.rstrip() + '\n')
        self.write(f"ERROR: {exc}\n")

    def log_init_error(self, msg: str):
        self.write(f"[FATAL] {msg}\n")


# ──────────────────────────────────────────────
# 执行引擎
# ──────────────────────────────────────────────

def connect(args) -> mysql.connector.MySQLConnection:
    # 先不指定 database，连上后再建库/选库
    return mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        consume_results=True,
        connection_timeout=30,
    )


def run(args):
    # 读取 SQL 文件
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] 找不到输入文件：{input_path}")
        sys.exit(1)

    raw_text = input_path.read_text(encoding='utf-8', errors='replace')
    statements = split_statements(raw_text)

    if not statements:
        print("[WARN] 未解析到任何 SQL 语句，请检查输入文件。")
        sys.exit(0)

    logger = Logger(args.output)

    # 连接 MySQL，并自动建库 + 选库
    try:
        conn = connect(args)
        cursor = conn.cursor(dictionary=True)   # 返回 dict，列名作 key

        db = args.database
        init_sqls = [
            f"DROP DATABASE IF EXISTS `{db}`",
            f"CREATE DATABASE `{db}`",
            f"USE `{db}`",
        ]
        for isql in init_sqls:
            cursor.execute(isql)
            conn.commit()

    except MySQLError as e:
        logger.log_init_error(f"无法连接 MySQL 或初始化数据库：{e}")
        logger.close()
        sys.exit(1)

    ok_count = err_count = 0

    for idx, sql in enumerate(statements, 1):
        try:
            cursor.execute(sql)

            if cursor.description:
                # SELECT / SHOW / EXPLAIN 等有结果集的语句 → 输出 SQL + 表格
                rows = cursor.fetchall()
                cols = list(rows[0].keys()) if rows else []
                logger.log_select(sql, rows, cols)
            else:
                # DML / DDL 正常返回 → 只记录 SQL，不输出结果行
                conn.commit()
                logger.write(sql.rstrip() + '\n')

            ok_count += 1

        except MySQLError as e:
            logger.log_error(sql, e)
            err_count += 1
            try:
                conn.rollback()
            except Exception:
                pass

        except Exception as e:
            logger.log_error(sql, e)
            err_count += 1

    cursor.close()
    conn.close()
    logger.close()

    print(f"\n日志已写入：{Path(args.output).resolve()}")


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="SQLancer SQL 回放脚本：读取 SQL 文件并在 MySQL 中执行，输出结构化日志"
    )
    p.add_argument('--input',    required=True,  help='输入的 SQL txt/log 文件路径')
    p.add_argument('--output',   required=True,  help='输出日志文件路径')
    p.add_argument('--host',     default='127.0.0.1', help='MySQL 主机（默认 127.0.0.1）')
    p.add_argument('--port',     default=3307, type=int, help='MySQL 端口（默认 3307）')
    p.add_argument('--user',     default='root', help='MySQL 用户名（默认 root）')
    p.add_argument('--password', default='123456',     help='MySQL 密码')
    p.add_argument('--database', default='test_replay', help='目标数据库（默认test_replay）')
    return p.parse_args()


if __name__ == '__main__':
    run(parse_args())