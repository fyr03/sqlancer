#!/bin/bash
BACKUP_DIR="./logs/sqlite_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

cp -r ./logs/sqlite3 "$BACKUP_DIR/"
cp ./logs/sqlite_run.log "$BACKUP_DIR/" 2>/dev/null || echo "⚠ sqlite_run.log 不存在，跳过"
cp ./logs/sqlite_bug_list.txt "$BACKUP_DIR/" 2>/dev/null || echo "⚠ sqlite_bug_list.txt 不存在，跳过"

echo "✓ 备份完成：$BACKUP_DIR"