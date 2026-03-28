#!/bin/bash
BACKUP_DIR="./logs/mysql_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

cp -r ./logs/mysql "$BACKUP_DIR/"
cp ./logs/mysql_run.log "$BACKUP_DIR/" 2>/dev/null || echo "⚠ mysql_run.log 不存在，跳过"
cp ./logs/mysql_bug_list.txt "$BACKUP_DIR/" 2>/dev/null || echo "⚠ mysql_bug_list.txt 不存在，跳过"

echo "✓ 备份完成：$BACKUP_DIR"