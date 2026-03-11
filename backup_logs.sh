#!/bin/bash
BACKUP_DIR="./logs/mysql_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

cp -r ./logs/mysql "$BACKUP_DIR/"
cp ./logs/subset_run.log "$BACKUP_DIR/" 2>/dev/null || echo "⚠ subset_run.log 不存在，跳过"
cp ./logs/bug_list.txt "$BACKUP_DIR/" 2>/dev/null || echo "⚠ bug_list.txt 不存在，跳过"

echo "✓ 备份完成：$BACKUP_DIR"