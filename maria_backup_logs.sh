#!/bin/bash
BACKUP_DIR="./logs/mariadb_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

cp -r ./logs/mariadb "$BACKUP_DIR/"
cp ./logs/mariadb_run.log "$BACKUP_DIR/" 2>/dev/null || echo "⚠ mariadb_run.log 不存在，跳过"
cp ./logs/mariadb_bug_list.txt "$BACKUP_DIR/" 2>/dev/null || echo "⚠ mariadb_bug_list.txt 不存在，跳过"

echo "✓ 备份完成：$BACKUP_DIR"