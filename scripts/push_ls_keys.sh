#!/bin/bash
# OCI cron: LS existing key 목록 export → GitHub repo push → GA가 checkout해서 사용
# 실행 주기: 매시간 55분 (GA cron 시작 5분 전)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$SCRIPT_DIR/.."
cd "$REPO_DIR"

KEYS_FILE="data/ls_existing_keys.json"

echo "[$(date)] Exporting LS keys..."
.venv/bin/python scripts/export_ls_keys.py "$KEYS_FILE"

if [ ! -f "$KEYS_FILE" ]; then
    echo "ERROR: export failed, no file created"
    exit 1
fi

COUNT=$(python3 -c "import json; print(json.load(open('$KEYS_FILE')).get('count',0))")
echo "[$(date)] $COUNT keys exported"

# Check if file changed
if git diff --quiet "$KEYS_FILE" 2>/dev/null; then
    echo "[$(date)] No changes, skipping push"
    exit 0
fi

git add "$KEYS_FILE"
git commit -m "data: update LS existing keys ($COUNT records)" || true
git push origin main

echo "[$(date)] Pushed $COUNT keys to GitHub"
