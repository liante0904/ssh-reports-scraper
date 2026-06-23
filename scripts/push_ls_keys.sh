#!/bin/bash
# OCI cron: LS existing key 목록 export → GitHub repo push → GA가 checkout해서 사용
# 실행 주기: 매시간 55분 (GA cron 시작 5분 전)
# Docker 컨테이너 내부 Python으로 실행 (ssh_library 의존성)
set -euo pipefail

REPO_DIR="/home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper"
cd "$REPO_DIR"
mkdir -p data

KEYS_FILE="data/ls_existing_keys.json"
CONTAINER=$(sudo docker ps --format '{{.Names}}' | grep 'main-scraper' | head -1)

if [ -z "$CONTAINER" ]; then
    echo "[$(date)] ERROR: no main-scraper container found"
    exit 1
fi

echo "[$(date)] Exporting LS keys from $CONTAINER..."

# 컨테이너 내 디렉토리 보장
sudo docker exec "$CONTAINER" mkdir -p /app/scripts /app/data

# export 스크립트가 컨테이너에 없으면 호스트에서 복사
sudo docker cp scripts/export_ls_keys.py "$CONTAINER:/app/scripts/" 2>/dev/null || true

# 컨테이너 내부에서 export 실행
sudo docker exec "$CONTAINER" .venv/bin/python /app/scripts/export_ls_keys.py /app/data/ls_existing_keys.json

# 결과를 호스트로 복사
sudo docker cp "$CONTAINER:/app/data/ls_existing_keys.json" "$KEYS_FILE"

COUNT=$(python3 -c "import json; print(json.load(open('$KEYS_FILE')).get('count',0))")
echo "[$(date)] $COUNT keys exported"

# 변경 없으면 push 스킵
if git diff --quiet "$KEYS_FILE" 2>/dev/null; then
    echo "[$(date)] No changes, skipping push"
    exit 0
fi

git add "$KEYS_FILE"
git commit -m "data: update LS existing keys ($COUNT records)" 2>&1 || true
git push origin main 2>&1

echo "[$(date)] Pushed $COUNT keys to GitHub"
