#!/bin/bash
# OCI cron: LS key export → 암호화 → GitHub Release 업로드
# 매시간 55분 실행. 암호화 키: 기존 TELEGRAM_BOT_TOKEN (OCI/GA 양쪽에 존재)
# Release tag: ls-keys-data (영구 — asset 덮어쓰기)
set -euo pipefail

REPO="liante0904/ssh-reports-scraper"
RELEASE_TAG="ls-keys-data"
DIR="/home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper"
cd "$DIR"
mkdir -p data

CT=$(sudo docker ps --format '{{.Names}}' | grep 'main-scraper' | head -1)
[ -z "$CT" ] && { echo "[$(date)] no container"; exit 1; }

# export
sudo docker exec "$CT" mkdir -p /app/scripts /app/data 2>/dev/null
sudo docker cp scripts/export_ls_keys.py "$CT:/app/scripts/" 2>/dev/null || true
sudo docker exec "$CT" .venv/bin/python /app/scripts/export_ls_keys.py /app/data/ls_existing_keys.json
sudo docker cp "$CT:/app/data/ls_existing_keys.json" data/ls_existing_keys.json

# encrypt with existing shared secret (from container env or host env)
ENCRYPT_KEY="${TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET:-}"
if [ -z "$ENCRYPT_KEY" ]; then
    ENCRYPT_KEY=$(sudo docker exec "$CT" printenv TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET 2>/dev/null || echo "")
fi
if [ -z "$ENCRYPT_KEY" ]; then
    echo "[$(date)] ERROR: TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET not found"
    exit 1
fi
openssl enc -aes-256-cbc -pbkdf2 -pass "pass:${ENCRYPT_KEY:0:64}" \
    -in data/ls_existing_keys.json -out data/ls_existing_keys.enc 2>/dev/null

COUNT=$(python3 -c "import json; print(json.load(open('data/ls_existing_keys.json')).get('count',0))")

# GitHub Release upload
if ! gh release view "$RELEASE_TAG" --repo "$REPO" &>/dev/null; then
    gh release create "$RELEASE_TAG" data/ls_existing_keys.enc \
        --repo "$REPO" --title "LS Keys" --notes "encrypted" 2>&1
else
    gh release upload "$RELEASE_TAG" data/ls_existing_keys.enc \
        --repo "$REPO" --clobber 2>&1
fi

rm -f data/ls_existing_keys.json data/ls_existing_keys.enc
echo "[$(date)] Released $COUNT LS keys"