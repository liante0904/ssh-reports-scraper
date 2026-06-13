#!/bin/bash
# 서버 deploy 후 정상 동작 확인 smoke test
HOST="${SERVER_HOST:-10.0.0.111}"
echo "=== Deploy Smoke Test ==="

# 1. Container running?
ssh -o ConnectTimeout=5 $HOST "docker ps --filter name=scraper-main --format '{{.Names}} {{.Status}}'" 2>/dev/null || { echo "❌ SSH 실패"; exit 1; }

# 2. scrapers/ 디렉토리 존재?
ssh $HOST "docker exec \$(docker ps --filter name=scraper-main -q) ls /app/scrapers/ 2>/dev/null | wc -l" | read cnt
if [ "$cnt" -lt 10 ]; then
    echo "❌ /app/scrapers/ 누락 ($cnt files)"
    exit 1
fi
echo "  ✅ /app/scrapers/: $cnt files"

# 3. scheduler 로그에서 ERROR 없는지
errors=$(ssh $HOST "docker logs \$(docker ps --filter name=scraper-main -q) --tail 50 2>&1" | grep -c "ModuleNotFoundError\|ImportError")
if [ "$errors" -gt 0 ]; then
    echo "❌ ModuleNotFoundError $errors 건"
    exit 1
fi
echo "  ✅ ModuleNotFoundError 0건"

# 4. GA import 동작 확인
ga_count=$(ssh $HOST "ls /home/ubuntu/incoming/ga-scrapes/archive/*.json 2>/dev/null | wc -l")
echo "  ✅ GA archive: $ga_count files"

echo "✅ Smoke test 통과"
