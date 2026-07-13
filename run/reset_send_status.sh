#!/usr/bin/env bash
# ------------------------------------------------------------------
# reset_send_status.sh — 도커 컨테이너 내에서 reset_send_status.py 실행
#
# 사용법:
#   ./run/reset_send_status.sh --firm 19 --date 2026-07-13
#   ./run/reset_send_status.sh --firm 19 --date 2026-07-13 --send
#   ./run/reset_send_status.sh --firm 11 --date 2026-07-13 --board 3 --send
#
# Requirements:
#   - 호스트에서 docker 명령어 사용 가능
#   - ssh-reports-scraper-main-scraper-blue 또는 green 컨테이너 실행 중
# ------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# 실행 중인 scraper 컨테이너 찾기 (blue 우선)
CONTAINER=""
for candidate in ssh-reports-scraper-main-scraper-blue ssh-reports-scraper-main-scraper-green; do
    if docker inspect "${candidate}" --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
        CONTAINER="${candidate}"
        break
    fi
done

if [ -z "${CONTAINER}" ]; then
    echo "❌ 실행 중인 scraper 컨테이너를 찾을 수 없습니다." >&2
    echo "   대상: ssh-reports-scraper-main-scraper-blue / green" >&2
    exit 1
fi

echo "📦 대상 컨테이너: ${CONTAINER}"
echo "🚀 실행: .venv/bin/python run/reset_send_status.py $*"
echo ""

docker exec -i "${CONTAINER}" .venv/bin/python run/reset_send_status.py "$@"
