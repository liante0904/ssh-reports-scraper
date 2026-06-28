#!/usr/bin/env bash
# Read-only OCI production log helper.

set -euo pipefail

OCI=(ssh oci)
TODAY="$(date +%Y%m%d)"
DATE="$TODAY"
SINCE="06:00"
SERVICE=""
WATCHDOG=false
SCRAPER=false
LOGS_ONLY=false
DOCKER_ONLY=false

usage() {
    cat <<'HELP'
Usage: bash scripts/ops_tail_errors.sh [OPTIONS]

Read-only production log tail for OCI scraper/watchdog logs.

Options:
  --since "HH:MM"             Show logs after this time (default: 06:00).
  --since "YYYY-MM-DD HH:MM"  Full datetime.
  --date YYYYMMDD             Log date (default: today KST).
  --service NAME              Query only that Docker container log.
  --watchdog                  Limit Docker section to watchdog container.
  --scraper                   Limit Docker section to ssh-reports-scraper containers.
  --logs-only                 Skip Docker logs.
  --docker-only               Skip file logs.
  --help                      Show this help.

Examples:
  bash scripts/ops_tail_errors.sh --since "09:00"
  bash scripts/ops_tail_errors.sh --date 20260627 --logs-only
  bash scripts/ops_tail_errors.sh --docker-only --watchdog
  bash scripts/ops_tail_errors.sh --docker-only --scraper
  bash scripts/ops_tail_errors.sh --service ssh-reports-scraper-main-scraper-green

Safety:
  This script is read-only. It must not write DB rows, edit files, restart
  services, delete logs, mutate git state, change crontab, or use sudo.
HELP
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --since)
            SINCE="$2"; shift 2 ;;
        --date)
            DATE="$2"; shift 2 ;;
        --service)
            SERVICE="$2"; shift 2 ;;
        --watchdog)
            WATCHDOG=true; shift ;;
        --scraper)
            SCRAPER=true; shift ;;
        --logs-only)
            LOGS_ONLY=true; shift ;;
        --docker-only)
            DOCKER_ONLY=true; shift ;;
        --help|-h)
            usage; exit 0 ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 2 ;;
    esac
done

if [[ "$SINCE" =~ ^[0-9]{2}:[0-9]{2}$ ]]; then
    SINCE_DATETIME="${DATE:0:4}-${DATE:4:2}-${DATE:6:2} ${SINCE}"
else
    SINCE_DATETIME="$SINCE"
fi

SINCE_TS="$(date -d "$SINCE_DATETIME" +%s 2>/dev/null)" || {
    echo "ERROR: cannot parse --since '$SINCE_DATETIME'" >&2
    exit 1
}
SINCE_HHMM="$(date -d "@$SINCE_TS" +%H:%M)"

echo "============================================================"
echo " OCI Production Log Tail"
echo " Local  : $(date '+%Y-%m-%d %H:%M:%S KST')"
echo " Since  : $(date -d "@$SINCE_TS" '+%Y-%m-%d %H:%M:%S') KST"
echo "============================================================"

echo ""
echo "=== Remote Timestamp ==="
"${OCI[@]}" "date '+%Y-%m-%d %H:%M:%S KST'" 2>/dev/null || {
    echo "ERROR: ssh oci failed. Check SSH key and network." >&2
    exit 1
}

if ! $LOGS_ONLY; then
    echo ""
    echo "=== Docker Containers ==="
    "${OCI[@]}" "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' 2>/dev/null" || echo "(docker ps failed)"
fi

if ! $DOCKER_ONLY; then
    echo ""
    echo "=== File Logs (/home/ubuntu/logs/${DATE}) ==="
    LOG_DIR="/home/ubuntu/logs/${DATE}"
    if "${OCI[@]}" "test -d $LOG_DIR" 2>/dev/null; then
        "${OCI[@]}" "find $LOG_DIR -name '*.log' -type f -print0 2>/dev/null | xargs -0 awk -v since=\"$SINCE_HHMM\" '
            BEGIN { printed = 0 }
            /ERROR|FATAL|Traceback|CRITICAL|Exception|WARNING|WARN/ {
                if (match(\$0, /[0-9]{2}:[0-9]{2}:[0-9]{2}/)) {
                    ts = substr(\$0, RSTART, 5)
                    if (ts >= since) {
                        if (!printed) { print \"=== \" FILENAME \" ===\"; printed = 1 }
                        print
                    }
                } else {
                    if (!printed) { print \"=== \" FILENAME \" ===\"; printed = 1 }
                    print
                }
            }
        '" 2>/dev/null || echo "(no matching log lines)"
    else
        echo "(log dir $LOG_DIR not found on oci)"
    fi
fi

if ! $LOGS_ONLY; then
    echo ""
    echo "=== Docker Logs (since $SINCE_DATETIME) ==="
    if [[ -n "$SERVICE" ]]; then
        echo "--- $SERVICE ---"
        "${OCI[@]}" "docker logs --since '$SINCE_DATETIME' '$SERVICE' 2>&1 | grep -E 'ERROR|FATAL|Traceback|CRITICAL|Exception|WARNING|WARN' | tail -100" || echo "(no matching lines)"
    else
        CONTAINERS="$("${OCI[@]}" "docker ps --format '{{.Names}}' 2>/dev/null" || echo "")"
        if [[ -z "$CONTAINERS" ]]; then
            echo "(no running containers)"
        else
            for c in $CONTAINERS; do
                if $WATCHDOG && [[ "$c" != *"watchdog"* ]]; then
                    continue
                fi
                if $SCRAPER && [[ "$c" != *"ssh-reports-scraper"* ]]; then
                    continue
                fi
                echo "--- $c ---"
                "${OCI[@]}" "docker logs --since '$SINCE_DATETIME' '$c' 2>&1 | grep -E 'ERROR|FATAL|Traceback|CRITICAL|Exception|WARNING|WARN' | tail -50" 2>/dev/null || echo "(no matching lines)"
            done
        fi
    fi
fi

echo ""
echo "============================================================"
echo " Log tail complete."
echo "============================================================"
