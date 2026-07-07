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
FIRM_ORDER=""
FIRM_NAME=""
DATE_FROM=""
DATE_TO=""

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
  --firm-order N              Query DB for firm metadata + latest 10 rows.
  --firm-name PATTERN         grep -E pattern for firm logs (e.g. 'HANA|하나|hana').
  --date-from YYYYMMDD        Start date for firm log scan.
  --date-to YYYYMMDD          End date for firm log scan.
  --help                      Show this help.

Examples:
  bash scripts/ops_tail_errors.sh --since "09:00"
  bash scripts/ops_tail_errors.sh --date 20260627 --logs-only
  bash scripts/ops_tail_errors.sh --docker-only --watchdog
  bash scripts/ops_tail_errors.sh --docker-only --scraper
  bash scripts/ops_tail_errors.sh --service ssh-reports-scraper-main-scraper-green
  bash scripts/ops_tail_errors.sh --firm-order 3 --firm-name 'HANA|하나|hana' --date-from 20260626 --date-to 20260629 --logs-only

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
        --firm-order)
            FIRM_ORDER="$2"; shift 2 ;;
        --firm-name)
            FIRM_NAME="$2"; shift 2 ;;
        --date-from)
            DATE_FROM="$2"; shift 2 ;;
        --date-to)
            DATE_TO="$2"; shift 2 ;;
        --help|-h)
            usage; exit 0 ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 2 ;;
    esac
done

if [[ -n "$FIRM_ORDER" && ! "$FIRM_ORDER" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --firm-order must be numeric" >&2
    exit 2
fi

if [[ -n "$DATE" && ! "$DATE" =~ ^[0-9]{8}$ ]]; then
    echo "ERROR: --date must be YYYYMMDD" >&2
    exit 2
fi

if [[ -n "$DATE_FROM" && ! "$DATE_FROM" =~ ^[0-9]{8}$ ]]; then
    echo "ERROR: --date-from must be YYYYMMDD" >&2
    exit 2
fi

if [[ -n "$DATE_TO" && ! "$DATE_TO" =~ ^[0-9]{8}$ ]]; then
    echo "ERROR: --date-to must be YYYYMMDD" >&2
    exit 2
fi

case "$FIRM_NAME" in
    *[\`\'\"\;\&\<\>\$\(\)\{\}\[\]\\]*)
        echo "ERROR: --firm-name allows plain text and simple regex chars only; avoid shell metacharacters" >&2
        exit 2
        ;;
esac

FIRM_NAME_B64=""
if [[ -n "$FIRM_NAME" ]]; then
    FIRM_NAME_B64="$(printf '%s' "$FIRM_NAME" | base64 | tr -d '\n')"
fi

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

# ── Firm diagnostics ──
if [[ -n "$FIRM_NAME" || -n "$FIRM_ORDER" ]]; then
    echo ""
    echo "=== Firm Diagnostics ==="

    # Auto-detect scraper container name (green/blue)
    SCRAPER_CONTAINER="$("${OCI[@]}" "docker ps --format '{{.Names}}' 2>/dev/null | grep 'ssh-reports-scraper-main-scraper' | head -1" || echo "")"

    # Date range
    from="${DATE_FROM:-$DATE}"
    to="${DATE_TO:-$DATE}"

    if [[ -n "$FIRM_ORDER" ]]; then
        echo "--- Firm Metadata (order=$FIRM_ORDER) ---"
        "${OCI[@]}" "docker exec main-postgres psql -U ssh_reports_hub -d ssh_reports_hub -c \"
            SELECT firm_id, firm_nm, telegram_update_yn, ga_enabled_yn
            FROM tbm_sec_firm_info WHERE firm_id = $FIRM_ORDER\" 2>/dev/null" || echo "(DB query failed)"

        echo "--- Latest 10 Rows ---"
        "${OCI[@]}" "docker exec main-postgres psql -U ssh_reports_hub -d ssh_reports_hub -c \"
            SELECT report_date, article_title, save_at
            FROM tbl_sec_reports WHERE firm_id = $FIRM_ORDER
            ORDER BY save_at DESC LIMIT 10\" 2>/dev/null" || echo "(DB query failed)"
    fi

    if [[ -n "$FIRM_NAME" ]]; then
        echo "--- Log Scan: $FIRM_NAME ($from ~ $to) ---"
        # Iterate date range
        d="$from"
        while [[ "$d" -le "$to" ]]; do
            LOG_DIR="/home/ubuntu/logs/$d"
            echo "  Date: $d"
            if "${OCI[@]}" "test -d $LOG_DIR" 2>/dev/null; then
                # Count FULL-SCRAPE / REGULAR lines
                full_count="$("${OCI[@]}" "find $LOG_DIR -name '*.log' -type f -exec grep -ch 'FULL-SCRAPE MODE' {} + 2>/dev/null | awk '{s+=\$1}END{print s+0}'" || echo "0")"
                reg_count="$("${OCI[@]}" "find $LOG_DIR -name '*.log' -type f -exec grep -ch 'REGULAR MODE' {} + 2>/dev/null | awk '{s+=\$1}END{print s+0}'" || echo "0")"
                echo "    FULL-SCRAPE=$full_count REGULAR=$reg_count"

                hits="$("${OCI[@]}" "PATTERN=\$(printf %s '$FIRM_NAME_B64' | base64 -d); find $LOG_DIR -name '*.log' -type f -exec grep -chE \"\$PATTERN\" {} + 2>/dev/null | awk '{s+=\$1}END{print s+0}'" || echo "0")"
                echo "    firm hits=$hits"

                # Sample firm lines (up to 3)
                if [[ "$hits" -gt 0 ]]; then
                    "${OCI[@]}" "PATTERN=\$(printf %s '$FIRM_NAME_B64' | base64 -d); find $LOG_DIR -name '*.log' -type f -exec grep -hE \"\$PATTERN\" {} + 2>/dev/null | tail -3" || true
                fi
            else
                echo "    (no log dir)"
            fi
            # Increment date by 1 day (POSIX-safe)
            d="$(date -d "$d +1 day" +%Y%m%d 2>/dev/null)" || break
        done
    fi

    # Scraper container firm log tail
    if [[ -n "$SCRAPER_CONTAINER" && -n "$FIRM_NAME" ]]; then
        echo "--- Scraper Container ($SCRAPER_CONTAINER) Firm Lines ---"
        "${OCI[@]}" "PATTERN=\$(printf %s '$FIRM_NAME_B64' | base64 -d); docker logs '$SCRAPER_CONTAINER' 2>&1 | grep -E \"\$PATTERN\" | tail -20" 2>/dev/null || echo "(no matching lines)"
    fi
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
                if $SCRAPER && [[ "$c" != *"ssh-reports-scraper-main-scraper"* ]]; then
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
