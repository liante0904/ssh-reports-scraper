#!/usr/bin/env bash
# OCI 운영 명령 통합 스크립트.
#
# 사용법:
#   bash scripts/ops_ssh.sh psql "SELECT count(*) FROM tbl_sec_reports"
#   bash scripts/ops_ssh.sh psql-admin "ALTER TABLE ..."
#   bash scripts/ops_ssh.sh logs backend    # backend | scraper | nginx
#   bash scripts/ops_ssh.sh logs scraper 100
#   bash scripts/ops_ssh.sh restart backend
#   bash scripts/ops_ssh.sh deploy-status
#   bash scripts/ops_ssh.sh dbshell
#   bash scripts/ops_ssh.sh smoke           # API health check

set -euo pipefail

OCI="ssh oci"
PG_EXEC="docker exec -i main-postgres psql"

usage() {
    grep '^#' "$0" | grep -v '^#!/' | sed 's/^# //'
    exit 0
}

[[ $# -eq 0 || "$1" == "-h" || "$1" == "--help" ]] && usage

CMD="$1"
shift || true

case "$CMD" in
    psql)
        $OCI "$PG_EXEC -U ssh_reports_hub -d ssh_reports_hub -c '$*'"
        ;;
    psql-admin)
        $OCI "$PG_EXEC -U admin -d ssh_reports_hub -c '$*'"
        ;;
    dbshell)
        $OCI "$PG_EXEC -U ssh_reports_hub -d ssh_reports_hub" </dev/tty
        ;;
    logs)
        CONTAINER="${1:-backend}"
        LINES="${2:-50}"
        case "$CONTAINER" in
            backend) NAME=$($OCI "docker ps --format '{{.Names}}' | grep 'fastapi-blue\|fastapi-green' | head -1") ;;
            scraper) NAME=$($OCI "docker ps --format '{{.Names}}' | grep 'main-scraper' | head -1") ;;
            nginx)   NAME="external-nginx" ;;
            *)       NAME="$CONTAINER" ;;
        esac
        $OCI "docker logs ${NAME:-$CONTAINER} --tail $LINES 2>&1"
        ;;
    restart)
        CONTAINER="${1:-backend}"
        case "$CONTAINER" in
            backend) NAME=$($OCI "docker ps --format '{{.Names}}' | grep 'fastapi-blue\|fastapi-green' | head -1") ;;
            scraper) NAME=$($OCI "docker ps --format '{{.Names}}' | grep 'main-scraper' | head -1") ;;
            *)       NAME="$CONTAINER" ;;
        esac
        $OCI "docker restart ${NAME:-$CONTAINER}"
        ;;
    deploy-status)
        echo "=== scraper ===" && gh run list --repo liante0904/ssh-reports-scraper --workflow deploy.yml --limit 1 --json status,conclusion 2>/dev/null
        echo "=== backend ===" && gh run list --repo liante0904/ssh-reports-hub-fastAPI --limit 1 --json status,conclusion 2>/dev/null
        echo "=== frontend ===" && gh run list --repo liante0904/ssh-reports-hub --limit 1 --json status,conclusion 2>/dev/null
        ;;
    smoke)
        for ep in /external/api/companies "/external/api/recent?limit=1" "/external/api/search?limit=1"; do
            code=$(curl -s -o /dev/null -w "%{http_code}" "https://ssh-oci.duckdns.org$ep")
            echo "$ep: $code"
        done
        ;;
    *)
        echo "Unknown: $CMD"
        usage
        ;;
esac
