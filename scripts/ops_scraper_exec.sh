#!/usr/bin/env bash
# Execute commands in the active ssh-reports-scraper main container.
#
# Usage:
#   bash scripts/ops_scraper_exec.sh list
#   bash scripts/ops_scraper_exec.sh sh 'python --version'
#   bash scripts/ops_scraper_exec.sh py <<'PY'
#   print("hello")
#   PY
#
# Selection:
#   - SCRAPER_CONTAINER overrides auto-detection.
#   - Auto-detection uses the newest running container named
#     ssh-reports-scraper-main-scraper-*.

set -euo pipefail

CONTAINER_PREFIX="${CONTAINER_PREFIX:-ssh-reports-scraper-main-scraper-}"
APP_DIR="${APP_DIR:-/app}"
PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"

usage() {
    awk '
        NR == 1 { next }
        /^#/ { sub(/^# ?/, ""); print; next }
        { exit }
    ' "$0"
}

list_containers() {
    docker ps \
        --filter "name=${CONTAINER_PREFIX}" \
        --filter "status=running" \
        --format '{{.Names}}'
}

select_container() {
    if [[ -n "${SCRAPER_CONTAINER:-}" ]]; then
        echo "$SCRAPER_CONTAINER"
        return
    fi

    local selected
    selected="$(list_containers | head -n 1)"
    if [[ -z "$selected" ]]; then
        echo "No running ${CONTAINER_PREFIX}* container found." >&2
        return 1
    fi
    echo "$selected"
}

cmd="${1:-}"
case "$cmd" in
    ""|-h|--help)
        usage
        ;;
    list)
        list_containers
        ;;
    name)
        select_container
        ;;
    sh)
        shift
        container="$(select_container)"
        echo "[ops_scraper_exec] container=${container}" >&2
        docker exec -i "$container" bash -lc "cd ${APP_DIR} && $*"
        ;;
    py)
        shift || true
        container="$(select_container)"
        echo "[ops_scraper_exec] container=${container}" >&2
        docker exec -i "$container" bash -lc "cd ${APP_DIR} && ${PYTHON_BIN} - $*"
        ;;
    *)
        echo "Unknown command: $cmd" >&2
        usage >&2
        exit 2
        ;;
esac
