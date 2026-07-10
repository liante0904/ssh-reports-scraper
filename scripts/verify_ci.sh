#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXCLUSIONS="$ROOT/tests/ci_excluded_tests.txt"

cd "$ROOT"

if [[ -d "$ROOT/vendor/ssh_library/ssh_library" ]]; then
    export PYTHONPATH="$ROOT/vendor/ssh_library${PYTHONPATH:+:$PYTHONPATH}"
fi

if [[ ! -f "$EXCLUSIONS" ]]; then
    echo "Missing CI exclusion manifest: $EXCLUSIONS" >&2
    exit 1
fi

ignore_args=()
while IFS='|' read -r test_path reason; do
    [[ -z "$test_path" || "$test_path" == \#* ]] && continue
    if [[ -z "$reason" ]]; then
        echo "CI exclusion requires a reason: $test_path" >&2
        exit 1
    fi
    if [[ ! -f "$test_path" ]]; then
        echo "CI exclusion points to a missing file: $test_path" >&2
        exit 1
    fi
    ignore_args+=("--ignore=$test_path")
done < "$EXCLUSIONS"

echo "==> Offline manifest and runtime file guards"
timeout --signal=TERM --kill-after=10s 30s \
    uv run python scripts/harness.py --all --offline

echo "==> Deterministic unit and contract regression suite"
CI=true DB_BACKEND=static ENV=dev \
    timeout --signal=TERM --kill-after=10s 240s \
    uv run pytest -vv "${ignore_args[@]}"
