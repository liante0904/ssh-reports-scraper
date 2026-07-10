#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXCLUSIONS="$ROOT/tests/ci_excluded_tests.txt"

cd "$ROOT"

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
uv run python scripts/harness.py --all --offline

echo "==> Deterministic unit and contract regression suite"
CI=true DB_BACKEND=static ENV=dev uv run pytest -q "${ignore_args[@]}"
