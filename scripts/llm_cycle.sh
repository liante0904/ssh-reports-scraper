#!/usr/bin/env bash
# Run the standard DeepSeek -> Gemini/AGY delegation cycle.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TIMEOUT_SEC="${LLM_CYCLE_TIMEOUT_SEC:-1800}"
SEND=true
PARALLEL=false

usage() {
    cat <<'HELP'
Usage: bash scripts/llm_cycle.sh [--dry-run] [--parallel] [--timeout SEC]

Run the standard delegated LLM cycle:
  1. Send .agent_tasks/deepseek_next.md to DeepSeek.
  2. Wait for .agent_tasks/deepseek_result.md.
  3. Send .agent_tasks/gemini_agy_next.md to Gemini/AGY.
  4. Wait for .agent_tasks/gemini_agy_result.md.

Environment:
  DEEPSEEK_TMUX_TARGET       default: deepseek
  GEMINI_AGY_TMUX_TARGET     default: agy
  LLM_DISPATCH_ALLOW_SHELL_TARGET=1
    Use only when the target pane is known to contain an LLM CLI launched from a shell,
    such as cc-backed Claude Code for DeepSeek.

Examples:
  LLM_DISPATCH_ALLOW_SHELL_TARGET=1 \
  DEEPSEEK_TMUX_TARGET='deepseek' \
  GEMINI_AGY_TMUX_TARGET='0:8.1' \
  bash scripts/llm_cycle.sh

  bash scripts/llm_cycle.sh --dry-run

  bash scripts/llm_cycle.sh --parallel
HELP
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            SEND=false; shift ;;
        --parallel)
            PARALLEL=true; shift ;;
        --timeout)
            TIMEOUT_SEC="$2"; shift 2 ;;
        --help|-h)
            usage; exit 0 ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 2 ;;
    esac
done

cd "$ROOT_DIR"

if $PARALLEL; then
    echo "=== LLM cycle: DeepSeek + Gemini/AGY parallel ==="
    if $SEND; then
        bash scripts/llm_dispatch.sh both --send --wait --parallel --timeout "$TIMEOUT_SEC"
    else
        bash scripts/llm_dispatch.sh both --parallel
    fi
else
    echo "=== LLM cycle: DeepSeek ==="
    if $SEND; then
        bash scripts/llm_dispatch.sh deepseek --send --wait --timeout "$TIMEOUT_SEC"
    else
        bash scripts/llm_dispatch.sh deepseek
    fi

    echo "=== LLM cycle: Gemini/AGY ==="
    if $SEND; then
        bash scripts/llm_dispatch.sh gemini --send --wait --timeout "$TIMEOUT_SEC"
    else
        bash scripts/llm_dispatch.sh gemini
    fi
fi

echo "=== LLM cycle complete ==="
