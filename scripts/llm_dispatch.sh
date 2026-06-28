#!/usr/bin/env bash
# Dispatch fixed .agent_tasks prompts to tmux-backed LLM CLI sessions.
#
# Default mode is dry-run. Use --send only after verifying tmux session names.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DEEPSEEK_TARGET="${DEEPSEEK_TMUX_TARGET:-${DEEPSEEK_TMUX_SESSION:-deepseek}}"
GEMINI_TARGET="${GEMINI_AGY_TMUX_TARGET:-${GEMINI_AGY_TMUX_SESSION:-agy}}"

DEEPSEEK_NEXT="$ROOT_DIR/.agent_tasks/deepseek_next.md"
DEEPSEEK_RESULT="$ROOT_DIR/.agent_tasks/deepseek_result.md"
GEMINI_NEXT="$ROOT_DIR/.agent_tasks/gemini_agy_next.md"
GEMINI_RESULT="$ROOT_DIR/.agent_tasks/gemini_agy_result.md"

MODE="deepseek"
SEND=false
WAIT=false
TIMEOUT_SEC=900

usage() {
    cat <<'HELP'
Usage: bash scripts/llm_dispatch.sh [deepseek|gemini|both] [--send] [--wait] [--timeout SEC]

Dry-run by default. Prints the exact command that would be sent to each tmux
session. Add --send to actually send it with tmux send-keys.

Environment:
  DEEPSEEK_TMUX_TARGET    default: deepseek
  GEMINI_AGY_TMUX_TARGET  default: agy

Compatibility aliases:
  DEEPSEEK_TMUX_SESSION
  GEMINI_AGY_TMUX_SESSION

Expected fixed files:
  .agent_tasks/deepseek_next.md
  .agent_tasks/deepseek_result.md
  .agent_tasks/gemini_agy_next.md
  .agent_tasks/gemini_agy_result.md

Examples:
  bash scripts/llm_dispatch.sh deepseek
  bash scripts/llm_dispatch.sh deepseek --send --wait
  bash scripts/llm_dispatch.sh gemini --send
  DEEPSEEK_TMUX_TARGET=ds GEMINI_AGY_TMUX_TARGET=0:8.1 bash scripts/llm_dispatch.sh both --send
HELP
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        deepseek|gemini|both)
            MODE="$1"; shift ;;
        --send)
            SEND=true; shift ;;
        --wait)
            WAIT=true; shift ;;
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

ensure_tmux_target() {
    local target="$1"
    if ! tmux display-message -p -t "$target" '#{session_name}:#{window_index}.#{pane_index} #{pane_current_command}' >/dev/null 2>&1; then
        echo "ERROR: tmux target not found: $target" >&2
        echo "Existing panes:" >&2
        tmux list-panes -a -F '#{session_name}:#{window_index}.#{pane_index} #{pane_current_command} #{pane_current_path}' 2>/dev/null || true
        exit 1
    fi
}

ensure_not_plain_shell() {
    local target="$1"
    local command
    command="$(tmux display-message -p -t "$target" '#{pane_current_command}' 2>/dev/null || true)"
    case "$command" in
        bash|sh|zsh|fish)
            if [[ "${LLM_DISPATCH_ALLOW_SHELL_TARGET:-0}" != "1" ]]; then
                echo "ERROR: target '$target' is plain shell command '$command', not an LLM CLI." >&2
                echo "Start the LLM CLI in that pane or set the correct *_TMUX_TARGET." >&2
                echo "Override only if intentional: LLM_DISPATCH_ALLOW_SHELL_TARGET=1" >&2
                exit 1
            fi
            ;;
    esac
}

mtime() {
    local file="$1"
    if [[ -f "$file" ]]; then
        stat -c %Y "$file"
    else
        echo 0
    fi
}

wait_for_result_update() {
    local label="$1"
    local file="$2"
    local before="$3"
    local start
    start="$(date +%s)"

    echo "Waiting for $label result update: $file"
    while true; do
        local now current
        now="$(date +%s)"
        current="$(mtime "$file")"
        if [[ "$current" -gt "$before" ]]; then
            echo "$label result updated."
            return 0
        fi
        if (( now - start >= TIMEOUT_SEC )); then
            echo "ERROR: timed out waiting for $label result update after ${TIMEOUT_SEC}s" >&2
            return 1
        fi
        sleep 5
    done
}

dispatch_one() {
    local label="$1"
    local target="$2"
    local next_file="$3"
    local result_file="$4"
    local prompt
    local before
    local pane_info="(not checked in dry-run)"

    if [[ ! -f "$next_file" ]]; then
        echo "ERROR: missing next file: $next_file" >&2
        exit 1
    fi

    prompt="$next_file 를 읽고 그대로 수행해. 결과는 $result_file 에 작성해. 결과에는 Agent 이름($label)과 완료 시각을 YYYY-MM-DD HH:MM:SS KST 형식으로 초 단위까지 반드시 포함해."
    before="$(mtime "$result_file")"
    if tmux display-message -p -t "$target" '#{session_name}:#{window_index}.#{pane_index} #{pane_current_command} #{pane_current_path}' >/dev/null 2>&1; then
        pane_info="$(tmux display-message -p -t "$target" '#{session_name}:#{window_index}.#{pane_index} #{pane_current_command} #{pane_current_path}')"
    fi

    echo "============================================================"
    echo "Agent   : $label"
    echo "Target  : $target"
    echo "Pane    : $pane_info"
    echo "Next    : $next_file"
    echo "Result  : $result_file"
    echo "Prompt  :"
    echo "$prompt"
    echo "============================================================"

    if ! $SEND; then
        return 0
    fi

    ensure_tmux_target "$target"
    ensure_not_plain_shell "$target"
    tmux send-keys -t "$target" "$prompt" C-m

    if $WAIT; then
        wait_for_result_update "$label" "$result_file" "$before"
    fi
}

case "$MODE" in
    deepseek)
        dispatch_one "DeepSeek" "$DEEPSEEK_TARGET" "$DEEPSEEK_NEXT" "$DEEPSEEK_RESULT"
        ;;
    gemini)
        dispatch_one "Gemini/AGY" "$GEMINI_TARGET" "$GEMINI_NEXT" "$GEMINI_RESULT"
        ;;
    both)
        dispatch_one "DeepSeek" "$DEEPSEEK_TARGET" "$DEEPSEEK_NEXT" "$DEEPSEEK_RESULT"
        dispatch_one "Gemini/AGY" "$GEMINI_TARGET" "$GEMINI_NEXT" "$GEMINI_RESULT"
        ;;
esac
