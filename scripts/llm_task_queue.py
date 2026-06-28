#!/usr/bin/env python3
"""Render one JSON LLM task queue into the fixed .agent_tasks/*_next.md files."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any


KST = timezone(timedelta(hours=9))
ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_QUEUE = ROOT_DIR / ".agent_tasks" / "llm_task_queue.json"
NEXT_FILE_NAMES = {
    "deepseek": "deepseek_next.md",
    "gemini": "gemini_agy_next.md",
}
AGENT_LABELS = {
    "deepseek": "DeepSeek",
    "gemini": "Gemini/AGY",
}


TEMPLATE: dict[str, Any] = {
    "batch_id": "YYYYMMDD-HHMM-topic",
    "mode": "parallel",
    "tasks": [
        {
            "agent": "deepseek",
            "status": "ready",
            "priority": "P1",
            "title": "구현/검증 작업 제목",
            "objective": "작업 목표를 한 문장으로 적는다.",
            "max_scope": "작업 범위를 한 문장으로 제한한다. 예: 문서/스크립트만, 특정 firm만.",
            "allowed_files": [
                "수정해도 되는 파일 경로",
            ],
            "forbidden_files": [
                "수정하면 안 되는 파일 경로 또는 패턴",
            ],
            "context": [
                "읽어야 할 파일 또는 현재 상황",
            ],
            "instructions": [
                "수행할 일을 순서대로 적는다.",
            ],
            "validation": [
                "실행할 검증 명령을 적는다.",
            ],
            "acceptance_criteria": [
                "완료 판정 기준을 적는다.",
            ],
            "constraints": [
                "main merge, push, 배포, DB write 금지처럼 안전 경계를 적는다.",
            ],
            "deliverable": "결과 파일에 요약할 내용을 적는다.",
        },
        {
            "agent": "gemini",
            "status": "blocked",
            "priority": "P3",
            "title": "문서 정리 작업 제목",
            "objective": "AGY 토큰이 없으면 blocked로 둔다.",
            "max_scope": "문장 정리만",
            "allowed_files": [],
            "forbidden_files": ["코드 파일 전체"],
            "context": [],
            "instructions": [],
            "validation": [],
            "acceptance_criteria": [],
            "constraints": ["코드 수정 금지"],
            "deliverable": "결과 파일에 요약할 내용을 적는다.",
        },
    ],
}


def load_queue(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit("ERROR: queue root must be a JSON object")
    if not isinstance(data.get("tasks"), list):
        raise SystemExit("ERROR: queue.tasks must be a list")
    return data


def normalize_agent(agent: str) -> str:
    value = agent.strip().lower()
    if value in {"deepseek", "ds"}:
        return "deepseek"
    if value in {"gemini", "agy", "gemini/agy"}:
        return "gemini"
    raise SystemExit(f"ERROR: unsupported agent: {agent}")


def list_block(title: str, values: list[Any]) -> str:
    if not values:
        return f"## {title}\n\n- 없음\n"
    lines = [f"## {title}", ""]
    for value in values:
        lines.append(f"- {value}")
    return "\n".join(lines) + "\n"


def render_task(queue: dict[str, Any], task: dict[str, Any]) -> str:
    agent = normalize_agent(str(task.get("agent", "")))
    generated_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    batch_id = queue.get("batch_id", "")
    mode = queue.get("mode", "")
    title = task.get("title", "")
    priority = task.get("priority", "")
    objective = task.get("objective", "")
    deliverable = task.get("deliverable", "결과를 result 파일에 요약한다.")

    return "\n".join(
        [
            f"# {AGENT_LABELS[agent]} 작업 지시",
            "",
            f"- Generated At: {generated_at}",
            f"- Batch ID: {batch_id}",
            f"- Queue Mode: {mode}",
            f"- Priority: {priority}",
            f"- Title: {title}",
            "",
            "## 목표",
            "",
            str(objective),
            "",
            f"## 최대 범위\n\n{task.get('max_scope') or '명시된 파일과 지시 범위 안에서만 작업한다.'}\n",
            list_block("수정 허용 파일", task.get("allowed_files") or []),
            list_block("수정 금지 파일", task.get("forbidden_files") or []),
            list_block("맥락", task.get("context") or []),
            list_block("수행 지시", task.get("instructions") or []),
            list_block("검증", task.get("validation") or []),
            list_block("완료 기준", task.get("acceptance_criteria") or []),
            list_block("제약", task.get("constraints") or []),
            "## 결과 작성",
            "",
            str(deliverable),
            "",
            f"결과는 지정된 result 파일에만 작성한다. Agent 이름({AGENT_LABELS[agent]})과 완료 시각을 YYYY-MM-DD HH:MM:SS KST 형식으로 반드시 포함한다.",
            "",
        ]
    )


def render(queue_path: Path, selected_agent: str, output_dir: Path) -> int:
    queue = load_queue(queue_path)
    wanted = {"deepseek", "gemini"} if selected_agent == "all" else {normalize_agent(selected_agent)}
    ready_by_agent: dict[str, list[dict[str, Any]]] = {agent: [] for agent in wanted}

    for task in queue["tasks"]:
        if not isinstance(task, dict):
            raise SystemExit("ERROR: every task must be an object")
        agent = normalize_agent(str(task.get("agent", "")))
        status = str(task.get("status", "ready")).lower()
        if agent in wanted and status == "ready":
            ready_by_agent.setdefault(agent, []).append(task)

    rendered = 0
    for agent, tasks in ready_by_agent.items():
        if len(tasks) > 1:
            raise SystemExit(f"ERROR: multiple ready tasks for {agent}; keep one task per agent per dispatch")
        if not tasks:
            continue
        output_path = output_dir / NEXT_FILE_NAMES[agent]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_task(queue, tasks[0]), encoding="utf-8")
        print(f"rendered {agent}: {output_path}")
        rendered += 1

    if rendered == 0:
        print("no ready tasks rendered")
    return 0


def init_queue(path: Path) -> int:
    if path.exists():
        raise SystemExit(f"ERROR: queue already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(TEMPLATE, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"created {path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default=str(DEFAULT_QUEUE), help="JSON queue path")
    parser.add_argument("--output-dir", default=str(ROOT_DIR / ".agent_tasks"), help="directory for rendered next md files")
    parser.add_argument("--agent", choices=["all", "deepseek", "gemini"], default="all")
    parser.add_argument("--init", action="store_true", help="create a template queue")
    parser.add_argument("--render", action="store_true", help="render ready JSON tasks into next md files")
    args = parser.parse_args()

    queue_path = Path(args.queue).resolve()
    if args.init:
        return init_queue(queue_path)
    if args.render:
        return render(queue_path, args.agent, Path(args.output_dir).resolve())
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
