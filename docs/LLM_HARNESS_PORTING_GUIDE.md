# LLM 하네스 포팅 가이드

목적: 다른 LLM, 다른 repo, 다른 도메인이 들어와도 같은 파일 큐와 `.sh` 도구로 작업을 통제한다.

처음 읽는 사람은 `docs/LLM_HARNESS_README.md`를 먼저 읽고, 이 문서는 복사/포팅 작업 때만 읽는다.

## 핵심 원칙

하네스는 LLM 종류가 아니라 작업 계약을 고정한다.

고정 계약:

```text
.agent_tasks/deepseek_next.md
.agent_tasks/deepseek_result.md
.agent_tasks/gemini_agy_next.md
.agent_tasks/gemini_agy_result.md
```

새 LLM이 들어와도 새 `_next.md`, `_result.md`를 늘리지 않는다. 역할만 바꾼다.

## 현재 표준 역할

| 역할 | 현재 도구 | 책임 | 기본 금지 |
|---|---|---|---|
| Codex | Codex CLI | 작업 분해, 최종 판단, diff 리뷰, 위험 작업 직접 처리 | 반복 조사에 토큰 낭비 |
| DeepSeek | `cc` alias 기반 Claude Code CLI + DeepSeek API | 조사, 작은 구현, 검증, 브랜치 작업 | 승인 없는 main merge/배포/DB write |
| Gemini/AGY | `agy` CLI | 요약, 문장 정리, 체크리스트, read-only 검토 | 코드 수정, 명령 실행, git 조작 |

다른 LLM을 붙일 때도 위 역할 중 하나에 매핑한다.

예:

```text
새 저가 coding agent -> DeepSeek 역할에 매핑
새 문서 요약 agent -> Gemini/AGY 역할에 매핑
고성능 reviewer -> Codex가 직접 쓰되 next/result 파일은 늘리지 않음
```

## repo에 설치할 최소 파일

대상 repo 루트:

```text
scripts/llm_dispatch.sh
scripts/llm_cycle.sh
scripts/llm_task_queue.py
docs/LLM_DELEGATION_PROTOCOL.md
docs/LLM_DISPATCH_AUTOMATION.md
docs/LLM_HARNESS_PORTING_GUIDE.md
.agent_tasks/
```

`.agent_tasks/` 안:

```text
deepseek_next.md
deepseek_result.md
gemini_agy_next.md
gemini_agy_result.md
llm_task_queue.json
```

`.agent_tasks/`는 커밋하지 않는다.

```gitignore
.agent_tasks/
```

## 새 도메인에 복사

```bash
cd <target-repo>
mkdir -p scripts docs .agent_tasks

cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/scripts/llm_dispatch.sh scripts/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/scripts/llm_cycle.sh scripts/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/scripts/llm_task_queue.py scripts/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/docs/LLM_DELEGATION_PROTOCOL.md docs/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/docs/LLM_DISPATCH_AUTOMATION.md docs/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/docs/LLM_HARNESS_PORTING_GUIDE.md docs/

touch .agent_tasks/deepseek_next.md
touch .agent_tasks/deepseek_result.md
touch .agent_tasks/gemini_agy_next.md
touch .agent_tasks/gemini_agy_result.md
python3 scripts/llm_task_queue.py --init
```

## tmux 표준

DeepSeek 역할:

```bash
tmux new -s deepseek
cd <target-repo>
cc
```

`cc`는 Claude Code CLI에 DeepSeek API를 물린 alias다.

Gemini/AGY 역할:

```bash
tmux new -s agy
cd <target-repo>
agy
```

pane 확인:

```bash
tmux list-panes -a -F '#{session_name}:#{window_index}.#{pane_index} #{pane_current_command} #{pane_current_path}'
```

## 실행

현재 repo 표준:

```bash
LLM_DISPATCH_ALLOW_SHELL_TARGET=1 \
DEEPSEEK_TMUX_TARGET='deepseek' \
GEMINI_AGY_TMUX_TARGET='0:8.1' \
bash scripts/llm_cycle.sh
```

dry-run:

```bash
bash scripts/llm_cycle.sh --dry-run
```

JSON 원장 렌더링:

```bash
python3 scripts/llm_task_queue.py --render
```

독립 작업 병렬 실행:

```bash
bash scripts/llm_cycle.sh --parallel
```

개별 실행:

```bash
bash scripts/llm_dispatch.sh deepseek --send --wait
bash scripts/llm_dispatch.sh gemini --send --wait
```

## 새 LLM을 붙일 때

1. 새 LLM을 tmux pane에서 입력 대기 상태로 띄운다.
2. `tmux list-panes -a`로 target을 확인한다.
3. DeepSeek 역할이면 `DEEPSEEK_TMUX_TARGET`에 연결한다.
4. Gemini/AGY 역할이면 `GEMINI_AGY_TMUX_TARGET`에 연결한다.
5. next/result 파일명은 바꾸지 않는다.

예:

```bash
DEEPSEEK_TMUX_TARGET='new-agent:1.1' bash scripts/llm_dispatch.sh deepseek --send --wait
GEMINI_AGY_TMUX_TARGET='doc-agent:1.1' bash scripts/llm_dispatch.sh gemini --send --wait
```

## 자동 송신 금지 작업

아래 작업은 자동 cycle에 넣지 않는다.

- 운영 DB write/delete
- 서비스 restart
- 배포
- main merge
- secret/env 출력 가능성이 있는 조사
- 파일 삭제
- 여러 repo 동시 수정

이 작업들은 Codex가 직접 처리하거나 사용자 승인 후 단일 명령으로 실행한다.

## result 판정 기준

DeepSeek result에는 최소한 아래가 있어야 한다.

```text
Agent:
Completed At:
Changed Files:
Validation:
Blocked:
Next Recommended Step:
```

Gemini/AGY result에는 최소한 아래가 있어야 한다.

```text
Agent:
Completed At:
Summary:
Remaining Risk:
Next Action:
Stop Conditions:
```

## 흔한 실패

### target이 bash

증상:

```text
ERROR: target 'deepseek' is plain shell command 'bash', not an LLM CLI.
```

해결:

```bash
tmux attach -t deepseek
cd <target-repo>
cc
```

`cc`가 shell 안에서 떠 있어서 `pane_current_command`가 `claude`나 `bash`로 보이는 경우만 아래를 허용한다.

```bash
LLM_DISPATCH_ALLOW_SHELL_TARGET=1 ...
```

### Gemini가 DeepSeek보다 먼저 실행됨

Gemini/AGY는 DeepSeek result를 읽는 요약 역할이다. DeepSeek result가 최신이 아니면 `Plan/result not ready`로 끝나는 것이 정상이다.

### result 파일이 갱신되지 않음

확인:

```bash
tmux capture-pane -t <target> -p -S -80
stat .agent_tasks/*_result.md
```

LLM이 승인 대기 중이거나 CLI가 입력 대기 상태가 아닐 수 있다.
