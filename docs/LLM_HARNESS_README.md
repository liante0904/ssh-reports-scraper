# LLM 운영 하네스 한 장 요약

목적: Codex, DeepSeek, Gemini/AGY, 새 LLM이 들어와도 같은 `.sh`와 같은 파일 큐로 작업한다.

## 1. 읽는 순서

새 LLM이나 새 작업자가 들어오면 이 파일만 먼저 읽는다.

상세 문서:

| 문서 | 용도 |
|---|---|
| `docs/LLM_DELEGATION_PROTOCOL.md` | next/result 파일 계약과 역할 상세 |
| `docs/LLM_DISPATCH_AUTOMATION.md` | tmux 자동 송신 상세 |
| `docs/LLM_HARNESS_PORTING_GUIDE.md` | 다른 repo로 복사하는 절차 |
| `docs/OPS_LOG_TAIL.md` | OCI 운영 로그 조회 |
| `docs/LLM_CONTROL_HARNESS.md` | scraper 특화 장애 패턴과 검증 명령 |

## 2. 고정 파일

LLM 지시와 결과는 이 네 파일만 사용한다.

```text
.agent_tasks/deepseek_next.md
.agent_tasks/deepseek_result.md
.agent_tasks/gemini_agy_next.md
.agent_tasks/gemini_agy_result.md
```

새 LLM이 와도 파일을 늘리지 않는다. DeepSeek 역할 또는 Gemini/AGY 역할에 매핑한다.

`.agent_tasks/`는 커밋하지 않는다.

작업이 둘 이상이면 사람이 `_next.md`를 직접 나눠 쓰지 않는다. JSON 원장을 먼저 만들고 렌더링한다.

```text
.agent_tasks/llm_task_queue.json
```

이 JSON도 커밋하지 않는다.

## 3. 역할

| 역할 | 현재 도구 | 맡길 일 | 금지 |
|---|---|---|---|
| Codex | Codex | 작업 분해, 최종 판단, diff 리뷰, 위험한 변경 | 반복 조사에 토큰 낭비 |
| DeepSeek | `cc` | 조사, 작은 구현, 검증, 브랜치 작업 | 승인 없는 main merge, 배포, DB write |
| Gemini/AGY | `agy` | 요약, 문장 정리, 체크리스트 | 코드 수정, 명령 실행, git 조작 |

## 4. tmux 표준

DeepSeek:

```bash
tmux attach -t deepseek
cd /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper
cc
```

Gemini/AGY:

```bash
tmux attach -t agy
cd /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper
agy
```

pane 확인:

```bash
tmux list-panes -a -F '#{session_name}:#{window_index}.#{pane_index} #{pane_current_command} #{pane_current_path}'
```

현재 reports scraper 표준 실행:

```bash
LLM_DISPATCH_ALLOW_SHELL_TARGET=1 \
DEEPSEEK_TMUX_TARGET='deepseek' \
GEMINI_AGY_TMUX_TARGET='0:8.1' \
bash scripts/llm_cycle.sh
```

검증만:

```bash
bash scripts/llm_cycle.sh --dry-run
```

## 5. JSON 원장 병렬 작업

여러 작업을 나눌 때는 한 개 JSON에 DeepSeek/Gemini 작업을 같이 적는다.

초기 템플릿:

```bash
python3 scripts/llm_task_queue.py --init
```

JSON에서 `status: "ready"`인 작업을 기존 next 파일로 렌더링:

```bash
python3 scripts/llm_task_queue.py --render
```

양쪽 LLM에 병렬 송신:

```bash
LLM_DISPATCH_ALLOW_SHELL_TARGET=1 \
DEEPSEEK_TMUX_TARGET='deepseek' \
GEMINI_AGY_TMUX_TARGET='0:8.1' \
bash scripts/llm_cycle.sh --parallel
```

AGY 토큰이 없으면 Gemini 작업은 `status: "blocked"`로 두고 DeepSeek만 렌더링/송신한다.

LLM 효율 기준:

- 한 agent에 `ready` 작업은 한 번에 하나만 둔다.
- `objective`는 한 문장으로 쓴다.
- `max_scope`로 작업 범위를 닫는다.
- `allowed_files`와 `forbidden_files`를 반드시 채운다.
- `acceptance_criteria`에는 사람이 판정할 완료 조건을 적는다.
- 운영 DB write, 배포, main merge는 JSON 작업에 넣지 않는다.

이 구조가 효율적인 이유:

- LLM은 긴 배경 설명보다 닫힌 파일 목록과 완료 기준을 더 잘 따른다.
- 병렬 작업은 한 JSON에서 관리하되, 실행은 기존 `_next.md` 파일로 유지한다.
- 결과 파일 계약이 그대로라 기존 tmux/복붙 운영과 충돌하지 않는다.
- AGY처럼 약한 모델은 코드가 아니라 요약/문장 정리 작업만 받게 제한할 수 있다.

## 6. 운영 로그

운영 로그는 직접 긴 SSH 명령을 만들지 않고 이 스크립트로 본다.

```bash
bash scripts/ops_tail_errors.sh --since "09:00"
bash scripts/ops_tail_errors.sh --docker-only --watchdog
bash scripts/ops_tail_errors.sh --service ssh-reports-scraper-main-scraper-green
```

이 스크립트는 읽기 전용이다. restart, DB write, 파일 삭제를 넣지 않는다.

## 7. 로그 DB 저장 정책

전체 로그를 DB에 저장하지 않는다.

이유:

- Docker/file 로그는 이미 원본 저장소가 있다.
- 전체 로그 DB 적재는 용량과 노이즈가 크다.
- 검색/보관 목적이면 Dozzle, Loki, 파일 압축, grep이 더 싸다.
- 장애 처리에 필요한 것은 원문 전체가 아니라 사건 단위 요약이다.

DB에 저장해도 되는 것:

```text
incident_id
detected_at
service_name
severity
fingerprint
first_seen_at
last_seen_at
count
status: open | mitigated | resolved | ignored
owner
summary
resolution_note
source_log_ref
```

즉, 원문 로그 DB가 아니라 장애 이벤트/해결 상태 DB만 만든다.

## 8. 자동화 금지

아래는 LLM 자동 cycle에 넣지 않는다.

- 운영 DB write/delete
- 서비스 restart
- 배포
- main merge
- secret/env 출력 가능성이 있는 조사
- 파일 삭제
- 여러 repo 동시 수정

이 작업은 Codex가 직접 처리하거나 사용자 승인 후 단일 명령으로 실행한다.

## 9. 다른 repo에 복사

```bash
cd <target-repo>
mkdir -p scripts docs .agent_tasks
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/scripts/llm_dispatch.sh scripts/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/scripts/llm_cycle.sh scripts/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/scripts/llm_task_queue.py scripts/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/scripts/ops_tail_errors.sh scripts/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/docs/LLM_HARNESS_README.md docs/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/docs/LLM_DELEGATION_PROTOCOL.md docs/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/docs/LLM_DISPATCH_AUTOMATION.md docs/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/docs/LLM_HARNESS_PORTING_GUIDE.md docs/
cp /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/docs/OPS_LOG_TAIL.md docs/
touch .agent_tasks/deepseek_next.md .agent_tasks/deepseek_result.md
touch .agent_tasks/gemini_agy_next.md .agent_tasks/gemini_agy_result.md
```
