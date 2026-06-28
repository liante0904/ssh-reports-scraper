# LLM tmux 자동 송신 가이드

목적: 사용자가 DeepSeek와 Gemini/AGY 터미널을 번갈아 보며 복붙하는 부담을 줄인다.

## 전제

이 방식은 LLM CLI가 `tmux` 세션 안에서 이미 실행 중일 때만 동작한다.

권장 세션명:

```bash
tmux new -s deepseek
tmux new -s agy
```

다른 세션명이나 특정 pane을 쓰면 환경변수로 지정한다.

```bash
export DEEPSEEK_TMUX_TARGET=ds
export GEMINI_AGY_TMUX_TARGET=0:8.1
```

현재 pane 목록 확인:

```bash
tmux list-panes -a -F '#{session_name}:#{window_index}.#{pane_index} #{pane_current_command} #{pane_current_path}'
```

`deepseek`, `agy`라는 이름의 세션이 있어도 그 안이 `bash`이면 자동 송신 대상이 아니다. 대상 pane에는 DeepSeek/Gemini CLI가 입력 대기 상태로 떠 있어야 한다.

## 고정 파일

자동 송신도 기존 규칙을 그대로 따른다.

```text
.agent_tasks/deepseek_next.md
.agent_tasks/deepseek_result.md
.agent_tasks/gemini_agy_next.md
.agent_tasks/gemini_agy_result.md
```

새 `_next.md`, `_result.md` 파일을 만들지 않는다.

## Dry-Run

기본은 전송하지 않고 보낼 문구만 출력한다.

```bash
bash scripts/llm_dispatch.sh deepseek
bash scripts/llm_dispatch.sh gemini
```

## 실제 송신

세션명이 맞는지 확인한 뒤 `--send`를 붙인다.

```bash
bash scripts/llm_dispatch.sh deepseek --send
bash scripts/llm_dispatch.sh gemini --send
```

결과 파일 갱신까지 기다리려면:

```bash
bash scripts/llm_dispatch.sh deepseek --send --wait
bash scripts/llm_dispatch.sh gemini --send --wait
```

timeout 조정:

```bash
bash scripts/llm_dispatch.sh deepseek --send --wait --timeout 1800
```

## 순차 실행

DeepSeek 완료 후 Gemini/AGY를 실행하는 흐름:

```bash
bash scripts/llm_dispatch.sh deepseek --send --wait
bash scripts/llm_dispatch.sh gemini --send --wait
```

같은 흐름을 한 번에 실행:

```bash
bash scripts/llm_cycle.sh
```

target 지정 예:

```bash
DEEPSEEK_TMUX_TARGET='0:7.1' GEMINI_AGY_TMUX_TARGET='0:8.1' bash scripts/llm_cycle.sh
```

`both --send`는 두 세션에 연속 송신한다. Gemini/AGY가 DeepSeek result를 필요로 하는 작업이면 사용하지 않는다.

```bash
bash scripts/llm_dispatch.sh both --send
```

## 실패 조건

아래 상황이면 자동 송신하지 말고 수동 확인한다.

- `tmux list-sessions`에 대상 세션이 없다.
- target pane의 `pane_current_command`가 `bash`, `sh`, `zsh`, `fish` 같은 일반 shell이다.
- LLM CLI가 프롬프트 입력 대기 상태가 아니다.
- 이전 작업이 아직 실행 중이다.
- result 파일이 갱신되지 않는다.
- 운영 DB write, 서비스 restart, main merge, 배포가 포함된 작업이다.

## Codex 사용 방식

Codex는 `_next.md`를 갱신한 뒤 아래 명령을 실행하면 된다.

```bash
bash scripts/llm_dispatch.sh deepseek --send --wait
```

다만 운영 변경이 포함된 작업은 자동 송신보다 사용자 승인 후 수동 송신을 우선한다.

## 흔한 실패

잘못된 구조:

```text
deepseek tmux 세션 안에서 bash scripts/llm_dispatch.sh deepseek --send --wait 실행
```

이 경우 dispatcher가 LLM CLI가 아니라 자기 shell에 프롬프트를 보내므로 동작하지 않는다.

`scripts/llm_dispatch.sh`는 기본적으로 일반 shell target에는 전송하지 않는다. 정말 의도한 경우에만 아래처럼 강제로 허용한다.

```bash
LLM_DISPATCH_ALLOW_SHELL_TARGET=1 bash scripts/llm_dispatch.sh deepseek --send
```

올바른 구조:

```text
deepseek target pane: DeepSeek CLI 입력 대기
agy target pane: Gemini/AGY CLI 입력 대기
별도 shell 또는 Codex: bash scripts/llm_dispatch.sh ...
```
