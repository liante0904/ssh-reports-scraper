# 하위 LLM 단계적 작업 프로토콜

목적: Codex가 매번 긴 조사/지시문을 새로 작성하지 않도록, DeepSeek와 Gemini/AGY가 작은 단위 작업을 단계적으로 수행하는 표준 절차를 고정한다.

처음 읽는 사람은 `docs/LLM_HARNESS_README.md`를 먼저 읽고, 이 문서는 상세 계약이 필요할 때 읽는다.

## 고정 파일

LLM 지시와 결과는 아래 네 파일만 사용한다.

```text
.agent_tasks/deepseek_next.md
.agent_tasks/deepseek_result.md
.agent_tasks/gemini_agy_next.md
.agent_tasks/gemini_agy_result.md
```

새로운 `_next.md`, `_result.md`, 임시 markdown 파일을 만들지 않는다.

## 에이전트 역할

### DeepSeek

용도:

- 코드/워크플로우/로그 조사
- 작은 범위 구현
- 검증 명령 실행
- 브랜치 생성, 커밋, push
- 다음 작업 후보 제안

금지:

- 사용자 승인 없는 `main` merge
- 사용자 승인 없는 운영 DB write
- 사용자 승인 없는 서비스 restart
- `.agent_tasks/` 커밋

### Gemini/AGY

용도:

- 사람이 읽을 승인 문구 작성
- 결과 요약
- 체크리스트 정리
- 문서 일관성 검토
- DeepSeek 작업 결과의 위험 포인트 요약

금지:

- 코드 수정
- git 조작
- 명령 실행
- 새 markdown 생성

## 기본 실행 문구

DeepSeek:

```text
/home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/.agent_tasks/deepseek_next.md 를 읽고 그대로 수행해. 결과는 /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/.agent_tasks/deepseek_result.md 에 작성해. 결과에는 Agent 이름(DeepSeek)과 완료 시각을 YYYY-MM-DD HH:MM:SS KST 형식으로 초 단위까지 반드시 포함해.
```

Gemini/AGY:

```text
/home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/.agent_tasks/gemini_agy_next.md 를 읽고 그대로 수행해. 결과는 /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/.agent_tasks/gemini_agy_result.md 에 작성해. 결과에는 Agent 이름(Gemini/AGY)과 완료 시각을 YYYY-MM-DD HH:MM:SS KST 형식으로 초 단위까지 반드시 포함해.
```

## tmux 자동 송신

사용자가 직접 복붙하지 않으려면 `scripts/llm_dispatch.sh`를 사용한다.

기본은 dry-run이다.

```bash
bash scripts/llm_dispatch.sh deepseek
bash scripts/llm_dispatch.sh gemini
```

실제 송신:

```bash
bash scripts/llm_dispatch.sh deepseek --send --wait
bash scripts/llm_dispatch.sh gemini --send --wait
```

세부 규칙은 `docs/LLM_DISPATCH_AUTOMATION.md`를 따른다.

다른 repo/도메인에 같은 하네스를 복사할 때는 `docs/LLM_HARNESS_PORTING_GUIDE.md`를 따른다.

자동 송신 금지:

- 운영 DB write
- 서비스 restart
- main merge
- 배포
- LLM CLI가 입력 대기 중인지 확인되지 않은 상태

## DeepSeek 작업서 템플릿

```markdown
# DeepSeek Next Task

## Agent

DeepSeek

## Requested At

YYYY-MM-DD HH:MM:SS KST

## Task Type

조사 | 구현 | 검증 | 문서화 | 배포준비

## Goal

한 문단으로 목표를 쓴다.

## Inputs

읽을 파일과 확인할 명령만 적는다.

## Required Changes

수정이 필요한 파일과 범위를 좁게 적는다.

## Forbidden

하지 말아야 할 일을 명시한다.

## Validation

반드시 실행할 검증 명령을 적는다.

## Commit / Push

브랜치명, 커밋 메시지, push 여부를 적는다.
main merge는 명시 승인 전 금지한다.

## Output

결과는 `.agent_tasks/deepseek_result.md`에만 작성한다.
```

## 결과 형식

DeepSeek 결과에는 반드시 포함한다.

```text
Agent:
Completed At:
Branch:
Commit:
Changed Files:
Validation:
Main touched:
Blockers:
Next Recommended Step:
```

Gemini/AGY 결과에는 반드시 포함한다.

```text
Agent:
Completed At:
요약:
사람이 확인할 것:
중단 조건:
승인 문장:
```

## 운영 로그 조사 규칙

운영 로그가 필요한 작업은 먼저 아래 명령으로 시작한다.

```bash
bash scripts/ops_tail_errors.sh --since "06:00"
```

범위를 좁힐 때:

```bash
bash scripts/ops_tail_errors.sh --scraper --since "09:00"
bash scripts/ops_tail_errors.sh --watchdog --since "09:00"
bash scripts/ops_tail_errors.sh --service ssh-reports-scraper-main-scraper-green --since "09:00"
```

세부 사용법은 `docs/OPS_LOG_TAIL.md`를 따른다.

## 단계적 진행 규칙

1. DeepSeek가 조사/구현을 수행한다.
2. Gemini/AGY가 결과를 사람이 읽을 형태로 압축한다.
3. Codex가 결과 파일과 git diff만 검토한다.
4. 통과하면 Codex가 main merge/운영 배포 여부를 판단한다.
5. 다음 작업이 있으면 기존 `_next.md`를 덮어쓴다.

## Codex 토큰 절약형 큐

Codex는 매번 전체 코드를 길게 재분석하지 않고, 하위 LLM에게 아래 순서로 작은 산출물을 만들게 한다.

### 1단계: DeepSeek 조사 큐

DeepSeek에게 맡기는 작업:

- 특정 장애나 기술부채 후보를 파일 단위로 조사
- 영향 파일, 호출부, 테스트 범위 목록화
- 작은 브랜치로 나눌 수 있는 첫 작업 제안
- 실행 가능한 검증 명령 제안

DeepSeek에게 맡기지 않는 작업:

- 승인 없는 main merge
- 승인 없는 운영 DB write
- 넓은 범위 리팩터링
- `.agent_tasks/` 외 임시 지시 문서 생성

### 2단계: Gemini/AGY 압축 큐

Gemini/AGY에게 맡기는 작업:

- DeepSeek 결과를 사람이 승인하기 쉬운 문장으로 축약
- 위험도, 중단 조건, 승인 문장 정리
- 문서 표현 정리
- 체크리스트 형식화

Gemini/AGY에게 맡기지 않는 작업:

- 코드 수정
- 명령 실행
- git 조작
- 실제 장애 원인 최종 판정

### 3단계: Codex 최종 판단 큐

Codex는 아래 입력만 보고 판단하는 것을 목표로 한다.

```text
1. deepseek_result.md
2. gemini_agy_result.md
3. git diff --stat
4. 필요한 경우 해당 diff의 핵심 파일 일부
```

Codex가 직접 토큰을 많이 써야 하는 경우:

- 운영 배포 여부 판단
- DB write/delete 여부 판단
- 브랜치 merge 충돌 해결
- 테스트 실패 원인 분석
- 보안/비밀정보/권한 관련 변경

### 작업 쪼개기 기준

하위 LLM에게 넘기는 단위는 아래 중 하나로 제한한다.

- 문서화만
- 조사만
- 테스트 추가만
- 단일 함수 rename 후보 조사만
- 단일 firm 장애 원인 조사만
- 단일 스크립트 검증만

한 번에 금지하는 묶음:

- 조사 + 대규모 구현 + 배포
- 여러 firm 동시 수정
- DB schema 변경 + 운영 데이터 수정
- workflow 변경 + scraper 구조 변경 + 배포

### 결과가 부족할 때 재귀 지시

결과가 부족하면 새 파일을 만들지 말고 같은 `_next.md`를 덮어쓴다.

재지시 문장은 짧게 쓴다.

```text
이전 result의 누락 항목만 보완한다. 코드 수정 금지. 결과는 같은 result 파일에 덮어쓴다.
```

## 함수명/구조 리팩터링 후보 관리

애매한 함수명이나 구조 위험을 발견하면 바로 대규모 리팩터링하지 않는다.

먼저 DeepSeek에게 아래 형식으로 후보를 정리시킨다.

```text
현재 이름:
문제:
추천 이름:
영향 파일:
호출부 수:
테스트 필요 범위:
우선순위:
```

후보가 쌓이면 Codex가 우선순위를 정하고 작은 브랜치로 분리한다.
