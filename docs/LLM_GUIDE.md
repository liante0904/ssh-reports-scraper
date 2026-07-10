# LLM Agent Delegation Protocol & Guidelines

> **통합 일자**: 2026-07-01
> **대상 원본 문서**: LLM_DELEGATION_PROTOCOL.md, LLM_PAIN_POINTS.md

---

## [통합 섹션] LLM_DELEGATION_PROTOCOL

# 하위 LLM 단계적 작업 프로토콜

목적: Codex가 매번 긴 조사/지시문을 새로 작성하지 않도록, DeepSeek와 Gemini/AGY가 작은 단위 작업을 단계적으로 수행하는 표준 절차를 고정한다.

처음 읽는 사람은 `docs/DEBUG_ENTRYPOINTS.md`를 먼저 읽고, 이 문서는 상세 계약이 필요할 때 읽는다.

## 고정 파일

LLM 지시와 결과는 아래 네 파일만 사용한다. **JSON 우선** (토큰 절약, 키 짧게, 중복 없이, 구조화).

```text
.agent_tasks/deepseek_next.json   ← DeepSeek 작업 지시 (JSON)
.agent_tasks/deepseek_result.json ← DeepSeek 작업 결과 (JSON)
.agent_tasks/gemini_agy_next.json ← Gemini/AGY 작업 지시 (JSON)
.agent_tasks/gemini_agy_result.json ← Gemini/AGY 작업 결과 (JSON)
```

기존 `.md` 파일은 deprecated — 새로운 작업은 항상 `.json`으로 작성한다.

### JSON 스키마 (compact)

**Task file** (`_next.json`):
```json
{
  "agt": "deepseek|gemini",
  "ver": "ISO8601 KST",
  "typ": "impl|investigate|audit|doc|review",
  "goal": "한문장 목표",
  "ctx": "배경 컨텍스트 (짧게)",
  "src": ["읽기전용 파일목록"],
  "mod": [{"f": "파일경로", "do": "무엇을 바꾸는지"}],
  "ban": ["금지파일", "op:db_write", "op:deploy", "op:main_merge", "op:push"],
  "tst": ["검증명령어"],
  "br": "브랜치명",
  "msg": "커밋메시지",
  "out": "결과파일경로"
}
```

Key 약어:
- `agt`=agent, `ver`=version/ts, `typ`=type, `ctx`=context
- `src`=source(read-only), `mod`=modifications, `ban`=forbidden
- `tst`=tests, `br`=branch, `msg`=commit message, `out`=output

**Result file** (`_result.json`):
```json
{
  "agt": "deepseek|gemini",
  "ts": "ISO8601 KST",
  "sum": "한문단 요약",
  "files": ["변경된 파일 목록"],
  "ok": true,
  "tst": [{"cmd": "검증명령어", "ok": true}],
  "blk": [{"what": "블로커 설명"}],
  "next": "다음 추천 작업"
}
```

새로운 `_next.json`, `_result.json`, 임시 markdown 파일을 만들지 않는다.

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
/home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/.agent_tasks/deepseek_next.json 을 읽고 그대로 수행해. 결과는 /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/.agent_tasks/deepseek_result.json 에 JSON으로 작성해. "agt":"deepseek","ts":"ISO8601 KST" 반드시 포함.
```

Gemini/AGY:

```text
/home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/.agent_tasks/gemini_agy_next.json 을 읽고 그대로 수행해. 결과는 /home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper/.agent_tasks/gemini_agy_result.json 에 JSON으로 작성해. "agt":"gemini","ts":"ISO8601 KST" 반드시 포함.
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

세부 규칙과 포팅 이력은 통합 문서 `docs/LLM_HARNESS.md`를 따른다.

자동 송신 금지:

- 운영 DB write
- 서비스 restart
- main merge
- 배포
- LLM CLI가 입력 대기 중인지 확인되지 않은 상태

## DeepSeek 작업서 템플릿 (JSON)

`.agent_tasks/deepseek_next.json`:
```json
{
  "agt": "deepseek",
  "ver": "ISO8601 KST",
  "typ": "impl|investigate|doc|review",
  "goal": "한 문단으로 목표",
  "ctx": "배경 컨텍스트",
  "src": ["읽을 파일"],
  "mod": [{"f": "파일", "do": "변경내용"}],
  "ban": ["금지파일", "op:db_write", "op:deploy", "op:main_merge"],
  "tst": ["검증명령어"],
  "br": "브랜치명",
  "msg": "커밋메시지",
  "out": ".agent_tasks/deepseek_result.json"
}
```

## 결과 형식 (JSON)

DeepSeek 결과 (`deepseek_result.json`):
```json
{
  "agt": "deepseek",
  "ts": "ISO8601 KST",
  "sum": "한문단 요약",
  "br": "브랜치명",
  "cm": "커밋해시",
  "files": ["변경파일"],
  "tst": [{"cmd": "명령어", "ok": true}],
  "blk": [{"what": "블로커"}],
  "next": "다음 추천 작업"
}
```

Gemini/AGY 결과 (`gemini_agy_result.json`):
```json
{
  "agt": "gemini",
  "ts": "ISO8601 KST",
  "sum": "한문단 요약",
  "review": ["사람이 확인할 항목"],
  "blk": ["중단 조건"],
  "ok": "승인 문장"
}
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
5. 다음 작업이 있으면 기존 `_next.json`을 덮어쓴다.

## Codex 토큰 절약형 큐

Codex는 매번 전체 코드를 길게 재분석하지 않고, 하위 LLM에게 아래 순서로 작은 산출물을 만들게 한다.

### 1단계: DeepSeek 조사 큐

DeepSeek에게 맡기는 작업 (`deepseek_next.json` → `deepseek_result.json`):

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

Gemini/AGY에게 맡기는 작업 (`gemini_agy_next.json` → `gemini_agy_result.json`):

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
1. deepseek_result.json
2. gemini_agy_result.json
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

결과가 부족하면 새 파일을 만들지 말고 같은 `_next.json`을 덮어쓴다.

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


---

## [통합 섹션] LLM_PAIN_POINTS

# Scraper Codebase LLM Perspective — 킹받는 포인트 분석

> **분석일**: 2026-06-11
> **대상**: `ssh-reports-scraper` 전체 코드베이스 (29개 증권사 모듈 + 코어 인프라)
> **관점**: LLM/신규 개발자가 코드를 이해하고 수정할 때 혼란을 주는 구조적 문제점

## 2026-06-22 재조사: 현재도 헷갈리는 포인트

최근 수정으로 `key → report_unique_key`, GA import 발송 상태, 일부 core/list 호환, 상상인/하나/NH 계열 버그는 많이 정리됐다. 그래도 오늘 기준으로 LLM이 계속 실수하기 쉬운 지점은 아래다.

| # | 현재 함정 | 왜 헷갈리는가 | 작업 원칙 |
|:---:|---|---|---|
| 1 | `*_URLS_JSON`이 이름과 달리 URL list일 수도, full config dict일 수도 있음 | `run/standalone/*.py`는 env secret을 직접 읽고, `modules/*.py`는 `ConfigManager.get_urls()`를 읽는다. 회사별 core는 selector/payload가 필요한 곳과 URL만 필요한 곳이 섞여 있다. | standalone은 `run/standalone/_runner.py`를 통해 실행하고, full config가 필요한 core는 명시적으로 required key를 검증한다. `KeyError` 그대로 노출 금지. |
| 2 | `scripts/standalone_all_scraper.py` 문서/구조가 개별 `scrape-*.yml` 현실과 다름 | 과거 all-scraper artifact 방식 설명이 남아 있지만 현재는 회사별 workflow가 대부분 SCP로 서버에 직접 전송한다. | 장애 분석은 `.github/workflows/scrape-*.yml` + `run/standalone/{firm}.py`를 우선 본다. `standalone_all_scraper.py`는 보조/레거시로 취급한다. |
| 3 | GA 이관 회사가 서버 full-scrape에도 다시 들어간다 | `scraper.py`는 KST 1/7/13/21시에 `_GA_FIRMS_*`를 다시 실행한다. 그래서 “GA 성공 후 서버 발송”과 “서버 fallback 발송” 시간이 섞인다. | 중복/발송 원인 분석 시 GitHub Actions만 보지 말고 `scraper.py` full-scrape 시간대와 서버 scheduler 로그를 같이 본다. |
| 4 | 발송 상태 컬럼 alias 흔적이 남아 있음 | 운영 물리 컬럼은 `telegram_sent`이며, 과거 `is_sent`, `main_ch_send_yn` 이름을 기억한 코드/문서가 혼입되기 쉽다. | 새 코드는 `telegram_sent`/`report_unique_key`를 canonical로 사용한다. 운영 DB에는 `main_ch_send_yn` 물리 컬럼이 없으므로 새 SQL에 쓰지 않는다. |
| 5 | workflow 실패 로그가 실제 예외를 숨김 | 대부분 `uv run ... > result.json 2>log.txt` 뒤 `bash -e`라서 Python이 실패하면 `cat log.txt`가 실행되지 않는다. | workflow run step은 실패 시에도 stderr 파일을 출력하도록 `set +e` 패턴 또는 공통 composite action으로 바꾼다. |
| 6 | `validate_scrape_result.py --require-non-empty`가 “장애”와 “장중 0건”을 구분하지 않음 | 사이트가 정상이어도 특정 시간/게시판은 0건일 수 있는데 workflow는 실패 처리한다. 반대로 실제 파싱 깨짐도 0건으로만 보일 수 있다. | 회사별 기대 수집 정책을 분리한다. “0건 허용 회사/시간대”와 “반드시 non-empty”를 config로 나눈다. |
| 7 | 회사별 workflow env 이름이 통일되지 않음 | 대다수는 `FIRM_URLS_JSON`인데 LS/DS/Daeshin/KoreaInvestment는 `urls`를 쓴다. | 새 workflow는 `FIRM_URLS_JSON`만 사용한다. 레거시는 바꾸기 전까지 standalone entrypoint가 어떤 env를 읽는지 먼저 확인한다. |
| 8 | docs 상태표가 실제 Actions와 빠르게 어긋남 | 수동 문서의 정상/장애 표는 최신 run과 다를 수 있다. | 현재 상태 판단은 `gh run list`와 운영 로그를 기준으로 하고, docs는 배경 설명으로만 사용한다. |
| 9 | 같은 증권사가 `modules/*`, `scrapers/*_core.py`, `run/standalone/*`, workflow 네 군데에 걸쳐 있음 | “어디를 고쳐야 하는지”가 회사마다 다르다. 일부 서버 모듈은 core wrapper, 일부는 독자 구현이다. | 우선순위: core 로직 수정 → standalone wrapper 확인 → server module wrapper 확인 → workflow env/result 파일명 확인. |
| 10 | LS는 일반 GA standalone 패턴과 다름 | DB 기반 URL 복구, WARP 상태, `FirmInfo`, `get_db()`까지 얽혀 있어 순수 HTTP scraper가 아니다. | LS는 별도 시스템으로 취급한다. 일반 `scrapers/*_core.py` 규칙을 무리하게 적용하지 않는다. |

### 2026-06-22 즉시 반영한 정리

- `run/standalone/_runner.py` 추가: env JSON 파싱, missing secret, JSON 오류, config key 누락을 공통 처리.
- env 기반 standalone 전부 공통 runner 경유로 정리: `KeyError` traceback 대신 회사명 포함 `FATAL` 로그.
- `scrapers/config_guard.py` 추가: core에서 config shape 오류를 명시적으로 던질 수 있게 함.
- `run/standalone/sangsanginib.py`의 존재하지 않는 `scrapers.sangsanginib_core` import를 `scrapers.sangsangin_core`로 수정.
- `scrapers/hanwha_core.py`는 다운로드 URL이 비는 항목을 append하지 않도록 수정해 `report_unique_key=""` 검증 실패를 줄임.

### 다음에 손대면 좋은 순서

1. workflow run step 공통화: 실패해도 `*_log.txt`를 반드시 출력.
2. `*_URLS_JSON` 스키마를 회사별 manifest로 선언: `url_list`/`full_config` 구분을 코드에 박아둔다.
3. `scraper_registry.py`를 실제 SSoT로 만들고 `scraper.py`, `standalone_all_scraper.py`, docs 표를 거기서 생성.
4. 0건 허용 정책을 `validate_scrape_result.py`에 회사/시간대별로 반영.
5. `telegram_sent` 전환 이후 남은 `is_sent`/`main_ch_send_yn` 문서·legacy 예시는 운영 SQL로 복사되지 않게 격리.

### 해결 완료 (2026-06-11)

| # | 문제 | 조치 |
|:---:|------|------|
| 1 | GA ↔ 서버 코드 중복 (Pain 4) | **10개사 통일**: `scrapers/*_core.py` → GA+서버 wrapper |
| 2 | `"key:"` 오타 (ShinHanInvest_1) | `"key:"` → `"key"` |
| 3 | 상상인 하드코딩 쿠키 | env var로 분리 |
| 4 | scheduler.py dead code | 61줄 제거 |
| 5 | KB board_id=0 | 13종 게시판 분류 + 8,142건 백필 |
| 6 | DB 타입 불일치 (Pain 17.2) | `saved_at`(timestamptz), `report_date`(date), `telegram_sent`(bool) |
| 7 | DB 33컬럼 짬뽕 테이블 (Pain 17.1) | 4종 정규화 테이블 분리 + `v_sec_reports_full` 뷰 |
| 8 | FnGuide 매칭 성능 | `report_date`+`writer`+`board` 인덱스, `v_fnguide_authors` 뷰 |
| 9 | `key` 컬럼명 모호 | `report_unique_key` 추가, dedup 우선 사용 |
| 10 | 애널리스트 마스터 공백 | 2,355명 시딩 (`tbm_analyst_master`) |
| 11 | RAG 임베딩 파이프 없음 | `run/rag_embed_batch.py` 배치 파이프라인 |
| 12 | `call_async_scraper` 취약점 (Pain 15) | `iscoroutinefunction()` → 호출 전 판별 + to_thread fallback |
| 13 | FirmInfo 메타클래스 LLM 진입장벽 (Pain 2) | `models/firm_utils.py` 함수형 wrapper 추가 |
| 14 | `COMMENT_PDF_URL` 대문자 (Pain 17.6) | `comment_pdf_url` 소문자로 마이그레이션 (운영 DB 반영) |
| 15 | WebScraper `_set_headers()` if/elif | `headers=` 파라미터 추가, 기존 분기는 deprecation |
| 16 | `tbl_report_ai_tags` → AI 아님 | `tbl_report_enricher_tags`로 rename → 이후 DROP (2026-07-09, enricher가 tbl_sec_reports 직접 사용) |
| 17 | `tbl_report_downloads` 중복 | DROP (pdf-archiver가 `tbl_sec_reports` 직접 관리) |

### 아직 안 함

| # | 항목 | 난이도 | 비고 |
|:---:|------|:---:|------|
| B | LS_0 전역 상태 제거 | 🔴 | `USE_WARP_ONLY`, `skip_boards` — LS 마이그레이션 시 병행 |
| D | enricher 정규화 테이블 완전 전환 | 🟡 | 지금은 `tbl_sec_reports` + 신규 테이블 이중기록. 저사용 컬럼 정리 전 영향 범위 확인 |
| E | 옛 컬럼 드랍 (save_time, reg_dt, main_ch_send_yn, key) | ✅ | 운영 DB 물리 컬럼에서 이미 제거됨 (2026-07-09 확인) |
| F | ORM `v_sec_reports_full` 매핑 | 🟡 | Backend submodule 작업. 컬럼 드랍 전 필수 |
| G | URL 컬럼 통합 (`article_url`, `download_url` 물리 컬럼 제거) | ✅ | 운영 물리 컬럼은 `telegram_url`, `pdf_url`, `gdrive_pdf_url`; 호환 뷰 alias는 유지 |
| H | `json_util` / `report_json_store` 통합 | 🔴 | telegram/local-json 호환 계약 보존을 위해 보류(보존) |
| I | `validate_scrape_result` 규칙 개편 | 🟡 | 결과 유효성 검증 규칙 호환 계약 보존을 위해 보류(보존) |
| J | `BNKfn_23` / `LS_0` standalone 및 모듈 | 🔴 | bnk/ls standalone 스크랩 호환 계약 보존을 위해 보류(보존) |

---

## 1. 모듈 네이밍 불일치 (⭐⭐⭐)

29개 증권사 모듈명이 3가지 컨벤션이 섞여 있음:

| 패턴 | 예시 | 개수 |
|------|------|:---:|
| `영문약어_숫자.py` | `LS_0.py`, `DS_11.py`, `SKS_26.py` | 대부분 |
| `영문풀네임_숫자.py` | `ShinHanInvest_1.py`, `Koreainvestment_13.py` | ~6개 |
| `소문자+숫자.py` | `eugenefn_12.py`, `iMfnsec_18.py` | 2개 |

**LLM 혼란 포인트**:
- `HANA_3.py` — 하나증권이지만 Hana → HANA (대문자)
- `eugenefn_12.py` — 유진투자증권인데 `eugenefn` (eugene + fn?)
- `Hygood_22.py` — 한양증권인데 Hygood? (옛날 한양증권 영문명)
- 숫자가 `firm_id`인데 일부는 언더스코어(`_`)로 구분, 일부는 그냥 붙여씀

**권장**: `firm_01_shinhan.py`, `firm_04_kb.py` 같은 통일된 네이밍. 또는 파일명에 `firm_id`를 포함하지 말고 `firm_nm`만 사용.

---

## 2. FirmInfo 메타클래스 — 과도한 추상화 (⭐⭐⭐)

`models/FirmInfo.py`:
```python
class MetaFirmInfo(type):          # ← 메타클래스? 왜?
    @property
    def firm_names(cls): ...

class FirmInfo(metaclass=MetaFirmInfo):  # ← 싱글톤 데이터 + 인스턴스
```

**LLM 혼란 포인트**:
- **메타클래스**는 Python에서도 rare 패턴. LLM은 메타클래스 코드를 이해하는 데 토큰을 많이 소모함.
- `FirmInfo(firm_id, board_id)` — 인스턴스를 매번 생성하는데 실제로는 싱글톤 `_firm_data`를 참조. 생성자 비용 낭비.
- `FirmInfo.firm_names` → 클래스 프로퍼티지만 메타클래스로 구현되어 있어 추적 어려움.
- `load_data_from_db()` → 클래스 메서드지만 첫 호출 시점을 예측할 수 없음 (lazy init).

**권장**:
```python
# 단순한 데이터 클래스 + 모듈 레벨 함수로 충분
_firm_data: dict[int, str] = {}

def get_firm_name(firm_id: int) -> str: ...
def get_board_name(firm_id: int, board_id: int) -> str: ...
```

---

## 3. ConfigManager — 4단계 URL 해상도 (⭐⭐⭐)

`models/ConfigManager.py` — URL을 찾는 경로가 4가지:
1. `urls` 환경변수 (전체 JSON, generate_env.py가 주입)
2. `URLS_{key}` 환경변수 (개별)
3. `~/secrets/ssh-reports-scraper/secrets.json` → `urls.{key}`
4. `default` 파라미터 또는 `[]`

**LLM 혼란 포인트**:
- "이 증권사 URL이 어디서 오는가?" → 4개 소스를 다 체크해야 함
- `MissingConfigError`는 source가 로드되었는데 key만 없을 때만 발생 — 조건이 미묘함
- `get_base_url()`은 `urls[0]`의 scheme+netloc만 추출 — 첫 번째 URL이 무엇인지 알아야 함
- GA standalone에서는 `{FIRM}_URLS_JSON` 환경변수만 사용 → ConfigManager와 다른 체계

**권장**: 단일 URL 소스로 통합. secrets.json 하나만 사용하거나 환경변수 하나만 사용.

---

## 4. GA Standalone 코드 중복 (⭐⭐⭐)

동일한 스크래핑 로직이 **두 곳**에 존재:

| 위치 | 용도 |
|------|------|
| `modules/KBsec_4.py` | 서버 scraper.py fallback |
| `run/standalone/kb.py` | GA standalone primary |

**LLM 혼란 포인트**:
- 버그 수정할 때 두 곳 다 고쳐야 함 = DRY 위반
- 두 구현이 완전히 동일한지 보장할 수 없음
- `modules/KBsec_4.py`는 `aiohttp` + `AsyncWebScraper` 사용, `kb.py`는 `requests` 사용 — HTTP 라이브러리도 다름

**권장**: 공통 코어 로직을 `scrapers/kb_core.py`로 추출하고, GA/서버는 wrapper만 제공.

---

## 5. scraper.py 함수 리스트 수동 관리 (⭐⭐)

```python
sync_funcs = [
    Miraeasset_checkNewArticle, Sks_checkNewArticle, Shinyoung_checkNewArticle, ...
]
async_functions = [
    ShinHanInvest_checkNewArticle, HANA_checkNewArticle, ...
]
_GA_FIRMS_SYNC = {Samsung_checkNewArticle, TOSSinvest_checkNewArticle, ...}
_GA_FIRMS_ASYNC = {NHQV_checkNewArticle, KB_checkNewArticle, ...}
```

**LLM 혼란 포인트**:
- 새 증권사 추가 시 최대 3곳(sync_funcs, async_functions, GA sets)을 수정해야 함
- 함수가 sync인지 async인지 `scraper_registry.py`에도 정의되어 있음 → 이중 관리
- `is_full` 조건으로 GA 함수들이 extend되는데, 이 로직이 직관적이지 않음

**권장**: `scraper_registry.py`를 단일 진실 공급원(SSoT)으로 만들고 scraper.py는 registry만 참조.

---

## 6. WebScraper 추상화의 비일관적 사용 (⭐⭐)

`models/WebScraper.py`:
- `SyncWebScraper` — requests 기반 동기 래퍼
- `AsyncWebScraper` — aiohttp 기반 비동기 래퍼

**실제 사용 현황**:
- `ShinHanInvest_1.py`: `SyncWebScraper` 사용 + `aiohttp` 직접 사용 (혼합)
- `KBsec_4.py`: `AsyncWebScraper.PostJson()` 사용
- `Leading_16.py`: `AsyncWebScraper.Get()` 사용
- `HANA_3.py`: `aiohttp` 직접 사용 (WebScraper 미사용)
- `NHQV_2.py`: `aiohttp` 직접 사용 (WebScraper 미사용)

**LLM 혼란 포인트**:
- 어떤 모듈은 WebScraper를 쓰고, 어떤 모듈은 직접 HTTP 클라이언트를 씀 → 패턴 파악 불가
- WebScraper가 타임아웃/재시도를 추상화해주지 않음 → 존재 가치 의문
- `SyncWebScraper`는 `FirmInfo` 인스턴스를 받는데, 실제로는 URL과 firm_info 로깅만 씀

**권장**: WebScraper를 없애거나, 모든 모듈이 일관되게 사용하도록 강제. HTTP 호출 패턴 통일.

---

## 7. 모듈별 리턴 딕셔너리 필드 비일치 (⭐⭐)

각 모듈이 반환하는 dict 필드가 다름. 아래 표는 2026-06-11 당시 스냅샷이며, 현재 운영 DB 물리 컬럼은 `report_date`, `report_unique_key`, `save_at`, `telegram_sent`가 canonical이다.

| 필드 | KBsec | HANA | NHQV | Shinyoung | Leading | DAOL |
|------|:---:|:---:|:---:|:---:|:---:|:---:|
| `firm_id` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `board_id` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `firm_nm` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `report_date` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `article_title` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `writer` | ✅ | ✅ | ✅ | - | - | ✅ |
| `download_url` | ✅ | ✅ | - | ✅ | ✅ | ✅ |
| `telegram_url` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `pdf_url` | ✅ | ✅ | ✅ | - | ✅ | - |
| `report_unique_key` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `save_at` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `mkt_tp` | ✅ | ✅ | - | ✅ | - | - |
| `article_url` | - | - | - | - | - | - |

**LLM 혼란 포인트**:
- 필수 필드가 무엇인지 명세가 없음
- `writer`가 없는 모듈은 DB에 null로 들어감 → 검색/필터링 시 누락
- `mkt_tp`가 없으면 기본값이 무엇인지 추론해야 함 (KR인가? null인가?)

**권장**: dataclass 또는 TypedDict로 출력 스키마 강제. 모듈별로 누락된 필드를 자동 보완하는 레이어 추가.

---

## 8. DB_BACKEND 단일화 (⭐)

```python
if os.getenv("DB_BACKEND", "postgres").lower() == "postgres":
    cls._load_from_postgres()
else:
    cls._load_static_fallback()
```

**LLM 혼란 포인트**:
- 운영 DB 경로는 `SecReportsManager` 단일 경로다.
- GA standalone/test에서 DB 접근이 필요 없으면 `DB_BACKEND=static`으로 메타데이터 static fallback만 사용한다.
- `db_factory.py`는 더 이상 SQLite manager를 반환하지 않는다.
- `models/PostgreSQLManager.py` alias도 제거됐다. 새 코드/문서는 `models.SecReportsManager` 또는 `models.db_factory.get_db()`를 사용한다.
- 네이버/조선비즈 뉴스 workflow/core는 제거됐고, 뉴스 발송은 `naver-stock-news`가 담당한다.

**권장**: 새 코드는 PostgreSQL/`SecReportsManager` 또는 명시적 static fallback만 사용한다.

---

## 9. scheduler.py와 scraper.py의 책임 분리 모호 (⭐)

`scheduler.py`가 하는 일:
- APScheduler로 cron job 등록
- GA import 폴링 (5분 간격, `incoming/ga-scrapes/` 디렉토리)
- `scraper.py main()` 호출 → enrich + send

`scraper.py`가 하는 일:
- 증권사 스크래핑 (REGULAR / FULL-SCRAPE 모드)
- enrich_data (DBfi, LS 후처리)
- daily_send_report (텔레그램 전송)

**LLM 혼란 포인트**:
- "왜 scheduler가 직접 scraper를 import해서 호출하는가?" → subprocess가 아닌 in-process 호출
- GA import가 scheduler에 있지만, 스크래핑은 scraper에 있음 → "스크래핑" 책임이 두 파일로 분산
- `enrich_data()`는 scraper에 있지만, `import_*_artifact.py`에도 후처리 로직이 있음 → 중복

---

## 10. board_id = 0 관행 (⭐⭐)

대부분의 증권사가 `board_id = 0`을 하드코딩:
- KB증권: 13개 카테고리가 있지만 최근까지 전부 0으로 저장
- Leading, Daeshin, DAOL, MERITZ 등: 실제로 여러 게시판을 순회하면서도 board_order는 URL 인덱스만 사용

**LLM 혼란 포인트**:
- `board_id`가 무엇을 의미하는지 모듈마다 다름
  - HANA: URL_TUPLE의 enumerate index
  - KB: pCategoryid 매핑 (이제 수정됨)
  - Shinyoung: 무조건 0
- `tbm_sec_firm_board_info`에 게시판 정보가 있는 증권사는 소수

---

## 요약: LLM이 가장 헷갈리는 Top 5

| 순위 | 문제 | 영향 | 해결 난이도 |
|:---:|---|:---|:---:|
| 1 | 모듈 네이밍 불일치 | 파일 찾기 어려움, import문 혼란 | 낮음 |
| 2 | GA ↔ 서버 코드 중복 | 수정 시 누락, 동기화 이슈 | 중간 |
| 3 | FirmInfo 메타클래스 | 과도한 추상화, 토큰 낭비 | 중간 |
| 4 | 리턴 필드 비일치 | 데이터 무결성, null 누락 | 중간 |
| 5 | 수동 함수 리스트 관리 | 신규 증권사 추가 시 누락 | 낮음 |

---

## 11. ~~실제 버그: ShinHanInvest_1.py의 `"key:"` 오타~~ ✅ 수정완료 (2026-06-11)

`modules/ShinHanInvest_1.py` line 114 (레거시 `_back` 함수):
```python
"key:": LIST_ARTICLE_URL,  # ← 콜론(:)이 key에 포함됨 → "key"가 아닌 "key:" 필드 생성
```
`scraper.py`의 dedup 로직은 `d.get("key")`로 접근 → 이 버그가 있는 레코드는 **전부 중복 체크에서 누락**되어 DB upsert가 동작하지 않음. 다행히 현재는 `_back` 함수가 호출되지 않지만, dead code로 남아있어 실수로 활성화될 위험.

---

## 12. LS_0.py — 전역 가변 상태와 O(N²) URL 탐색 (⭐⭐⭐)

`modules/LS_0.py` (720줄, 가장 큰 모듈):
- `USE_WARP_ONLY` (bool): 모듈 레벨에서 WARP 프록시 사용 여부를 제어. `asyncio.gather`로 여러 코루틴이 동시에 읽고 씀 → data race.
- `skip_boards` (set): 스크래핑 중 동적으로 수정되는 전역 집합.
- `reconstruct_msg_url_from_db()`: 최대 21 days × 101 seq = 2,121회 HEAD 요청으로 유효 URL 탐색 → O(N²). API 문서를 알면 O(1)로 가능.

**LLM 혼란 포인트**: 전역 상태가 코루틴 간에 공유되어 비결정적 버그 발생 가능.

---

## 13. ~~Sangsanginib_6.py — 하드코딩된 세션 쿠키~~ ✅ 수정완료 (2026-06-11)

```python
cookies = {
    "SSISTOCK_JSESSIONID": "F63EB7BB0166E9ECA5988FF541287E07",  # ← 만료됨
}
```
세션 토큰이 만료되면 `fetch_data()`가 `{}`를 반환하고, 빈 응답은 조용히 스킵됨 → **0건 수집이지만 에러 없음**.

---

## 14. DBfi_19.py — PDF URL 추출 로직이 두 벌 (⭐⭐)

- `extract_dbfi_pdf_url()`: async 버전, 패턴 1개
- `DBfi_detail()`: sync 버전, 패턴 8개 + fallback 3단계

같은 PDF URL 추출을 두 방식이 다르게 구현 → 버그 수정 시 두 곳 다 고쳐야 함.

---

## 15. call_async_scraper — 취약한 sync/async 감지 (⭐)

`scraper.py` line 289:
```python
res = func()
if asyncio.iscoroutine(res):
    res = await asyncio.wait_for(res, ...)
```
함수를 **먼저 호출**하고 반환값이 코루틴인지 확인 → sync 함수를 이벤트 루프 스레드에서 실행해버림.

---

## 16. scheduler.py — Dead code와 subprocess 낭비 (⭐)

- `run_enricher_batch`, `run_enricher_backfill`: 주석 처리된 채 방치
- `scraper.py`를 매번 subprocess로 새로 spawn → 29개 모듈을 매번 import (캐시 무용)
- `in-process`로 `import scraper; await scraper.main()` 호출하면 훨씬 효율적

---

## 심각도 기반 요약

**Critical (데이터 손실/무결성)**:
1. 공통 리턴 스키마 부재 → 필드 누락으로 DB null (Pain 7, 1)
2. `"key:"` 오타 → 중복 제거 실패 (Pain 13 → 11)
3. LS_0 전역 상태 → data race (Pain 11 → 12)
4. 상상인 세션 만료 → 무소식 실패 (Pain 14 → 13)

**High (유지보수 장애)**:
5. GA ↔ 서버 코드 중복 (Pain 4)
6. ConfigManager 3중 URL 해상도 (Pain 3)
7. 수동 함수 리스트 3곳 관리 (Pain 5)
8. WebScraper의 firm-specific 하드코딩 (Pain 7)

**Medium (인지 부하)**:
9. FirmInfo 메타클래스 (Pain 2)
10. DB_BACKEND 듀얼 모드 (Pain 8)
11. DBfi 이중 PDF 추출 경로 (Pain 14)
12. 모듈 네이밍 불일치 (Pain 1)

---

## 단기 개선 제안 (다음 스프린트)

1. **공통 리턴 스키마 정의**: `models/report_schema.py` → `ReportArticle` dataclass + 런타임 검증
2. **모듈 레지스트리 자동화**: `scraper_registry.py` → `@register_firm` 데코레이터로 SSoT 통합
3. **GA/서버 코드 통합**: `scrapers/kb_core.py` 패턴으로 중복 제거, 11개 standalone을 core 모듈로 대체
4. **FirmInfo 단순화**: 메타클래스 제거, 일반 함수로 교체
5. **Dead code 제거**: ShinHanInvest `_back` 함수, scheduler 주석 블록
6. **상상인 세션 환경변수화**: `SSISTOCK_JSESSIONID`를 env var로
7. **LS_0 리팩토링**: 전역 상태 제거, URL 재구성 O(1) 최적화

---

## 17. DB 스키마 비효율 (2026-07-09 운영 DB 확인)

### 17.1 거의 100% 미사용 컬럼 (제거 검토)

운영 DB 310,244건 기준:

| 컬럼 | 채워진 row | fill rate | 비고 |
|------|:---:|:---:|------|
| `gdrive_pdf_url` | 1 | 0.00% | 거의 미사용 |
| `gemini_summary` / `summary_model` / `summary_time` | 321 | 0.10% | Gemini 요약 실험 → 폐기/저사용 |
| `article_text` | 1,877 | 0.61% | 본문 추출 저사용 |
| `archive_path` | 2,422 | 0.78% | PDF 아카이빙 경로 저사용 |
| `target_price` | 5,941 | 1.91% | 목표가 추출 저사용 |
| `sector` | 7,342 | 2.37% | 섹터 태깅 저사용 |
| `fnguide_summary_id` | 8,069 | 2.60% | FnGuide 매칭 일부 사용 |
| `rating` / `revision_type` / `report_type` | 약 9,567~9,570 | 3.08% | 프리미엄/분류 기능 저사용 |

**LLM 혼란**: 운영 물리 컬럼 33개 중 일부 enrichment/AI 컬럼은 채움률이 낮다. `SELECT *`나 ORM 매핑 시 불필요한 데이터까지 로드하기 쉽다.

### 17.2 이미 드랍된 legacy 물리 컬럼

2026-07-10 KST 기준 `public.tbl_sec_reports`에는 `article_url`, `download_url`, `key`, `reg_dt`, `save_time`, `main_ch_send_yn` 물리 컬럼이 없다. 새 SQL/DDL/문서에서 이 컬럼들을 “나중에 드랍” 대상으로 쓰지 않는다.

현재 canonical 컬럼은 `report_unique_key`, `report_date`, `save_at`, `telegram_sent`다.

### 17.3 의미 불명확한 컬럼/enum

| 컬럼 | 값 분포 | 문제 |
|------|------|------|
| `download_status_yn` | blank 200,300 / Y 109,944 | boolean 성격인데 문자열 상태값 |
| `sync_status` | 2:233,909 / 0:57,516 / 3:17,155 / 9:1,664 | 0/2/3/9가 각각 무슨 의미인지 코드에만 존재, DB 주석 없음 |
| `pdf_sync_status` | 2:261,846 / 0:41,238 / 3:5,497 / 9:1,663 | sync_status와 같은 enum인데 별도 컬럼 — 왜 분리했는지 불명 |

### 17.4 URL 컬럼 정리

2026-07-10 KST 운영 DDL 이후 `tbl_sec_reports` 물리 URL 컬럼은 다음만 남았다:

- `telegram_url` — 텔레그램 전송용 URL
- `pdf_url` — PDF 직접 URL / 다운로드 기준 URL
- `gdrive_pdf_url` — Google Drive PDF URL

드랍 완료:

- `article_url`
- `download_url`

호환 뷰 `v_sec_reports_canonical`, `v_sec_reports_full`, `v_reports_api`, `v_reports`, `v_scraper_workbench`는 재생성 후 정상 조회되며 각 count는 310,395다.

- `article_url`, `source_page_url`: NULL alias
- `download_url`, `source_pdf_url_fallback`: `pdf_url` 매핑

DBfi는 `telegram_url`과 `pdf_url`이 다를 수 있는 알려진 특수 케이스다.

### 17.5 mkt_tp 값 불일치

| 값 | 건수 | 비고 |
|------|:---:|---|
| `KR` | 275,135 | 국내 |
| `GLOBAL` | 7,171 | 해외 |
| `US` | 1,605 | 일부 모듈만 `US` 사용 |
| `JP` | 17 | 일부 모듈만 `JP` 사용 |

`US`/`JP`는 `GLOBAL`로 통일 가능. 모듈마다 다른 값 쓰는 건 일관성 문제.

### 17.6 TBM 테이블 컬럼명 불일치

`tbm_sec_firm_info`:
- `COMMENT_PDF_URL` — 다른 컬럼은 모두 소문자인데 이것만 대문자 (PostgreSQL은 따옴표 없는 식별자를 소문자로 접음 → 실제 컬럼명은 `comment_pdf_url`인데 DDL에 대문자로 남아 혼란)

`tbm_sec_firm_board_info`:
- `board_cd` → 거의 모든 row가 null. 사용되지 않는 컬럼.

---

## DB 개선 제안 (쉬운 것부터)

| 난이도 | 작업 |
|:---:|---|
| ~~🟢~~ | ~~`COMMENT_PDF_URL` → 소문자~~ → 3개사만 사용, 개발자 노트 용도 확인 |
| 🟢 | `mkt_tp` `US`/`JP` → 보류 (국가 구분 정보 손실 우려) |
| ~~🟡~~ | ~~`save_time` → `timestamptz`, `reg_dt` → `date`~~ ✅ 완료 (2026-06-11) |
| ~~🟡~~ | ~~14개 미사용 컬럼 분리~~ → 롤백 (2026-07-09): 3종 좀비 테이블 DROP, enricher가 `tbl_sec_reports` 직접 사용, `docs/schema.sql`이 SSoT |
| 🟡 | `sync_status` enum 의미를 DB comment로 문서화 |
| ✅ | URL 컬럼 통합: `article_url` / `download_url` 물리 컬럼 제거, 호환 뷰 alias 유지 |
| ✅ | FnGuide 매칭 성능: `report_date` + `writer` + `board_order` 인덱스, `v_fnguide_authors` 뷰 |


---
