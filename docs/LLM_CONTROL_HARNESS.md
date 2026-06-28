# LLM 유지보수 통제 전략

> 작성일: 2026-06-24  
> 목적: 저비용 LLM에게 반복 작업을 맡기되, 운영 장애로 이어지지 않도록 하네스와 작업 규칙으로 통제한다.

## 결론

이 코드베이스의 반복 장애는 LLM 성능 부족만의 문제가 아니다. 더 큰 원인은 수정 대상이 `modules/*`, `scrapers/*_core.py`, `run/standalone/*`, `.github/workflows/scrape-*.yml`, secrets/env, validator, deploy 경로에 흩어져 있는데, 이를 한 번에 검증하는 강제 하네스가 아직 약하다는 점이다.

MCP는 LLM에게 GitHub Actions 로그, 서버 로그, DB 상태를 보여주는 관측 도구다. 필요하지만 1순위는 아니다. 지금 먼저 필요한 것은 변경 전후에 같은 명령으로 통과 여부를 판정하는 하네스다.

짧게 말하면:

```text
하네스 = LLM이 멍청하게 수정해도 merge/deploy 전에 걸러내는 문
MCP    = LLM이 운영 상태를 더 잘 보게 하는 눈
```

문 없이 눈만 좋아지면 더 많은 맥락을 보고도 엉뚱한 수정이 빨라질 수 있다. 따라서 순서는 하네스 → manifest/SSoT → MCP가 맞다.

## 현재 관리체계 평가

바이브코더 기준으로는 상위 5~10% 수준이다.

근거:

- `docs/LLM_PAIN_POINTS.md`에 LLM이 실수하는 구조적 함정이 정리돼 있다.
- `docs/DEPLOY_CHECKLIST.md`에 실제 장애 기록과 재발방지책이 있다.
- `docs/ARCHITECTURE.md`와 `docs/GA_STATUS.md`에 GA/server 구조와 상태가 문서화돼 있다.
- `scripts/validate_scrape_result.py`, `scripts/verify_standalones.sh`, `scripts/verify_dockerfile.sh` 같은 검증 스크립트가 있다.
- `tests/test_config_manager.py`, `tests/test_scraper_imports.py`, `tests/test_core_contract.py`처럼 LLM 실수를 일부 막는 테스트가 있다.

하지만 운영 신뢰성 기준으로는 아직 60~70점이다.

부족한 점:

- 문서에 적힌 규칙이 CI에서 강제되지 않는 부분이 많다.
- firm별 entrypoint/env/workflow/config shape/result policy가 단일 manifest로 선언돼 있지 않다.
- deploy workflow가 전체 위험면을 보지 않고 import/config 일부만 테스트한다.
- 회사별 workflow YAML이 많아 LLM이 수동 수정하다가 쉽게 어긋난다.
- `--require-non-empty`가 정상 0건과 파싱 장애를 구분하지 못한다.

## 근본 원인

### 1. 하나의 증권사가 너무 많은 파일에 걸쳐 있다

예를 들어 특정 firm을 고칠 때 실제로 확인해야 할 곳은 보통 아래 전체다.

- `scrapers/{firm}_core.py`
- `run/standalone/{firm}.py`
- `modules/{FirmName}_{order}.py`
- `.github/workflows/scrape-{firm}.yml`
- GitHub Secret env 이름
- `scripts/validate_scrape_result.py`
- DB insert/upsert 경로
- server scheduler fallback 경로

LLM은 이 중 일부만 보고 수정하는 경향이 강하다. 그래서 테스트는 통과했는데 GA runtime이나 서버 fallback에서 터진다.

### 2. SSoT가 없다

현재 필요한 SSoT는 firm manifest다. 이 manifest는 최소한 아래 정보를 담아야 한다.

```yaml
firms:
  kb:
    display_name: KB증권
    sec_firm_order: 4
    mode: ga_dual
    core_module: scrapers.kb_core
    standalone: run/standalone/kb.py
    server_module: modules.KBsec_4
    workflow: .github/workflows/scrape-kb.yml
    env_var: KB_URLS_JSON
    config_shape: full_config
    result_file: kb_result.json
    empty_policy: require_non_empty
```

이 정보가 코드와 workflow와 문서에 흩어져 있으면, LLM은 반드시 한두 군데를 놓친다.

### 3. 테스트가 운영 경로를 충분히 재현하지 못한다

지금 테스트는 import/config/contract 일부를 잘 잡는다. 그러나 실제 장애는 보통 아래 경계에서 난다.

- standalone이 읽는 env shape와 core가 기대하는 config shape 불일치
- workflow result filename과 validator/SCP filename 불일치
- core는 고쳤지만 server wrapper가 구형 인자를 넘김
- validator는 통과했지만 DB insert에서 빈 문자열/integer/date 문제 발생
- GitHub Actions 실패 시 log file이 출력되지 않아 원인 확인이 늦어짐

따라서 테스트 단위는 `함수 하나`만이 아니라 `firm 하나의 실행 경로`여야 한다.

## 목표 구조

### 1. Firm manifest

새로운 증권사 추가 또는 기존 증권사 수정 시 사람이 기억해야 하는 정보를 manifest로 모은다.

manifest가 담당할 것:

- firm key
- sec_firm_order
- 서버 모듈
- core 모듈
- standalone 파일
- workflow 파일
- result json 파일명
- secret/env 이름
- config shape
- empty result policy
- GA/server/dual/server-only 상태
- known blocked reason

### 2. Offline harness

목표 명령:

```bash
uv run python scripts/harness.py --firm kb --offline
```

최소 검증 항목:

- manifest에 선언된 파일들이 존재하는지
- standalone이 compile 되는지
- server module import가 되는지
- core module import가 되는지
- standalone env 이름과 workflow env 이름이 일치하는지
- workflow result filename과 validate/SCP filename이 일치하는지
- fake config 또는 fixture config로 core가 type error 없이 실행되는지
- 결과가 list[dict]인지
- `report_unique_key`, `reg_dt`, `firm_nm` 필수 필드가 있는지
- `scripts/validate_scrape_result.py` 정책과 manifest empty policy가 일치하는지
- Dockerfile COPY 누락이 없는지

### 3. CI에서 강제

deploy 전에 최소한 아래는 항상 돌아야 한다.

```bash
uv run pytest tests/test_config_manager.py tests/test_scraper_imports.py tests/test_core_contract.py -q
bash scripts/verify_standalones.sh
bash scripts/verify_dockerfile.sh
```

firm manifest/harness가 들어간 뒤에는 아래도 추가한다.

```bash
uv run python scripts/harness.py --all --offline
```

### 4. Workflow 생성

장기적으로 `.github/workflows/scrape-*.yml`은 사람이 직접 수정하지 않는다.

권장 흐름:

```text
config/firms.yaml
  → scripts/generate_workflows.py
  → .github/workflows/scrape-*.yml
  → CI에서 generated diff 확인
```

LLM에게 workflow를 직접 수정시키면 cron/env/result filename/SCP target이 계속 어긋난다. manifest에서 생성하는 방식으로 바꿔야 한다.

## LLM 작업 원칙

LLM에게는 자유도를 주면 안 된다. 수정 범위, 금지 범위, 테스트 명령, 산출물을 고정해야 한다.

### 허용

- 요청한 firm과 직접 관련된 core/standalone/test/fixture 수정
- 명확한 회귀 테스트 추가
- validator 또는 harness가 요구하는 좁은 범위의 보완

### 금지

- unrelated refactor
- 전체 formatter 적용
- broad sed replacement
- DB schema 변경
- workflow cron 임의 변경
- secrets/env 이름 임의 변경
- `report_unique_key`를 `key` 중심으로 되돌리는 변경
- 실패 원인을 숨기는 `except Exception: pass`
- 테스트를 통과시키기 위해 assertion을 약화하는 변경

### 산출물

LLM 작업 결과에는 반드시 아래가 있어야 한다.

- 원인 3줄 요약
- 수정 파일 목록
- 추가/수정한 테스트
- 실행한 검증 명령과 결과
- 남은 리스크

## DeepSeek용 기본 명령 프롬프트

아래 프롬프트를 그대로 붙여 넣고 `{...}`만 바꿔서 사용한다.

~~~text
너는 ssh-reports-scraper 레포의 저비용 유지보수 담당자다.
목표는 빠른 날코딩이 아니라, 기존 구조를 깨지 않고 좁은 범위의 수정과 회귀 테스트를 추가하는 것이다.

반드시 먼저 읽어라:
- docs/LLM_PAIN_POINTS.md
- docs/DEPLOY_CHECKLIST.md
- docs/GA_STATUS.md
- docs/ARCHITECTURE.md 중 관련 firm 섹션
- 수정 대상 firm의 workflow: .github/workflows/scrape-{firm}.yml
- 수정 대상 standalone: run/standalone/{firm}.py
- 수정 대상 core: scrapers/{firm}_core.py
- 관련 server module: modules/{module_name}.py

작업 목표:
- firm: {firm}
- 증상: {symptom}
- 기대 결과: {expected_result}

수정 허용 범위:
- scrapers/{firm}_core.py
- run/standalone/{firm}.py
- modules/{module_name}.py, 단 core wrapper 인자 호환성 확인 목적일 때만
- tests/test_{firm}_core.py
- tests/fixtures/{firm}/*
- scripts/validate_scrape_result.py, 단 firm 정책 검증에 꼭 필요할 때만

수정 금지:
- 관련 없는 firm 파일 수정 금지
- DB schema 변경 금지
- GitHub Actions cron 변경 금지
- secret/env 이름 변경 금지
- broad sed replacement 금지
- 전체 formatter 적용 금지
- 테스트 assertion 약화 금지
- 실패를 숨기는 broad except 추가 금지
- report_unique_key를 key 중심으로 되돌리는 변경 금지

작업 순서:
1. 현재 실행 경로를 요약해라.
   - workflow가 어떤 env를 주입하는지
   - standalone이 어떤 env를 읽는지
   - core가 list/config dict 중 무엇을 기대하는지
   - server module이 core를 어떻게 호출하는지
2. 실패 원인을 3줄 이내로 특정해라.
3. 가장 좁은 범위로 수정해라.
4. 회귀 테스트를 추가해라.
5. 아래 검증을 실행하고 결과를 보고해라.

검증 명령:
```bash
uv run pytest tests/test_{firm}_core.py -q
uv run pytest tests/test_core_contract.py tests/test_scraper_imports.py -q
bash scripts/verify_standalones.sh
uv run python scripts/validate_scrape_result.py {firm}_result.json --require-non-empty
```

주의:
- 실제 네트워크 호출이 필요한 테스트를 새로 만들지 마라. fixture/mock 기반으로 만들어라.
- 네트워크 문제와 파싱 문제를 구분해서 설명해라.
- 0건 결과가 정상일 수 있는 firm이면 validator 정책을 바꾸기 전에 docs/LLM_PAIN_POINTS.md의 0건 정책 문제를 참고해라.
- LS는 일반 GA standalone 패턴과 다르므로 다른 firm 규칙을 기계적으로 적용하지 마라.

최종 보고 형식:
1. 원인
2. 수정 내용
3. 추가한 테스트
4. 실행한 검증 명령과 결과
5. 남은 리스크
~~~

## DeepSeek용 하네스 구현 프롬프트

하네스 자체를 만들 때는 아래 프롬프트를 사용한다.

~~~text
너는 ssh-reports-scraper 레포의 유지보수 하네스를 구현한다.
목표는 LLM이 firm별 scraper를 수정했을 때 운영 배포 전에 구조적 불일치를 잡는 offline 검증 도구를 추가하는 것이다.

반드시 먼저 읽어라:
- docs/LLM_PAIN_POINTS.md
- docs/DEPLOY_CHECKLIST.md
- tests/test_core_contract.py
- tests/test_scraper_imports.py
- scripts/validate_scrape_result.py
- scripts/verify_standalones.sh
- scripts/verify_dockerfile.sh

구현 목표:
1. config/firms.yaml 또는 Python dict 기반 manifest를 추가한다.
2. scripts/harness.py를 추가한다.
3. 아래 명령이 동작하게 한다.

```bash
uv run python scripts/harness.py --firm kb --offline
uv run python scripts/harness.py --all --offline
```

harness가 검증해야 할 항목:
- manifest에 선언된 core/standalone/module/workflow 파일 존재
- standalone py_compile 성공
- core module import 성공
- server module import 성공
- workflow 파일 안의 result json 파일명과 manifest result_file 일치
- workflow env 이름과 manifest env_var 일치
- standalone 파일이 manifest env_var 또는 공통 runner를 통해 같은 env를 읽는지 확인
- config_shape가 url_list/full_config 중 하나인지 확인
- empty_policy가 require_non_empty/allow_empty/server_only 중 하나인지 확인
- scripts/verify_standalones.sh 실행 결과 성공
- scripts/verify_dockerfile.sh 실행 결과 성공

범위 제한:
- 실제 scraper 로직을 고치지 마라.
- 실제 네트워크 호출을 하지 마라.
- GitHub Actions workflow를 대량 수정하지 마라.
- DB 연결을 하지 마라.
- 기존 테스트를 약화하지 마라.

테스트:
- tests/test_harness.py를 추가한다.
- fixture manifest 1~2개로 정상/실패 케이스를 검증한다.
- 최소한 누락 파일, 잘못된 env 이름, 잘못된 empty_policy를 잡는 테스트를 포함한다.

검증 명령:
```bash
uv run pytest tests/test_harness.py -q
uv run pytest tests/test_core_contract.py tests/test_scraper_imports.py -q
bash scripts/verify_standalones.sh
bash scripts/verify_dockerfile.sh
```

최종 보고 형식:
1. 추가한 파일
2. harness가 잡는 오류 유형
3. 실행한 검증 명령과 결과
4. 아직 잡지 못하는 리스크
~~~

## Codex 사용 방침

Codex는 토큰을 날코딩에 쓰기보다 아래에 쓰는 것이 효율적이다.

- 하네스 설계 리뷰
- manifest 스키마 리뷰
- DeepSeek 결과물 코드 리뷰
- 운영 장애 원인 분석
- CI/CD 경계 검증 강화
- DB/schema 변경 전 위험 검토

DeepSeek 같은 저비용 LLM은 좁은 수정과 반복 작업에 쓰고, Codex는 기준선과 안전장치를 잡는 역할로 쓰는 것이 비용 대비 효율이 좋다.

## 멀티 에이전트 협업 체계

이 저장소는 역할이 세분화된 여러 LLM 에이전트와 메인 엔지니어인 Codex가 협업하여 운영 및 유지보수를 수행합니다.

### 에이전트별 역할 분담
- **Codex**: main engineer/reviewer/integrator/final verifier 역할을 담당하며, 설계, 위험도 판단, 코드 리뷰 및 좁은 범위의 핫픽스를 직접 처리합니다.
- **DeepSeek**: technical investigation, repetitive structured work, manifest/workflow evidence 역할을 담당하며, 기술 조사 및 반복적 구조화 작업 등을 수행합니다.
- **Gemini/AGY**: wording cleanup, summarization, checklist polishing, read-only documentation review, documentation consistency checks 역할을 담당하며, 문구 정리, 요약, 체크리스트 다듬기 및 문서 일관성 유지를 수행합니다.

### 태스크 관리 파일 경로
- `.agent_tasks/deepseek_next.md` / `.agent_tasks/deepseek_result.md`
- `.agent_tasks/gemini_agy_next.md` / `.agent_tasks/gemini_agy_result.md`

### 운영 로그 조회 규칙
- 운영 로그 확인은 사람이 긴 SSH 명령을 복붙하기 전에 `scripts/ops_tail_errors.sh`를 우선 사용한다.
- 사용법과 안전 경계는 `docs/OPS_LOG_TAIL.md`를 따른다.
- 이 스크립트는 읽기 전용이며, LLM은 로그 조회 결과를 근거로 코드 변경/배포/DB 수정이 필요한지 별도 제안만 한다.

### 응답 처리 방식 라벨
- `처리 방식: Codex 직접 처리`
- `처리 방식: 다른 LLM 위임`
- `처리 방식: 사용자 확인 필요`

*상세 운영 프로세스는 외부 가이드인 [AGENT_OPERATING_GUIDE.md](file:///home/ubuntu/workspace/AGENT_OPERATING_GUIDE.md)를 참고하시기 바랍니다.*

---

## 검증 명령 레퍼런스

### 현재 바로 사용 가능

```bash
# 필수 — 모든 변경 후 실행
uv run pytest tests/test_core_contract.py tests/test_scraper_imports.py -q

# standalone 30개 Python 문법 검증 (_TEMPLATE.py 제외)
bash scripts/verify_standalones.sh

# Dockerfile 디렉토리 COPY 누락 검증
bash scripts/verify_dockerfile.sh

# 단일 firm result JSON 검증 (--require-non-empty는 현재 default True)
uv run python scripts/validate_scrape_result.py {firm}_result.json --require-non-empty

# 주의: 현재 validate_scrape_result.py에는 allow-empty CLI 옵션이 없다.
# 0건 허용 정책은 firm manifest/harness 구현 시 별도 추가해야 한다.
```

**workflow/env/result filename 확인용 grep:**

```bash
# workflow 안의 result json 파일명 확인
grep -n ">.*\.json" .github/workflows/scrape-{firm}.yml

# workflow가 주입하는 env 이름 확인
grep -n "URLS_JSON\|urls:" .github/workflows/scrape-{firm}.yml

# standalone이 읽는 env 이름 확인
grep -n "URLS_JSON\|urls" run/standalone/{firm}.py

# workflow env 이름과 standalone env 이름 일치 여부 비교
diff <(grep -oh '[A-Z_]*URLS_JSON\|urls' .github/workflows/scrape-{firm}.yml | head -1) \
     <(grep -oh '[A-Z_]*URLS_JSON\|urls' run/standalone/{firm}.py | head -1)

# Dockerfile에서 scrapers/ COPY 확인
grep -n "COPY.*scrapers" Dockerfile
```

**firm별 테스트 (있는 경우만):**

```bash
uv run pytest tests/test_shinhan_core.py -q   # 신한
uv run pytest tests/test_sks_core.py -q        # SK
uv run pytest tests/test_standalone_runner.py -q  # 공통 _runner
```

### 하네스 구현 후 사용

아래 명령은 `scripts/harness.py`와 `config/firms.yaml`이 구현된 이후에 사용할 수 있다. 현재는 아직 구현되지 않았다.

```bash
# 단일 firm offline 검증
uv run python scripts/harness.py --firm {firm} --offline

# 전체 firm offline 검증
uv run python scripts/harness.py --all --offline

# manifest consistency 검증
uv run python scripts/harness.py --check-manifest
```

---

## 장애 패턴 → 진단 명령

| 증상 | 먼저 확인할 파일·명령 |
|------|----------------------|
| GA workflow 성공인데 서버 발송 안 됨 | `grep -n "GA_FIRMS" scraper.py` → 해당 firm이 GA set에 있는지 확인. 서버 scheduler 로그에서 import 확인. |
| standalone import error | `uv run python -c "import py_compile; py_compile.compile('run/standalone/{firm}.py', doraise=True)"` |
| server module import error | `uv run python -c "from modules.{ModuleName}_{order} import *"` |
| core module import error | `uv run python -c "from scrapers.{firm}_core import scrape_{firm}"` |
| result json 빈 배열 | `wc -c {firm}_result.json` → 0바이트면 Python crash. `uv run python scripts/validate_scrape_result.py {firm}_result.json` → exit code 확인. |
| workflow 실패했는데 stderr 로그 안 보임 | `grep -A5 "run:" .github/workflows/scrape-{firm}.yml \| grep -E "cat\|echo\|2>"` → `cat *_log.txt`가 실패 경로에서도 실행되는지 확인 |
| server module과 core 인자 불일치 | `grep -n "import.*_core\|scrape_" modules/{ModuleName}_*.py` → core 함수 호출 인자 확인 |
| config shape 불일치 (`list` vs `dict`) | `grep -n "isinstance.*cfg\|isinstance.*list\|isinstance.*str" scrapers/{firm}_core.py` → backward compat 분기 확인 |
| telegram_sent / is_sent / main_ch_send_yn 혼용 | `grep -rn "is_sent\|main_ch_send_yn" modules/ scrapers/ scraper.py` → `telegram_sent`만 사용해야 함 |
| 정상 0건과 파싱 장애 구분 불가 | `grep -E "FATAL\|Traceback\|Error\|CRITICAL" {firm}_log.txt` → 로그 먼저 확인. 그 후 validator exit code 확인. |
| LS 특수 경로 문제 | `grep -n "get_db\|FirmInfo\|WARP\|skip_boards\|USE_WARP_ONLY" modules/LS_0.py` → LS 전용 패턴 확인. `run/standalone/ls.py`가 `modules/LS_0.py`를 직접 import하는지 확인. |
| workflow cron/env/secret 이름 임의 변경 의심 | `git diff main -- .github/workflows/scrape-{firm}.yml` → cron, `secrets.`, `env:` 라인만 확인 |
| GA 러너 IP 차단 (하나증권, BNK) | 증상: timeout 후 0건. 서버에서 동일 URL 정상 작동하는지 확인. GA workflow cron 제거 검토. |
| Selenium 의존성 (한국투자) | workflow에 `browser-actions/setup-chrome@v1` 스텝이 있는지 확인. `run/standalone/koreainvestment.py`가 `modules/Koreainvestment_13.py`의 selenium 함수를 호출하는지 확인. |

`gh run view` 등의 GitHub CLI 명령은 **GitHub CLI 사용 가능 시**에만 사용한다.

---

## Firm별 예외 규칙

이 규칙은 "대부분의 firm이 core → standalone → workflow 패턴을 따른다"는 전제에서 **벗어나는 firm**만 기술한다. LS에 "standalone이 없다" 또는 "GA workflow가 없다"고 쓰지 않도록 주의한다. LS에는 `run/standalone/ls.py`, `run/standalone/ls_v2.py`, `scripts/standalone_ls_scraper.py` 3종 standalone과 `scrape-ls.yml`(cron 활성), `scrape-ls-v2.yml` 2종 workflow가 존재한다.

### LS (LS_0.py, sec_firm_order=0)

- `run/standalone/ls.py`는 `modules/LS_0.py`의 `LS_checkNewArticle()`을 직접 호출하는 동기 함수다. `run/standalone/_runner.py`를 사용하지 않는다.
- `modules/LS_0.py`는 DB 의존성(`FirmInfo`, `get_db`), WARP 프록시(`SOCKS_PROXY`, `USE_WARP_ONLY`), 전역 상태(`skip_boards`)를 갖는다. 일반 `scrapers/*_core.py`처럼 독립 함수로 분리되어 있지 않다.
- `scrape-ls.yml`은 env 이름으로 `urls`를 사용한다 (대다수 firm의 `*_URLS_JSON` 패턴과 다름).
- `scrape-ls-v2.yml`는 schedule 없이 `workflow_dispatch`만 활성화되어 있으며, `run/standalone/ls_v2.py`를 사용한다.
- `scripts/standalone_ls_scraper.py`는 DB 의존성 없이 순수 HTTP 크롤링만 수행하는 GA 전용 스크래퍼다.
- **금지**: LS에 일반 core import 패턴을 강제하지 마라. LS 수정 시 `modules/LS_0.py` + `run/standalone/ls.py` + `run/standalone/ls_v2.py` + `scripts/standalone_ls_scraper.py` + `scrape-ls.yml` + `scrape-ls-v2.yml` 6개 파일을 함께 확인한다.

### DBfi (DBfi_19.py, sec_firm_order=19)

- PDF URL 추출 로직이 2벌 존재: async `extract_dbfi_pdf_url()` + sync `DBfi_detail()`. 수정 시 양쪽 모두 확인.
- standalone은 `_runner.py` + `scrapers/dbfi_core.py` 정규 패턴을 따른다.

### 하나증권 (HANA_3.py, sec_firm_order=3)

- GA workflow(schedule) 비활성화 (2026-06-22). GA 러너 IP(미국/유럽)가 `www.hanaw.com`에서 차단되어 17개 URL 전부 timeout.
- `run/standalone/hana.py`는 존재하며 `_runner.py` + `scrapers/hana_core.py` 정규 패턴을 따른다. `workflow_dispatch`로 수동 실행은 가능하다.
- 서버에서는 `modules/HANA_3.py` → `scrapers/hana_core.py`로 동작. `modules/HANA_3.py`는 `aiohttp` 직접 사용 (WebScraper 미사용).
- workflow validate는 `--require-non-empty` 없이 실행 (default True이므로 실질 동일).

### 신한투자 (ShinHanInvest_1.py, sec_firm_order=1)

- 서버 전용 (GA standalone 미해당). `scrapers/shinhan_core.py`로 delegate.
- `SyncWebScraper` + `aiohttp` 직접 사용 혼합. `_back` 함수는 dead code (비활성, 삭제 금지 — rollback 대비).
- env 이름으로 `urls` 사용.

### NH투자 (NHQV_2.py, sec_firm_order=2)

- GA + 서버 듀얼모드. `run/standalone/nhqv.py` + `scrapers/nhqv_core.py` 정규 패턴.
- `modules/NHQV_2.py`는 `aiohttp` 직접 사용 (WebScraper 미사용). core delegate 구조다.
- core는 JSON POST 기반. `page_size=11`, `item_keys`로 응답 필드 매핑.

### 한국투자 (Koreainvestment_13.py, sec_firm_order=13)

- **Selenium 의존**: `run/standalone/koreainvestment.py`가 `modules/Koreainvestment_13.py`의 `Koreainvestment_selenium_checkNewArticle()`을 직접 호출한다. `_runner.py` 미사용.
- `scrapers/koreainvestment_core.py`는 **존재하지 않는다** — core 정규 패턴으로 이관되지 않음.
- GA workflow에 `browser-actions/setup-chrome@v1` 스텝이 필수다. ARM64/amd64 ChromeDriver 경로 분기 있음.
- 서버 전용 (GA_STATUS 기준). env 이름으로 `urls` 사용.

### BNK (BNKfn_23.py, sec_firm_order=23)

- `run/standalone/bnk.py` **없음** — `scripts/standalone_bnk_scraper.py`를 사용하는 비정규 패턴.
- GA workflow cron 비활성화 (IP 차단, `BLOCKED_BY_SOURCE_IP`). `workflow_dispatch`만 가능.
- **주의**: IP 차단은 코드 문제가 아니다. BNK 모듈을 수정하기 전에 IP 차단 해제 여부를 먼저 확인한다.

### IM (iMfnsec_18.py, sec_firm_order=18)

- `run/standalone/imfn.py`는 `_runner.py` + `scrapers/imfn_core.py` 정규 패턴.
- GA workflow cron 활성 (`0 * * * 1-5`). `IMFN_URLS_JSON` env 사용.
- 현재 인증만료 상태 (secure key 갱신 필요). **주의**: 0건이 인증만료 때문인지 파싱 오류 때문인지 구분한다.

---

## DeepSeek용 GA 장애 대응 프롬프트

기본 프롬프트가 범용 수정용이라면, 아래는 **GA workflow 실패 진단·수정 전용**이다.

~~~text
너는 ssh-reports-scraper 레포의 GA workflow 장애 대응 담당자다.
목표는 특정 firm의 GA workflow 실패 원인을 진단하고 최소한의 수정으로 복구하는 것이다.

반드시 먼저 읽어라:
- .github/workflows/scrape-{firm}.yml — 전체 파일
- run/standalone/{firm}.py — env 이름, import 구조
- scrapers/{firm}_core.py — config shape 분기 확인 (또는 modules/{module}.py)
- scripts/validate_scrape_result.py — 검증 기준 확인
- docs/LLM_PAIN_POINTS.md — 0건 정책, env 불일치, 로그 누락 항목

진단 순서:
1. workflow 파일에서 cron, env 이름, result filename, SCP target을 확인한다.
2. standalone 파일이 읽는 env 이름과 workflow가 주입하는 env 이름이 일치하는지 확인한다.
3. core 함수가 list/dict 중 무엇을 기대하는지 확인한다.
4. GA run log에서 FATAL, Traceback, Error, CRITICAL 라인을 찾는다.
5. 로컬 재현이 가능하면 동일 env로 standalone을 직접 실행한다.

수정 허용:
- scrapers/{firm}_core.py — config shape backward compat 분기 추가
- run/standalone/{firm}.py — env 이름, import 경로 수정
- 해당 firm의 workflow .yml — 실패 시 로그 출력 패턴 개선 (cat *_log.txt가 실행되도록)
- tests/ — 회귀 테스트 추가

수정 금지:
- cron schedule 임의 변경 (cron 변경은 사람이 승인)
- secret/env 이름 임의 변경
- 다른 firm의 파일 수정
- broad except: pass 추가
- DB schema 변경
- 전체 formatter 적용

검증:
1. bash scripts/verify_standalones.sh → exit 0
2. uv run pytest tests/test_core_contract.py tests/test_scraper_imports.py -q → all green
3. 수정한 workflow YAML diff 확인 → cron/env/result filename 불변

최종 보고:
1. 진단: 실패 원인 3줄
2. 수정: 변경 파일과 내용
3. 검증 결과: 실행한 명령과 exit code
4. 재발 가능성: 같은 패턴이 다른 firm에 있는지
~~~

---

## LLM 변경 결과 검증 체크리스트

LLM 작업 완료 보고에 아래 체크리스트 출력을 요구한다. 사람은 통과 여부만 확인한다.

```text
[ ] bash scripts/verify_standalones.sh → exit 0
[ ] bash scripts/verify_dockerfile.sh → exit 0
[ ] uv run pytest tests/test_core_contract.py tests/test_scraper_imports.py -q → all green
[ ] uv run pytest tests/test_{firm}_core.py -q → all green (해당 firm 테스트 존재 시)
[ ] git diff main --name-only → 수정 대상 firm 외 파일 없음
[ ] git diff main -- .github/workflows/ → cron/env/result filename 불변 확인
[ ] grep -rn "except\s*Exception\s*:\s*pass" → 새로 추가된 라인 없음
[ ] grep -rn "except\s*:" → 새로 추가된 빈 except 없음
[ ] uv run python scripts/validate_scrape_result.py {firm}_result.json → exit 0 (현재는 empty result도 exit 1)
```

---

## 하네스가 막았을 장애 사례

### 사례 1: Dockerfile COPY 누락 (실제, 2026-06-12)

- **원인**: `scrapers/` 디렉토리 신설 후 Dockerfile 미갱신.
- **증상**: `scraper.py`에서 ModuleNotFoundError → 모든 수집·발송 30시간 중단.
- **현재 방어**: `scripts/verify_dockerfile.sh` pre-push 훅.
- **하네스 검출**: `harness.py --all --offline` → "Dockerfile COPY missing: scrapers/"

### 사례 2: standalone 문법 오류 (실제, 2026-06-12)

- **원인**: sed로 standalone 수정 시 괄호·따옴표 파편 발생.
- **증상**: GA workflow 전체 SyntaxError → GA 수집 12시간 중단.
- **현재 방어**: `scripts/verify_standalones.sh` pre-push 훅.
- **하네스 검출**: `harness.py --firm {firm} --offline` → "standalone py_compile failed"
- **참고**: 2026-06-24 현재 `ls_v2.py`가 `verify_standalones.sh`에서 문법 오류 발생 중.

### 사례 3: core/wrapper config shape 불일치 (실제, 2026-06-15)

- **원인**: core 함수 signature를 config dict로 변경했으나 server wrapper는 URL 리스트 전달.
- **증상**: `invalid input syntax for type integer: ""` → DB 에러, 데이터 누락 가능성.
- **현재 방어**: core 함수 내 `isinstance(cfg, list/str)` backward compat 추가.
- **하네스 검출**: `harness.py --firm {firm} --offline` → "config_shape mismatch: standalone=list, core=dict"

### 사례 4: workflow env 이름 불일치 (가상)

- **원인**: standalone은 `KB_URLS_JSON`을 읽는데 workflow가 `KBSEC_URLS_JSON`을 주입.
- **증상**: GA runtime에서 env 읽기 실패 → result.json 0바이트 또는 KeyError.
- **하네스 검출**: `harness.py --firm kb --offline` → "env_var mismatch: manifest=KB_URLS_JSON, workflow=KBSEC_URLS_JSON"

### 사례 5: result filename 불일치 (가상)

- **원인**: core가 `kb_result.json`으로 출력하는데 workflow SCP는 `kbsec_result.json`을 전송.
- **증상**: GA는 성공, 서버에서 `incoming/ga-scrapes/`에 파일 없음 → 수집 누락.
- **하네스 검출**: `harness.py --firm kb --offline` → "result_file mismatch: manifest=kb_result.json, workflow=kbsec_result.json"
