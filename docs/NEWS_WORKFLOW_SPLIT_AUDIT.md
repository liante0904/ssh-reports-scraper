# News Workflow Split Audit

> 작성일: 2026-06-24
> 대상: ssh-reports-scraper ↔ naver-stock-news 뉴스 스크래핑 책임 분리

## 결론

**ssh-reports-scraper에서 `scrape-news.yml` schedule을 제거하고 `workflow_dispatch` only로 전환할 것을 권장한다.**

이유:
1. ssh-reports-scraper의 뉴스 SCP 경로는 서버 import 단계에서 전량 필터링되어 DB에 insert되지 않는다. 현재 기준으로 비활성 경로에 가깝다.
2. naver-stock-news가 동일한 3개 뉴스 소스를 Docker 컨테이너에서 직접 Telegram 발송하고 있다. 뉴스 전달은 이미 이쪽이 담당한다.
3. `*/5 * * * *` cron은 월 ~8,640회 실행되어 private repo 전환 시 Actions 2,000분 무료 한도를 단독으로 초과한다 (약 13,000분/월 예상).
4. 공개 레포에 SSH_PRIVATE_KEY를 두는 SCP 패턴은 보안 위험이 크다. naver-stock-news의 SCP도 제거하는 것이 안전하다.

**naver-stock-news 공개 레포 이관은 이미 완료된 상태다.** naver-stock-news는 자체 Docker 컨테이너 + 직접 Telegram 발송으로 독립 운영 중이며, ssh-reports-scraper에 의존하지 않는다. 추가 이관 작업은 필요하지 않다. 다만 양쪽 레포 모두 불필요한 SCP 경로를 정리하는 후속 PR이 필요하다.

---

## 현재 ssh-reports-scraper 뉴스 흐름

### Workflow
- **파일**: `.github/workflows/scrape-news.yml`
- **Schedule**: `*/5 * * * *` (5분 간격, 288회/일)
- **Timeout**: 5분
- **Secrets 사용**: `SSH_PRIVATE_KEY`, `SERVER_HOST`, `SERVER_USER`, `SERVER_PORT`

### Entrypoint → Core → Result
```
run/standalone/news.py
  → scrapers/news_core.py::scrape_all_news()
    → scrape_chosun_biz()       # firm_id=100, firm_nm="조선비즈"
    → scrape_naver_flash()      # firm_id=101, firm_nm="네이버"
    → scrape_naver_rank()       # firm_id=101, firm_nm="네이버"
  → stdout → news_result.json
  → SCP → incoming/ga-scrapes/
```

### 서버 Import 처리 (scheduler.py `run_ga_import()`)
```python
# scheduler.py:132-136
EXCLUDED_FIRMS = {"네이버", "조선비즈"}
filtered = [d for d in data if d.get("firm_nm") not in EXCLUDED_FIRMS]
```
→ `news_result.json`의 모든 row가 필터링됨 → DB insert 건수 0

### DB Insert 차단 (models/SecReportsManager.py)
```python
# SecReportsManager.py:28-34
EXCLUDED_FIRMS = {"네이버", "조선비즈"}
json_data_list = [e for e in json_data_list if e.get("firm_nm") not in EXCLUDED_FIRMS]
```
→ 중앙 차단: `insert_json_data_list()` 진입 시점에 뉴스 row 전체 제거

### Telegram Broadcast 차단 (scheduler.py `_broadcast_ga_reports()`)
```python
# scheduler.py:182-183
WHERE ... AND firm_nm NOT IN ('네이버', '조선비즈')
# scheduler.py:201
if firm_nm in EXCLUDED_FIRMS: continue
```
→ 뉴스 row는 Telegram 발송 대상에서도 제외

### 결론: 비활성 경로

ssh-reports-scraper의 뉴스 workflow는 현재 레포트 파이프라인 관점에서 **실질적으로 사용되지 않는 경로**다:
1. GA runner가 5분마다 뉴스 API 호출 → 비용 발생
2. SCP로 서버 전송 → 네트워크/IO 비용 발생
3. 서버 import가 전량 필터링 → DB insert 0건
4. Telegram broadcast도 전량 필터링 → 발송 0건

---

## 현재 naver-stock-news 구조

### Entrypoint
- **파일**: `app.py` → `scrapers/news.py::NewsScraper` → `scrapers/news_core.py`
- **동작 모드**: Docker 컨테이너에서 무한 루프 (5분 정시 aligned)
- **3개 소스**: chosun_biz, naver_flash, naver_rank (ssh-reports-scraper와 동일)

### GitHub Actions Workflow
- **파일**: `.github/workflows/scrape-news.yml`
- **Schedule**: KST 05~19시, 6회/시간 × 15시간 = 90회/일
- **Entrypoint**: `run/standalone/news.py` → `scrapers/news_core.py`
- **Output**: `news_result.json` → validate → **SCP to incoming/ga-scrapes/** (ssh-reports-scraper와 동일 경로)

### Docker 서비스 (실제 동작 경로)
- `app.py`가 Docker 컨테이너에서 영구 실행
- `NewsScraper`가 5분마다 3개 소스 스크래핑
- SQLite `news_history` 테이블에 중복 체크
- 신규 기사 → Telegram 3개 채널 직접 발송:
  - `TELEGRAM_CHANNEL_ID_CHOSUNBIZBOT`
  - `TELEGRAM_CHANNEL_ID_NAVER_FLASHNEWS`
  - `TELEGRAM_CHANNEL_ID_NAVER_RANKNEWS`

### Secrets/Env
- `TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET` — Telegram 봇 토큰
- `TELEGRAM_CHANNEL_ID_CHOSUNBIZBOT` — 조선비즈 채널
- `TELEGRAM_CHANNEL_ID_NAVER_FLASHNEWS` — 네이버 속보 채널
- `TELEGRAM_CHANNEL_ID_NAVER_RANKNEWS` — 네이버 랭킹뉴스 채널
- GitHub Actions secrets: `CHOSUN_BIZ_API_URL`, `NAVER_FLASH_API_URL`, `NAVER_FLASH_LINK_TPL`, `NAVER_RANK_API_URL`, `NAVER_RANK_LINK_TPL`, `SSH_PRIVATE_KEY`, `SERVER_HOST`, `SERVER_USER`, `SERVER_PORT`

### 이중 Delivery 문제
naver-stock-news는 뉴스를 **두 경로**로 보내고 있다:
1. Docker 컨테이너 → Telegram 직접 발송 (실제 작동)
2. GitHub Actions → SCP → server incoming/ga-scrapes/ → import 시 필터링 처리 (불필요한 경로)

---

## 결합도 평가

| 항목 | 결합 여부 | 근거 |
|------|-----------|------|
| 레포트 DB insert 필요 | **no** | `scheduler.py:132` EXCLUDED_FIRMS, `SecReportsManager.py:28` 중앙 차단 |
| 레포트 Telegram 채널 발송 필요 | **no** | `scheduler.py:182` WHERE firm_nm NOT IN, `scheduler.py:201` continue skip |
| ssh-reports-scraper 내부 모델 의존 | **no** | `scrapers/news_core.py`는 aiohttp + html 만 의존. FirmInfo, ConfigManager, db_factory 미사용 |
| incoming/ga-scrapes 의존 | **no** (의존은 하지만 dead) | SCP는 되지만 import 시 전량 필터링 |
| 별도 공개 레포 운영 가능 | **yes** (이미 완료) | naver-stock-news는 자체 Docker + Telegram direct로 완전 독립 운영 중 |

---

## Private Repo Actions 비용 영향

### scrape-news.yml 월 예상

| 항목 | 값 |
|------|:--:|
| Cron | `*/5 * * * *` |
| 1일 실행 횟수 | 288회 |
| 30일 실행 횟수 | ~8,640회 |
| run당 예상 소요 시간 | ~1.5분 (checkout + uv sync + scrape + SCP) |
| 월 예상 minutes | ~12,960분 |

### Private repo 한도 비교

| 항목 | 값 |
|------|:--:|
| GitHub Free private minutes | 2,000분/월 |
| scrape-news.yml 단독 소비 | ~12,960분/월 |
| 초과율 | **6.5배** — news workflow 하나만으로 전체 한도 초과 |

### 타 workflow minutes 추정

scrape-news 외 28개 증권사 workflow도 존재. 대부분 `0 * * * 1-5`(평일 매시간) 패턴이므로:
- 평일 매시간 = 8회/일 × 22일 = 176회/월/workflow
- 28개 × 176회 × 1분 = ~4,928분/월

**총 예상: 12,960 + 4,928 = ~17,888분/월 → private repo 무료 한도를 크게 초과**

scrape-news.yml 제거만으로도 72% 절감 → 나머지 28개 workflow는 ~5,000분/월 → 여전히 2,000분 초과지만, cron 주기 조정이나 일부 workflow_dispatch 전환으로 대응 가능한 범위.

---

## 이관 옵션

### Option A: naver-stock-news Docker 컨테이너에 완전 위임 (강력 권장)

naver-stock-news의 Docker 컨테이너(`app.py`)가 이미 5분 주기로 뉴스를 수집해 Telegram 3개 채널에 직접 발송하고 있다.

**장점:**
- GitHub Actions minutes 0 소비 (컨테이너는 서버에서 실행)
- SSH_PRIVATE_KEY 불필요 (Telegram API만 사용)
- 뉴스 채널 분리 운영 가능
- 이미 운영 중인 경로 — 추가 개발 불필요

**단점:**
- 서버 리소스 사용 (CPU/memory — 경미함)
- 컨테이너 장애 시 뉴스 수집 중단

**필요한 secrets:**
- `TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET` (이미 .env에 존재)
- `TELEGRAM_CHANNEL_ID_CHOSUNBIZBOT` 등 3개 채널 ID (이미 .env에 존재)

**ssh-reports-scraper 변경점:**
- `scrape-news.yml` cron 제거 (`workflow_dispatch` only 또는 파일 삭제)
- `scrapers/news_core.py`, `run/standalone/news.py` 보존 또는 삭제 (결정 필요)

### Option B: naver-stock-news 공개 레포에서 SCP (보안 보완 필요)

naver-stock-news의 GitHub Actions workflow가 SCP로 서버에 전송.

**장점:**
- GitHub Actions 공개 레포 무료 runner 사용
- 서버 import 파이프라인 재사용

**단점:**
- **공개 레포에 SSH_PRIVATE_KEY 저장 → secrets 자체는 GitHub가 보호하지만, maintainer가 악성/실수 workflow를 merge하면 서버 접근 키가 노출될 수 있음**
- 서버 import가 뉴스를 전량 필터링하므로 현재 구조에서는 실질적인 효과가 없음
- GitHub Actions minutes는 무료지만, SCP 자체가 무의미

### Option C: 뉴스는 서버 scheduler/컨테이너에서 직접 실행 (이미 구현됨)

naver-stock-news의 Docker 컨테이너가 이 방식으로 이미 동작 중이다. 추가 작업 불필요.

**장점:**
- GitHub Actions minutes 0 소비
- Secret 노출 위험 없음 (서버 내 .env)
- IP 차단 이슈 없음 (서버 IP 사용)

**단점:**
- 서버 의존성 (장애 시 수집 중단)
- Docker 컨테이너 유지보수 필요

### Option D: scrape-news 완전 제거

ssh-reports-scraper에서 news_core.py, run/standalone/news.py, scrape-news.yml을 삭제.

**장점:**
- 코드베이스 단순화
- GitHub Actions minutes 완전 절감
- 유지보수 부담 제거

**단점:**
- naver-stock-news 장애 시 fallback 없음
- naver-stock-news Docker 컨테이너에 대한 의존성 증가

---

## 권장안

**Option A + Option D 혼합** — naver-stock-news Docker 컨테이너에 완전 위임 + ssh-reports-scraper에서 뉴스 경로 제거.

내 우선순위 기준 평가:

| 우선순위 | 기준 | 평가 |
|:---:|------|------|
| 1 | private Actions minutes 제거 | 12,960분/월 → 0분. 가장 큰 단일 비용 제거. |
| 2 | secret 노출 최소화 | 공개 레포에 SSH_PRIVATE_KEY, SERVER_HOST 등 민감 정보 불필요. Telegram token만 서버 .env에 존재. |
| 3 | 레포트/뉴스 파이프라인 분리 | 이미 분리됨. naver-stock-news는 자체 DB + 자체 Telegram 채널. |
| 4 | 운영 복잡도 최소화 | 불필요한 경로 제거로 단순화. SCP → import → filter → discard 흐름이 사라짐. |

---

## 최소 변경 PR 계획

### PR 1: 문서 추가 (본 PR)
- `docs/NEWS_WORKFLOW_SPLIT_AUDIT.md` 추가
- 코드 변경 없음
- 승인 후 다음 PR 진행

### PR 2: ssh-reports-scraper scrape-news.yml cron 제거
- `.github/workflows/scrape-news.yml`: `schedule` 블록 제거, `workflow_dispatch`만 유지
- **근거**: SCP 경로가 현재 사용되지 않으며 naver-stock-news Docker가 실제 발송 담당
- 영향: GitHub Actions 월 12,960분 절감

### PR 3: naver-stock-news scrape-news.yml에서 SCP 제거
- `.github/workflows/scrape-news.yml`: SCP step 제거 (또는 전체 workflow_dispatch only 전환)
- **근거**: 공개 레포 SSH_PRIVATE_KEY 보안 위험. Docker 컨테이너가 이미 Telegram direct 발송 중.
- Docker 컨테이너의 Telegram direct 경로는 유지

### PR 4: 서버 incoming import 로그에서 뉴스 노이즈 제거
- `scheduler.py`: `run_ga_import()` 내 EXCLUDED_FIRMS 로그를 `INFO`에서 `DEBUG`로 낮추거나, news_result.json 자체를 incoming에서 무시
- **근거**: SCP 중단 후에도 남아있는 필터링 로직 정리

### PR 5: ssh-reports-scraper에서 news_core/standalone 정리 (결정 필요)
- `scrapers/news_core.py`, `run/standalone/news.py` 보존 또는 삭제
- **보존 시**: 추후 emergency fallback 용도로 주석 처리
- **삭제 시**: `Dockerfile`, `verify_standalones.sh`에서 news.py 참조 제거 필요

---

## 사람이 확인해야 할 결정

| # | 결정 사항 | 현황/권장 |
|:---:|---|------|
| 1 | naver-stock-news를 public으로 유지할지 | **public 유지** — GitHub Actions 무료 runner, 민감 정보 없음 (Telegram token만 .env, repo에 없음) |
| 2 | 뉴스 Telegram 채널을 별도로 쓸지 | **별도 채널 유지** — 이미 조선비즈/네이버속보/네이버랭킹 3개 채널 운영 중 |
| 3 | 공개 레포에 SSH secret을 둘지 | **제거 권장** — naver-stock-news의 SCP step 삭제 (PR 3). Docker direct로 충분 |
| 4 | 서버로 SCP할지 직접 Telegram 발송할지 | **Telegram direct** — 이미 동작 중. SCP는 현재 구조에서 불필요 |
| 5 | ssh-reports-scraper에서 news_core.py를 삭제할지 보존할지 | **보존 후 검토** — 1개월간 naver-stock-news Docker 안정성 확인 후 삭제 결정 |
