# SSH Reports Scraper

> 국내 28개 증권사 리서치 보고서를 실시간 수집·분류·발송하는 개인 운영 자동화 시스템 (2021~)

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Docker](https://img.shields.io/badge/Docker-GHCR-2496ED)
![PostgreSQL](https://img.shields.io/badge/DB-PostgreSQL-336791)
![Uptime](https://img.shields.io/badge/운영기간-4년-brightgreen)

---

## 어떤 프로젝트인가

증권사 리서치 보고서는 각사 홈페이지에 분산돼 있고, 공식 통합 API가 없다.  
이걸 하나의 파이프라인으로 묶어 텔레그램 채널에 자동 발송하는 시스템을 2021년부터 혼자 설계·운영 중이다.

**단순 크롤러가 아니라 4년간 실제 사용하면서 진화한 운영 시스템이다.**

---

## 현재 상태

- 28개 증권사, 30분 간격 자동 수집
- Oracle Cloud (OCI) 서버에서 Docker 컨테이너로 24/7 운영 중
- GitHub Actions → GHCR → SSH 자동 배포 (main 브랜치 푸시 시)
- AI 요약 (Gemini), 키워드 알림 독립 프로세스로 병렬 운영

---

## 기술적으로 풀었던 문제들

### 1. 28개 사이트, 28가지 방식

증권사마다 HTML 구조, 인증 방식, 페이지네이션이 전부 다르다.  
공통 인터페이스(`WebScraper`)를 설계하고 증권사별 모듈이 이를 구현하는 구조로 확장성을 확보했다.  
일부는 Selenium 헤드리스, 일부는 aiohttp 비동기, 일부는 세션 쿠키 유지가 필요하다.

### 2. 3680줄 단일 파일 → 모듈 시스템

2021년 첫 버전은 `main.py` 한 파일에 모든 증권사 로직이 들어있었다.  
운영하면서 기능을 추가할수록 유지보수가 불가능해졌고, 2024년에 전면 모듈 분리를 단행했다.  
현재는 증권사 추가 시 모듈 파일 하나만 작성하면 된다.

### 3. SQLite → PostgreSQL 운영 전환

운영 중인 서비스의 DB를 바꾸는 건 까다롭다.  
초기에는 SQLite로 로컬 데이터를 쌓았고, 이후 Oracle 검증 단계를 거쳐 현재는 PostgreSQL을 운영 단일 DB로 사용한다.
SQLite 관련 런타임/compose/tooling은 main에서 제거했고, 필요 시 `archive/sqlite-legacy-20260705` 브랜치에서 이력을 확인한다.

```python
# db_factory.py
def get_db():
    return SecReportsManager()
```

뉴스(네이버/조선비즈) standalone workflow/core는 제거되었고, 뉴스 발송은 `naver-stock-news` 컨테이너가 담당한다.

### 4. 시크릿 관리

API 키, DB 비밀번호, 수집 대상 URL(경쟁 우위 정보)을 소스코드에서 완전히 분리했다.  
`secrets.json` → `generate_env.py` → `.env` 파이프라인으로 컨테이너에 환경변수로 주입한다.  
수집 URL은 Git 히스토리 포함 전체 삭제(`git filter-repo`)하고 컨테이너 런타임에만 노출된다.

### 5. 중복 제거 전략

같은 보고서가 여러 번 수집될 수 있습니다.  
기존 `key` 컬럼 대신 고유 식별자인 `report_unique_key` 컬럼에 `ON CONFLICT DO UPDATE`를 적용하여 DB 레벨에서 완벽한 멱등성을 보장합니다.  

### 6. 물리 데이터 구조 현대화 (2026-07-01)
- **컬럼명 정규화**: `sec_firm_order` -> `firm_id`, `article_board_order` -> `board_id`로 개편하여 직관성을 높였습니다.
- **날짜/시간대 표준화**: 레거시 `save_time` (VARCHAR) 대신 시간대를 지원하는 `save_at` (TIMESTAMPTZ) 컬럼으로 전환하여 데이터 시간 정합성을 강화했습니다.
- **기사 식별키 통합**: 구형 `key` 컬럼의 데이터 적재를 전면 중단하고 `report_unique_key` 로 일원화하였습니다.

---

## 아키텍처

```
[GitHub Actions]
      │ push to main
      ▼
[GHCR :prod 이미지 빌드]
      │ SSH 자동 배포
      ▼
[Oracle Cloud (OCI)]
  ├── nginx (리버스 프록시 + SSL)
  ├── main-scraper (30분 스케줄, 28개 증권사)
  ├── keyword-alert (키워드 매칭 → 개인 DM)
  └── PostgreSQL + pgAdmin4
```

**관심사 분리:** 뉴스(네이버), 한경컨센서스는 별도 레포·컨테이너로 독립 운영 중

---

## 기술 스택

| 영역 | 기술 |
|---|---|
| Language | Python 3.12, uv |
| Scraping | aiohttp, BeautifulSoup4, Selenium (headless) |
| Scheduler | APScheduler |
| Database | PostgreSQL (운영 단일 DB) |
| AI / Data | Gemini API (요약), Enricher (태그/섹터 추출) |
| Network | aiohttp, requests (SOCKS5/WARP fallback) |
| Logging | Loguru (날짜별 자동 로테이션) |
| Infra | Docker, GitHub Actions, GHCR, Oracle Cloud |

---

## 프로젝트 구조

```
ssh-reports-scraper/
├── scraper.py                      # 메인 스케줄러
├── scheduler_keyword_alert.py      # 키워드 알림 스케줄러
├── modules/                        # 증권사별 스크래퍼 (28개)
├── enricher/                       # 데이터 강화 (태그/섹터 추출)
├── models/
│   ├── ConfigManager.py            # 환경별 설정 싱글톤
│   ├── FirmInfo.py                 # 증권사/게시판 메타 (DB 기반)
│   ├── SecReportsManager.py        # PostgreSQL CRUD
│   ├── db_factory.py               # DB_BACKEND 팩토리
│   └── WebScraper.py               # HTTP/Selenium 공통 추상화
├── docs/
│   ├── architecture.md             # ADR 및 설계 결정 기록
│   ├── changelog.md                # 2021~현재 변천사
│   └── url-semantics.md            # URL 컬럼 규약
└── sql/                            # PostgreSQL DDL (소문자 표준화)
```

---

## 수집 대상 (28개 증권사)

LS증권 · 신한투자증권 · NH투자증권 · 하나증권 · KB증권 · 삼성증권 · 상상인증권 · 신영증권 · 미래에셋증권 · 현대차증권 · 키움증권 · DS투자증권 · 유진투자증권 · 한국투자증권 · 다올투자증권 · 토스증권 · 리딩투자증권 · 대신증권 · iM증권 · DB금융투자 · 메리츠증권 · 한화투자증권 · 흥국증권 · BNK투자증권 · 교보증권 · IBK투자증권 · SK증권 · 유안타증권

---

## 운영 이력

| 시기 | 상태 |
|---|---|
| 2021 | `main.py` 단일 파일, MySQL, Heroku |
| 2024 | 모듈 분리, SQLite, Docker 전환 |
| 2026.04 | PostgreSQL 전환, 스키마 소문자 정규화, AI 요약 |
| 2026.05 | Enricher(태그/섹터) 통합, 인프라 고도화 (WARP 프록시 등) |

전체 변천사 → [docs/changelog.md](docs/changelog.md)  
설계 결정 배경 → [docs/architecture.md](docs/architecture.md)  
PostgreSQL 스키마 규약 → [sql/TB_SEC_REPORTS.sql](sql/TB_SEC_REPORTS.sql)
LLM 유지보수 통제 전략 → [docs/LLM_CONTROL_HARNESS.md](docs/LLM_CONTROL_HARNESS.md)  
LLM 운영 하네스 한 장 요약 → [docs/LLM_HARNESS_README.md](docs/LLM_HARNESS_README.md)
하위 LLM 단계적 작업 프로토콜 → [docs/LLM_DELEGATION_PROTOCOL.md](docs/LLM_DELEGATION_PROTOCOL.md)
LLM tmux 자동 송신 가이드 → [docs/LLM_DISPATCH_AUTOMATION.md](docs/LLM_DISPATCH_AUTOMATION.md)
LLM 하네스 포팅 가이드 → [docs/LLM_HARNESS_PORTING_GUIDE.md](docs/LLM_HARNESS_PORTING_GUIDE.md)
OCI 운영 로그 조회 헬퍼 → [docs/OPS_LOG_TAIL.md](docs/OPS_LOG_TAIL.md)
뉴스 워크플로 책임 분리 감사 (Actions 비용 분석) → [docs/NEWS_WORKFLOW_SPLIT_AUDIT.md](docs/NEWS_WORKFLOW_SPLIT_AUDIT.md)

---

## 실행

```bash
# Docker (운영)
python3 ~/secrets/generate_env.py "$PWD"
docker compose pull && docker compose up -d

# 로컬
uv sync && cp .env.example .env
uv run scraper.py
```

`.env`는 수동 편집하지 말고, 항상 `python3 ~/secrets/generate_env.py "$PWD"` 또는 `make env`로 재생성합니다.

운영 롤백은 PostgreSQL 백업/복구, 이전 컨테이너 이미지, 또는 배포 롤백으로 처리합니다. SQLite는 현재 이원화 쓰기를 하지 않으므로 최신 데이터 복구 수단으로 보지 않습니다.

---

*본 프로젝트는 개인 투자 정보 확인 목적으로 제작되었습니다. 리서치 자료의 저작권은 각 증권사에 있으며 상업적 이용을 금지합니다.*
