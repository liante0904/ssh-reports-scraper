# SSH Reports Scraper — 아키텍처 & 설계 문서

> **최종 갱신**: 2026-06-22
> **목표**: 증권사 리서치 레포트 스크래핑 → GA standalone + 서버 dual-mode
> **현황**: GA 24개사, 서버전용 7개사(하나증권 IP차단 포함)

---

> **디버깅 진입점**: [DEBUG_ENTRYPOINTS.md](DEBUG_ENTRYPOINTS.md)에서 현재 파일 구조와 빠른 검증 명령을 먼저 확인한다.

## 1. 배경과 동기

### 1.1 기존 구조의 문제점

```
[OCI 서버] scheduler.py (APScheduler, 30분 간격)
  └─ scraper.py
       ├─ 24개 증권사 checkNewArticle() 호출 (동기 7 + 비동기 17)
       ├─ DB insert (ON CONFLICT dedup)
       ├─ enrich_data() (DBfi gate URL 복구)
       └─ daily_send_report() (Telegram 전송)
```

**문제**:
- **IP 차단**: OCI 서버 IP로 장기간 크롤링 → LS증권 등에서 IP 차단 (WARP 우회 필요)
- **서버 부하**: 19개 증권사 동시 크롤링 → CPU/메모리 사용량 높음 (10개 GA 이관 완료)
- **Docker 의존성**: Selenium 필요한 증권사(한국투자)는 Docker 컨테이너 필요
- **관측성 부족**: 로그가 서버 로컬에만 존재, 실행 이력 추적 어려움

### 1.2 해결 전략

**GitHub Actions로 크롤링을 이관하고, 서버는 import + enrich + send만 담당.**

| 측면 | 이전 | 이후 |
|------|------|------|
| 크롤링 실행 | OCI 서버 (고정 IP) | GitHub Actions runner (매번 새 IP) |
| IP 차단 | SOCKS5 WARP 우회 필요 | 불필요 (클린 IP) |
| DB insert | 크롤링 직후 동기 실행 | 서버에서 artifact import로 분리 |
| 실행 이력 | 서버 로컬 로그 | GitHub Actions 로그 + artifact |
| 병렬성 | asyncio.gather | GitHub Actions 자체 isolation |

---

## 2. 아키텍처

### 2.1 전체 데이터 흐름

```
┌─────────────────────────────────────────────────────────────┐
│                    GitHub Actions                            │
│                                                              │
│  scrape-ls.yml          scrape-all.yml                      │
│  (LS 전용, :00/:30)     (전체 증권사, :05/:35)              │
│       │                       │                              │
│       ▼                       ▼                              │
│  standalone_ls_scraper   standalone_all_scraper              │
│       │                       │                              │
│       ▼                       ▼                              │
│  JSON artifact            JSON artifact                     │
│  (ls-scraped-data)        (all-scraped-data)                │
│       │                       │                              │
└───────┼───────────────────────┼──────────────────────────────┘
        │                       │
        ▼                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    OCI 서버 (scheduler.py)                   │
│                                                              │
│  import_ls (:05, :35)     import_all (:10, :40)             │
│       │                       │                              │
│       ▼                       ▼                              │
│  import_ls_artifact.py    import_all_artifact.py             │
│       │                       │                              │
│       └───────┬───────────────┘                              │
│               ▼                                              │
│          PostgreSQL                                          │
│          (INSERT ON CONFLICT dedup)                          │
│               │                                              │
│               ▼                                              │
│  scraper.py (30분 간격)                                     │
│  ├─ enrich_data() — DBfi gate URL 복구                      │
│  └─ daily_send_report() — Telegram 전송                    │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 타이밍 다이어그램

```
KST 시간    GitHub Actions                서버 scheduler
────────    ──────────────                ──────────────
08:00       scrape-ls.yml 실행 시작
08:02         standalone_ls_scraper 완료
08:03         artifact upload 완료
08:05                                     import_ls_artifact 실행
08:05       scrape-all.yml 실행 시작
08:07         standalone_all_scraper 완료
08:08         artifact upload 완료
08:10                                     import_all_artifact 실행
08:15                                     scraper.py (enrich+send)
08:30       scrape-ls.yml 실행 시작
...                                      ...
```

**핵심 설계 원칙**:
- LS와 전체 스크래퍼의 cron을 **5분 staggered**로 배치 (GitHub Actions 동시 실행 부하 방지)
- 각 import job은 대응되는 scrape cron의 **5분 후**에 실행 (artifact 업로드 완료 대기)
- import job은 성공한 최신 workflow run의 artifact만 가져오므로, 중복 import되지 않음

---

## 3. 파일별 상세

### 3.1 GitHub Actions Workflows

#### `.github/workflows/scrape-ls.yml` — LS증권 전용

```yaml
on:
  schedule:
    - cron: '0,30 23 * * 0-4'     # KST 월~금 08:00~08:30
    - cron: '0,30 0-8 * * 1-5'    # KST 화~토 09:00~17:30
    - cron: '0 9 * * 1-5'         # KST 화~토 18:00
  workflow_dispatch:
```

- `standalone_ls_scraper.py` 실행 → JSON artifact (`ls-scraped-data`)
- LS증권은 `LS_detail()`로 상세 페이지까지 크롤링해야 하므로 별도 workflow 유지
- **이유**: LS는 msg URL 복구(reconstruct_msg_url_from_db)에 DB 접근이 필요 → 서버에서 import 시 후처리

#### `.github/workflows/scrape-all.yml` — 나머지 24개 증권사

```yaml
on:
  schedule:
    - cron: '5,35 23 * * 0-4'     # KST 월~금 08:05~08:35
    - cron: '5,35 0-8 * * 1-5'    # KST 화~토 09:05~17:35
    - cron: '5 9 * * 1-5'         # KST 화~토 18:05
  workflow_dispatch:
    inputs:
      firms:       # 특정 증권사만 (쉼표 구분, 빈칸=전체)
      timeout:     # 기본 120초
```

- `standalone_all_scraper.py` 실행 → JSON artifact (`all-scraped-data`)
- `DB_BACKEND=static`으로 DB write 없이 실행 (GitHub Actions runner는 PostgreSQL 접근 불가)
- `SOCKS_PROXY_URL=''` — GitHub Actions 클린 IP이므로 프록시 불필요
- `retention-days: 1` — artifact 1일 보관 (서버 import가 완료되면 불필요)
- `overwrite: true` — 동일 이름 artifact 덮어쓰기

### 3.2 Standalone Scrapers

#### `scripts/standalone_all_scraper.py`

**목적**: 기존 `scraper.py`의 `checkNewArticle()` 함수를 그대로 재사용하여 모든 증권사를 GitHub Actions에서 실행.

```python
IMPORT_MAP = {
    "LS_0":               ("modules.LS_0",               "LS_checkNewArticle",               False),
    "ShinHanInvest_1":    ("modules.ShinHanInvest_1",    "ShinHanInvest_checkNewArticle",     True),
    "NHQV_2":             ("modules.NHQV_2",             "NHQV_checkNewArticle",              True),
    ...
    "Yuanta_27":          ("modules.Yuanta_27",          "Yuanta_checkNewArticle",            True),
}
```

**실행 흐름**:
1. `IMPORT_MAP`에서 대상 증권사 결정 (`--firms` 또는 전체, LS는 `SKIP_FIRMS`로 기본 제외)
2. 각 모듈의 `checkNewArticle` 함수를 `importlib.import_module`로 동적 로딩
3. **Phase 1: 동기 스크래퍼 (7개)** — 순차 실행 (requests 기반, 블로킹)
4. **Phase 2: 비동기 스크래퍼 (17개)** — `asyncio.gather(*tasks)` 동시 실행
5. 결과 JSON을 stdout으로 출력 → GitHub Actions가 artifact로 업로드

**출력 JSON 구조**:
```json
{
  "scraped_at": "2026-05-31T08:05:00",
  "source": "github-actions",
  "total_firms": 24,
  "success_firms": 22,
  "failed_firms": 2,
  "total_articles": 7500,
  "firms": [
    {
      "name": "HANA_3",
      "status": "success",
      "count": 510,
      "elapsed_sec": 3.5,
      "articles": [{ "firm_id": 3, "article_title": "...", ... }]
    },
    {
      "name": "DBfi_19",
      "status": "error",
      "error": "Connection timeout",
      "articles": []
    }
  ]
}
```

**제외된 증권사** (GA standalone으로 이관):
| 키 | 사유 |
|----|------|
| `Samsung_5` | GA standalone (scrape-samsung.yml) |
| `Sangsanginib_6` | GA standalone (scrape-sangsanginib.yml) |
| `Kiwoom_10` | GA standalone (scrape-kiwoom.yml) |
| `TOSSinvest_15` | GA standalone (scrape-toss.yml) |
| `Hanwhawm_21` | GA standalone (scrape-hanwha.yml) |
| `Hygood_22` | GA standalone (scrape-hanyang.yml) |
| `Heungkuk_28` | GA standalone (scrape-heungkuk.yml) |
| `KBsec_4` | GA standalone (scrape-kb.yml) |
| `NHQV_2` | GA standalone (scrape-nhqv.yml) |
| `Leading_16` | GA standalone (scrape-leading.yml) |
| `LS_0` | 별도 `scrape-ls.yml`에서 처리 (상세 페이지 크롤링 필요) |
| `eugenefn_12` | 세션 만료 이슈 (보류) |
| `Koreainvestment_13` | Selenium 필요 (GitHub Actions에서 Chrome 설치 부담) |
| `iMfnsec_18` | 보류 |

#### `scripts/standalone_ls_scraper.py`

- LS증권 전용. 목록 2페이지 + 각 게시글 상세 페이지(msg URL 추출)까지 수행
- 10개 게시판 × 2페이지 × 20건 = 최대 400건
- WARP 프록시 불필요 (GitHub Actions 클린 IP)

### 3.3 Import Scripts

#### `scripts/import_ls_artifact.py`

```bash
uv run python scripts/import_ls_artifact.py [--repo ...] [--json-file ...]
```

1. `gh run list --workflow scrape-ls.yml --status success` → 최신 run ID
2. `gh run download` → `ls-scraped-data` artifact 다운로드
3. DB에 이미 존재하는 key는 건너뛰고 신규만 `insert_json_data_list()` (ON CONFLICT dedup)
4. `telegram_url`이 없는 LS 레포트에 대해 `LS_detail()`로 msg URL 복구 후처리

#### `scripts/import_all_artifact.py`

```bash
uv run python scripts/import_all_artifact.py [--repo ...] [--dry-run] [--json-file ...]
```

1. `gh run list --workflow scrape-all.yml --status success` → 최신 run ID
2. `gh run download` → `all-scraped-data` artifact 다운로드
3. 증권사별 `db.insert_json_data_list(articles)` (ON CONFLICT dedup)
4. **후처리**: DBfi 증권사(firm_id=19)의 `telegram_url`이 비어있는 건들에 대해 `DBfi_detail()`로 gate URL 복구
5. `--dry-run` 플래그로 DB insert 없이 로직 검증 가능
6. `--json-file`로 GitHub Actions 없이 로컬 JSON 파일 import 가능
7. `--skip-post-process`로 후처리 생략 가능

### 3.4 서버 운영 파일

#### `scraper.py` (전환 후)

**기본 모드** (`EMERGENCY_SCRAPE` 미설정):
```
main()
  ├─ [스크래핑 건너뜀 — GitHub Actions가 담당]
  ├─ enrich_data()       # DBfi gate URL 복구
  └─ daily_send_report() # Telegram 전송
```

**비상 모드** (`EMERGENCY_SCRAPE=1`):
```
main()
  ├─ _emergency_scrape()  # 서버 직접 크롤링 → DB insert
  ├─ enrich_data()
  └─ daily_send_report()
```

- `_EMERGENCY` 플래그로 import 자체를 조건부 처리 (미사용 모듈 로딩 방지)
- `enrich_data()`는 `DBfi_detail`만 여전히 import (기본 모드에서 필요)
- LS, DS 증권사는 enrich_data()에서 `pass` 처리 (각각 GitHub Actions/LS import에서 처리)

#### `scheduler.py` (전환 후)

**등록된 Job**:
```
1. main_scraper_job        */30 * * * * (jitter=300)   → scraper.py (enrich+send)
2. import_ls_artifact_job  5,35 * * * * (jitter=60)    → import_ls_artifact.py
3. import_all_artifact_job 10,40 * * * * (jitter=60)   → import_all_artifact.py
```

- `run_import_ls()` / `run_import_all()`: subprocess로 import 스크립트 호출, stdout 요약만 로깅
- timeout: LS 300초, ALL 600초

### 3.5 Emergency Fallback

#### `scripts/emergency_scrape_all.sh`

```bash
# 전체 증권사 비상 스크래핑
bash scripts/emergency_scrape_all.sh

# 특정 증권사만
EMERGENCY_FIRMS="HANA_3,KBsec_4" bash scripts/emergency_scrape_all.sh
```

1. `standalone_all_scraper.py` 실행 → JSON 파일로 저장
2. `import_all_artifact.py --json-file`로 DB import

**또는** `EMERGENCY_SCRAPE=1 uv run scraper.py` 로 scheduler를 통해 기존 방식으로 폴백.

---

## 4. 검증 결과

### 4.1 서버 vs GitHub Actions 비교

2026-05-31 서버 로그와 standalone scraper의 HANA_3 결과를 비교 검증:

| 항목 | 서버 scraper.py | standalone_all_scraper.py |
|------|-----------------|---------------------------|
| HANA_3 크롤링 건수 | 510건 | 510건 |
| 실행 시간 | ~3.5s | ~3.5s |
| 아티클 필드 구조 | 동일 | 동일 |
| `checkNewArticle` 함수 | `modules.HANA_3` | `modules.HANA_3` (동일 모듈) |

### 4.2 import dry-run 검증

```bash
uv run python scripts/import_all_artifact.py \
  --json-file /tmp/mock_all_artifact.json \
  --dry-run --skip-post-process
```

```
[HANA_3] DRY-RUN: would insert/update 2 articles
[KBsec_4] DRY-RUN: would insert/update 1 articles
[BNKfn_23] DRY-RUN: would insert/update 2 articles
Import complete: 0 inserted, 0 updated, 0 errors
=== All-Firms Import Complete ===
```

### 4.3 서버 DB 현황 (2026-05-31)

- 전체 증권사 29개, 총 약 26만건의 레코드
- 오늘 신규 insert: 2건 (하나증권 1, 미래에셋 1)
- 나머지 7,423건은 ON CONFLICT UPDATE
- DBfi(19), BNK(23) 두 증권사는 서버에서도 이미 0건 반환 (소스 사이트 IP 차단)

### 4.4 `gh` CLI 서버 인증 상태

```
✓ Logged in to github.com account liante0904
- Token scopes: 'admin:public_key', 'gist', 'read:org', 'repo'
```

artifact 다운로드에 필요한 `repo` 스코프 보유.

---

## 5. 운영 절차

### 5.1 초기 배포

```bash
# 1. GitHub PR merge
git push origin feat/gh-actions-all-scrapers
# → GitHub에서 PR 생성 → main에 merge

# 2. 서버에 반영
ssh oci
cd ~/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper
git pull origin main

# 3. scheduler 재시작
pkill -f "scheduler.py"
uv run scheduler.py &

# 4. GitHub Actions 수동 실행 테스트
# GitHub → Actions → "All-Firms Scraper" → Run workflow (firms=HANA_3)
# → artifact 확인 → 서버에서 import 테스트
uv run python scripts/import_all_artifact.py --dry-run
uv run python scripts/import_all_artifact.py
```

### 5.2 일상 운영

- GitHub Actions가 cron에 따라 자동 실행
- 서버 `scheduler.py`가 자동으로 artifact import + enrich + send
- GitHub Actions 로그에서 각 증권사별 상태 확인 가능
- `SCRAPER_HEALTH_ERRORS` 발생 시 Telegram 알람 (기존 메커니즘 유지)

### 5.3 비상 대응

**상황 1: GitHub Actions 전체 장애**
```bash
ssh oci
cd ~/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper
bash scripts/emergency_scrape_all.sh
```

**상황 2: 특정 증권사만 GitHub Actions에서 실패**
```bash
EMERGENCY_FIRMS="DBfi_19,BNKfn_23" bash scripts/emergency_scrape_all.sh
```

**상황 3: scheduler 자체를 비상 모드로 전환**
```bash
EMERGENCY_SCRAPE=1 uv run scheduler.py &
```

### 5.4 모니터링

- **GitHub Actions 탭**: 각 workflow 실행 이력, 성공/실패, 소요 시간
- **Artifact**: 가장 최근 성공한 run의 JSON 데이터 (1일 보관)
- **서버 로그**: `~/logs/YYYYMMDD/YYYYMMDD_scheduler.log`
- **DB**: `tbl_sec_reports` 테이블의 `save_at` 컬럼으로 최신 import 시간 확인

---

## 6. URL 컬럼과 pdf-archiver

`tbl_sec_reports`의 4개 URL 컬럼은 각자 목적이 다르다:

| 컬럼 | 용도 | 비어있는 건수 | 관리 주체 |
|------|------|:---:|------|
| `article_url` | 증권사 원문 페이지 (nullable) | 72% | scraper |
| `download_url` | PDF 다운로드용 URL | 13% | scraper → pdf-archiver |
| `telegram_url` | 텔레그램 전송용 URL | 0.4% | scraper → Telegram |
| `pdf_url` | PDF 원본 URL | 0.4% | scraper |

**pdf-archiver** (`~/workspace/services/pdf-archiver/`)가 `download_url`에서 실제 PDF를 다운로드 → 해시 검증 → 클라우드 업로드 → `tbl_sec_reports_pdf_archive` + `pdf_sync_status` 업데이트.

DB증권(19)은 텔레그램용 URL과 다운로드용 URL이 다르다 (gate viewer vs StreamDocs). 그래서 `telegram_url ≠ download_url`인 케이스가 존재.

### 컴포넌트별 컬럼 책임

| 컴포넌트 | READ | WRITE (tbl_sec_reports) | WRITE (다른 테이블) |
|------|------|------|------|
| **scraper** (29개 모듈) | - | `report_id`, `firm_id`, `board_id`, `firm_nm`, `article_title`, `article_url`, `download_url`, `telegram_url`, `pdf_url`, `report_date`, `writer`, `mkt_tp`, `report_unique_key`, `save_at`, `telegram_sent` | - |
| **enricher** (tag_extractor) | `article_title`, `firm_nm` | `tags`, `stock_names`, `stock_tickers`, `sector`, `gemini_summary`, `summary_time`, `summary_model`, `target_price`, `rating`, `revision_type`, `report_type`, `fnguide_summary_id`, `sync_status` | `tbl_report_enricher_tags`, `tbl_report_ai_summaries`, `tbl_report_price_targets` |
| **pdf-archiver** | `download_url` | `pdf_sync_status`, `download_status_yn`, `pdf_hash` | `tbl_sec_reports_pdf_archive` (26컬럼 전체) |
| **scraper.py enrich_data()** | `telegram_url`, `report_unique_key`, `writer`, `report_date` | `telegram_url` (DBfi gate/LS msg URL 복구) | - |
| **scheduler.py** | `telegram_sent`, `telegram_url`, `article_title` | `telegram_sent=true` (발송 완료) | - |

---

## 7. 데이터베이스 연동 및 스키마 구조 (2026-07-01 업데이트)

### 7.1 주요 데이터 구조 변경사항
* **`firm_id` 도입**: 기존의 `sec_firm_order` 물리적 컬럼명이 `firm_id`로 전면 개편되었습니다. (19개 스크래퍼 모듈 및 백엔드 맵핑 완료)
* **`board_id` 도입**: 기존의 `article_board_order` 물리적 컬럼명이 `board_id`로 전면 개편되었습니다. (9개 스크래퍼 모듈 및 백엔드 맵핑 완료)
* **`save_at` 마이그레이션**: 기존 `save_time` (VARCHAR ISO 포맷) 대신 표준 시간대를 지원하는 `save_at` (TIMESTAMPTZ) 컬럼이 추가되었으며, 기존 `save_time` 컬럼은 `# deprecated` 처리되었습니다.
* **`report_unique_key` 전환**: 기존 `key` 컬럼은 사용이 중단(deprecated)되었으며, 모든 기사 식별은 `report_unique_key` 로 단일 통일되었습니다.

### 7.2 DDL 파일 목록 및 역할
데이터베이스 변경사항을 정의하는 DDL 스크립트는 `sql/` 디렉토리 내에 보관되어 관리됩니다:
- `sql/TB_SEC_REPORTS.sql`: 메인 리포트 테이블 (`tbl_sec_reports`) DDL 구조 정의
- `sql/TB_SEC_FIRM_INFO.sql`: 증권사 메타데이터 테이블 (`tbm_sec_firm_info`) DDL 구조 정의
- `sql/TB_SEC_FIRM_BOARD_INFO.sql`: 증권사별 게시판 메타데이터 테이블 (`tbm_sec_firm_board_info`) DDL 구조 정의
- `sql/VIEWS.sql`: 레거시 호환을 위한 `v_sec_reports_full` 등 종합 뷰(View) 정의

### 7.3 `tbl_sec_reports` — upsert 로직 (report_unique_key 기반)
```sql
INSERT INTO tbl_sec_reports (
    firm_id, board_id, firm_nm, report_date,
    article_title, article_url, telegram_sent, download_url,
    telegram_url, pdf_url, writer, mkt_tp, report_unique_key, save_at
) VALUES %s
ON CONFLICT (report_unique_key) DO UPDATE SET
    firm_id             = EXCLUDED.firm_id,
    firm_nm             = EXCLUDED.firm_nm,
    article_title       = EXCLUDED.article_title,
    report_date         = EXCLUDED.report_date,
    writer              = EXCLUDED.writer,
    mkt_tp              = EXCLUDED.mkt_tp,
    download_url        = COALESCE(NULLIF(EXCLUDED.download_url, ''), tbl_sec_reports.download_url),
    telegram_url        = COALESCE(NULLIF(EXCLUDED.telegram_url, ''), tbl_sec_reports.telegram_url),
    pdf_url             = COALESCE(NULLIF(EXCLUDED.pdf_url, ''),       tbl_sec_reports.pdf_url)
RETURNING report_unique_key, (xmax = 0) AS inserted
```

---

## 8. GA Standalone 전환 & 운영 현황 (GA_STATUS 통합)

* **정상 작동 (24개사)**: NH투자, KB, 삼성, 상상인, 미래에셋, 현대차, 키움, 다올, 토스, 리딩, DB, 메리츠, 한화, 한양, 교보, IBK, SK, 유안타, 흥국 등
* **서버 전용 수집 (5개사)**: LS, 신한, DS, 유진, 대신 (보안 설정 및 스파이더 방어로 인해 개발 서버에서 전용 수집 수행)
* **네트워크 차단 (2개사)**: 
  - `BNKfn_23`: 소스 사이트의 외부 IP 차단
  - `하나증권(HANA_3)`: GA 러너 IP 차단으로 인해 서버 직접 스크래핑으로 전환
* **인증 만료**: `iM증권(iMfnsec_18)` secure key 갱신 대기 중

---

## 9. 배포 체크리스트 & 안정성 대책 (DEPLOY_CHECKLIST 통합)

### 9.1 배포 전 체크리스트
- [ ] CI 통과 확인 (GitHub Actions workflow green)
- [ ] pre-push 훅 통과 (`scripts/verify_dockerfile.sh`, `scripts/verify_standalones.sh` 실행 완료)
- [ ] `report_unique_key` ON CONFLICT 정상 동작 확인
- [ ] `key` 컬럼 쓰기 차단 코드 배포 상태 확인

### 9.2 장애 방지 및 예외 대책
1. **Dockerfile COPY 누락 방지**: `verify_dockerfile.sh` 스크립트를 통해 신규 디렉토리 배포 누락 사전 차단.
2. **스크래퍼 구동 안정성**: `verify_standalones.sh` pre-push 훅을 통해 로컬 syntax 오류 사전 검증.
3. **듀얼 모드 백업**: KST 1시, 7시, 13시, 21시에 서버에서 직접 FULL-SCRAPE를 돌려 GA 다운 타임 백업 지원.
4. **IP 블록 대책**: 외부 차단된 BNK/LS의 경우 무리한 파서 갱신 대신 전용 로컬 서버 IP 및 프록시 처리를 권장함.

---

## 10. 향후 개선 사항
1. **한국투자증권 Selenium**: GitHub Actions에 Chrome 설치 step 추가 → `Koreainvestment_13` 활성화
2. **Matrix build**: 24개 증권사를 각각 독립 job으로 분할하여 빌드/수집 병렬성 극대화 (최대 20 parallel jobs)
3. **실패 자동 웹훅**: GitHub Actions 실패 시 Telegram/Discord 웹훅 경보 연동
4. **Retention 정책**: artifact 보관 기간을 1일 → 3일로 조정하여 장애 복구 용이성 확보
