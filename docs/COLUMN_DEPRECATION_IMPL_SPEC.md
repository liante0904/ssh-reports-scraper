# Column Deprecation — agy 구현 스펙

> **작성일**: 2026-07-03
> **사전분석**: `docs/COLUMN_DEPRECATION_SCRAPER_STATUS.md`
> **순서**: Phase 1 → Phase 2 → 테스트 → codex 리뷰

---

## Phase 1 — ssh_library INSERT 파이프라인 수정 (영향도: 전체)

**파일**: `~/workspace/lib/ssh_library/ssh_library/reports.py`

### 1-1. `_KNOWN_COLUMNS` 주석 업데이트 (line 22-31)

```python
_KNOWN_COLUMNS = (
    "firm_id", "board_id", "firm_nm",
    "article_title", "article_url",         # article_url: DEPRECATED (드랍 예정)
    "download_url", "save_time", "reg_dt",  # save_time, reg_dt: DEPRECATED → save_at, report_date
    "writer", "key", "telegram_url", "mkt_tp", "gemini_summary",  # key: DEPRECATED → report_unique_key
    "summary_time", "summary_model",
    "retry_count", "pdf_url", "pdf_sync_status",
    "report_unique_key", "save_at", "report_date", "telegram_sent",  # ← 신규 컬럼
    "tags", "stock_names", "stock_tickers", "sector",
    "fnguide_summary_id", "target_price", "rating", "revision_type", "report_type",
)
```

### 1-2. `insert_json_data_list()` — INSERT 컬럼 + 레코드 매핑 (line 135-174)

**변경 전** (현재):
```python
records = [
    (
        entry.get("firm_id"),
        entry.get("board_id"),
        entry.get("firm_nm"),
        entry.get("reg_dt", ""),                                          # ← 옛 키
        entry.get("article_title"),
        entry.get("article_url"),                                         # ← 드랍 예정
        entry.get("download_url"),
        entry.get("telegram_url"),
        entry.get("pdf_url") or entry.get("telegram_url"),
        entry.get("writer", ""),
        entry.get("mkt_tp", "KR"),
        entry.get("report_unique_key") or entry.get("key") or entry.get("article_url", ""),
        entry.get("save_time"),                                           # ← 옛 키
    )
    for entry in json_data_list
]

sql = f'''
    INSERT INTO {table_name} (
        firm_id, board_id, firm_nm, reg_dt,
        article_title, article_url, download_url,
        telegram_url, pdf_url, writer, mkt_tp, key, save_time
    ) VALUES %s
    ON CONFLICT (report_unique_key) DO UPDATE SET
        firm_id             = EXCLUDED.firm_id,
        firm_nm             = EXCLUDED.firm_nm,
        article_title       = EXCLUDED.article_title,
        reg_dt              = EXCLUDED.reg_dt,
        writer              = EXCLUDED.writer,
        mkt_tp              = EXCLUDED.mkt_tp,
        download_url        = COALESCE(NULLIF(EXCLUDED.download_url,''),  {table_name}.download_url),
        telegram_url        = COALESCE(NULLIF(EXCLUDED.telegram_url,''), {table_name}.telegram_url),
        pdf_url             = COALESCE(NULLIF(EXCLUDED.pdf_url,''),       {table_name}.pdf_url)
    RETURNING report_unique_key, (xmax = 0) AS inserted
'''
```

**변경 후**:
```python
records = [
    (
        entry.get("firm_id"),
        entry.get("board_id"),
        entry.get("firm_nm"),
        entry.get("report_date") or entry.get("reg_dt", ""),              # ← 신규 우선, 옛 폴백
        entry.get("article_title"),
        entry.get("download_url"),
        entry.get("telegram_url"),
        entry.get("pdf_url") or entry.get("telegram_url"),
        entry.get("writer", ""),
        entry.get("mkt_tp", "KR"),
        entry.get("report_unique_key") or entry.get("key") or entry.get("article_url", ""),  # ← 신규 우선
        entry.get("save_at") or entry.get("save_time"),                   # ← 신규 우선, 옛 폴백
    )
    for entry in json_data_list
]

sql = f'''
    INSERT INTO {table_name} (
        firm_id, board_id, firm_nm, report_date,
        article_title, download_url,
        telegram_url, pdf_url, writer, mkt_tp, report_unique_key, save_at
    ) VALUES %s
    ON CONFLICT (report_unique_key) DO UPDATE SET
        firm_id             = EXCLUDED.firm_id,
        firm_nm             = EXCLUDED.firm_nm,
        article_title       = EXCLUDED.article_title,
        report_date         = EXCLUDED.report_date,
        writer              = EXCLUDED.writer,
        mkt_tp              = EXCLUDED.mkt_tp,
        download_url        = COALESCE(NULLIF(EXCLUDED.download_url,''),  {table_name}.download_url),
        telegram_url        = COALESCE(NULLIF(EXCLUDED.telegram_url,''), {table_name}.telegram_url),
        pdf_url             = COALESCE(NULLIF(EXCLUDED.pdf_url,''),       {table_name}.pdf_url)
    RETURNING report_unique_key, (xmax = 0) AS inserted
'''
```

**핵심 변경**:
| 항목 | 전 | 후 |
|------|----|----|
| INSERT 컬럼 | `reg_dt`, `article_url`, `key`, `save_time` | `report_date`, `download_url`만, `report_unique_key`, `save_at` |
| 레코드 매핑 | `entry.get("reg_dt","")` | `entry.get("report_date") or entry.get("reg_dt","")` |
| 레코드 매핑 | `entry.get("save_time")` | `entry.get("save_at") or entry.get("save_time")` |
| DO UPDATE SET | `reg_dt = EXCLUDED.reg_dt` | `report_date = EXCLUDED.report_date` |
| `article_url` | INSERT 컬럼에 포함 | 제거 (DO UPDATE에선 이미 없었음) |

### 1-3. SELECT 쿼리 컬럼명 업데이트 (line 212-315)

`save_time` → `save_at`, `reg_dt` → `report_date` 로 변경. 상세는 파일 내 grep 후 일괄 적용.

**변경이 필요한 라인** (grep 기준):
- Line 214: `SELECT ... reg_dt, ... save_time` → `report_date, save_at`
- Line 220: `ORDER BY ... save_time` → `save_at`
- Line 228-230: 동일 패턴
- Line 239: `WHERE ... save_time >= %s` → 하위호환 유지 or `save_at` 전환
- Line 241: `ORDER BY reg_dt DESC, save_time DESC` → `report_date DESC, save_at DESC`
- Line 247-255: 동일 패턴
- Line 303-313: `save_time` 조건 → `save_at`
- Line 390, 402: `ORDER BY save_time DESC` → `save_at DESC`
- Line 418: `AND save_time >= %s` → `save_at`
- Line 427: `DATE(save_time)` → `DATE(save_at)`
- Line 503: `r.save_time` → `r.save_at`

---

## Phase 2 — Scraper 모듈 dict 키 이관

Phase 1의 fallback (`entry.get("save_at") or entry.get("save_time")`) 덕분에 **순차적 이관 가능**. 한 번에 다 안 바꿔도 운영 영향 없음.

### 2-1. `save_time` → `save_at` (15개 파일, ~20곳)

| 파일 | 라인 |
|------|------|
| `modules/LS_0.py` | 189, 566 |
| `modules/Shinyoung_7.py` | 73 |
| `modules/DS_11.py` | 88, 90, 102 |
| `modules/eugenefn_12.py` | 103 |
| `scrapers/ds_core.py` | 37 |
| `scrapers/sks_core.py` | 73 |
| `scrapers/daol_core.py` | 47 |
| `scrapers/kyobo_core.py` | 62 |
| `scrapers/hanyang_core.py` | 46 |
| `scrapers/samsung_core.py` | 36 |
| `scrapers/hanwha_core.py` | 56 |
| `scrapers/nhqv_core.py` | 74 |
| `scrapers/hana_core.py` | 84 |
| `scrapers/shinhan_core.py` | 66 |
| `scrapers/news_core.py` | 제거됨 (뉴스 수집은 naver-stock-news 담당) |

**예시** (LS_0.py:189):
```python
# 전
"save_time": datetime.now().isoformat()
# 후
"save_at": datetime.now().isoformat()
```

### 2-2. `reg_dt` → `report_date` (10개 파일, ~30곳)

| 파일 | 라인 |
|------|------|
| `modules/BNKfn_23.py` | 115, 121 |
| `modules/eugenefn_12.py` | 81, 95 |
| `modules/LS_0.py` | 181, 521, 551, 588, 603, 612, 646 |
| `modules/Shinyoung_7.py` | 55, 68 |
| `modules/DS_11.py` | 83, 101 |
| `scrapers/ds_core.py` | 28, 34 |
| `scrapers/sks_core.py` | 33-37, 63-64, 71 |
| `scrapers/daol_core.py` | 45 |
| `scrapers/kyobo_core.py` | 60 |
| `scrapers/validate.py` | 17-19, 23 |

**주의**: `LS_0.py`의 `reg_dt`는 dict 키뿐 아니라 내부 로직 변수명으로도 광범위하게 사용됨 (line 521, 551, 588, 603, 612, 646). dict 키만 `report_date`로 바꾸고 내부 변수명은 그대로 유지하거나, 별도 PR로 분리.

### 2-3. `key` 병행 설정 중단 (3개 파일)

`key`와 `report_unique_key`를 동시에 같은 값으로 설정 중인 모듈. `key` 설정만 제거:

| 파일 | 현재 코드 |
|------|-----------|
| `scrapers/hanyang_core.py:46` | `key=dl,report_unique_key=dl` → `report_unique_key=dl` |
| `scrapers/nhqv_core.py:74` | `key=u,report_unique_key=u` → `report_unique_key=u` |
| `scrapers/hana_core.py:84` | `key=dl,report_unique_key=dl` → `report_unique_key=dl` |

---

## 검증 체크리스트

- [ ] Phase 1 적용 후 `uv run pytest tests/ -v` 통과
- [ ] Phase 2 파일별 dict 키 변경 후 scraper 정상 동작
- [ ] `insert_json_data_list` 로그에서 신규 컬럼명으로 INSERT 확인
- [ ] `v_sec_reports_full` 뷰로 프론트 정상 조회
- [ ] 1주일 이상 `missing_save_at=0`, `missing_report_date=0` 확인
