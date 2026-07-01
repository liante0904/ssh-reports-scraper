# Refactoring Priorities (LLM-Friendly Codebase)

> 자동 생성: 2026-07-01  
> 기준: LLM이 코드 읽을 때 헷갈리거나, 컨텍스트 낭비를 유발하는 패턴

## P0 — 당장 수정 (PR 1회)

| # | 파일 | 문제 | 수정 |
|---|---|---|---|
| 1 | `run/standalone/_TEMPLATE.py:22-23` | 예제 dict에 `sec_firm_order`, `article_board_order` 그대로 | `firm_id`, `board_id`로 교체 |
| 2 | `run/colab/bnk_scraper.py:80-81` | 동일 | 동일 |
| 3 | `scripts/standalone_bnk_scraper.py:168` | 동일 | 동일 |
| 4 | `scripts/standalone_ls_scraper.py:179` | `"key": key` → 아직 `key` 씀 | `"report_unique_key": key` |

## P1 — 컬럼명 일관성

| # | 현재 | 문제 | 제안 |
|---|---|---|---|
| 5 | `gemini_summary` | 모델 여러 개(DeepSeek, Gemini) 쓰는데 컬럼명 고정 | `ai_summary` 또는 `llm_summary` |
| 6 | `firm_nm` | `nm` = name 축약 | 컬럼 rename까지는 무리, 주석 추가 |
| 7 | `mkt_tp` | `tp` = type 축약 | `market_type` |
| 8 | `reg_dt` | `dt` = date 축약, 타입은 text | `report_date`가 이미 있음 → 통합 |
| 9 | `tbl_` vs `tbm_` | `tbl_sec_reports` vs `tbm_sec_firm_info` | prefix 불일치 (마스터는 `tbm`, 트랜잭션은 `tbl`) |

## P2 — 중복/레거시 정리

| # | 파일 | 문제 |
|---|---|---|
| 10 | `tests/fnguide.py` | 1000줄 레거시 테스트. `sec_firm_order` 50회 이상 하드코딩 |
| 11 | `tests/MariaDB.py`, `MariaDB_bak.py` | 미사용 백업 파일 |
| 12 | `run/fix_*.py` (10개 파일) | 일회성 fix 스크립트. `archive/`로 이동 |
| 13 | `run/scraper_af.py` | `sec_firm_order` 참조. 거의 미사용 |
| 14 | `scripts/import_*_artifact.py` | `sec_firm_order=0` 하드코딩 → `firm_id=0` |

## P3 — 타입/구조 개선

| # | 현재 | 문제 | 제안 |
|---|---|---|---|
| 15 | `save_time` TEXT + `save_at` TIMESTAMPTZ | 같은 값 두 번 씀 | `save_time` DROP → `save_at` 통일 |
| 16 | `main_ch_send_yn` CHAR(1) | `telegram_sent` BOOLEAN이 이미 대체 | DROP COLUMN |
| 17 | `key` 컬럼 | `report_unique_key`가 대체 | INSERT에서 제거 → DROP |
| 18 | `reg_dt` TEXT `"YYYYMMDD"` | 매번 `::date` 캐스팅 | DATE 타입으로 ALTER |
| 19 | `download_status_yn` + `pdf_sync_status` + `sync_status` | 다 비슷한 의미 | 통합 검토 |

## P4 — 모듈 구조

| # | 이슈 |
|---|---|
| 20 | `modules/` (29개) + `scrapers/` (29개 core) → 디렉토리 2개로 분산 |
| 21 | 일부 모듈은 `_checkNewArticle` 동기, 일부는 비동기 → 일관성 없음 |
| 22 | `models/db_factory.py` → `get_db()`가 SQLite/Postgres 분기 → ssh_library로 통합됨 |

---

## 오늘 이미 해결된 것들

- [x] `sec_firm_order` → `firm_id` rename (96파일)
- [x] `article_board_order` → `board_id` rename
- [x] `saved_at` → `save_at` 통일
- [x] `tbm_sec_firm_info` 뷰 (`v_sec_firm_info`) 생성
- [x] 백엔드 ORM → raw SQL 전환
- [x] `/recent` 엔드포인트 신설
- [x] `mark-all-read` 422 fix
- [x] router prefix 중복 제거
- [x] `.KQ/.KS` 필터 강화
- [x] 한화 GA fallback 활성화
- [x] 문서 21→10개 통합
