# Refactoring Priorities (LLM-Friendly Codebase)

> 갱신: 2026-07-09 | 기준: LLM이 코드 읽을 때 헷갈리거나 컨텍스트 낭비를 유발하는 패턴
> 운영 DB 확인: `public.tbl_sec_reports`는 현재 35개 물리 컬럼이며 `key`, `reg_dt`, `save_time`, `main_ch_send_yn`은 이미 없다.

## 전체 통합 테이블

| # | 영역 | 현재 | 문제 | 제안 | 난이도 | 상태 |
|---|---|---|---|---|---|---|
| 1 | DB | `sec_firm_order` | `tbl_sec_reports` 물리컬럼명 | `firm_id`로 rename + DDL | 중 | ✅ 완료 |
| 2 | DB | `article_board_order` | 24글자, 너무 김 | `board_id`로 rename + DDL | 중 | ✅ 완료 |
| 3 | DB | `saved_at` / `save_at` | 같은 컬럼 철자 불일치 | `save_at` 통일 | 하 | ✅ 완료 |
| 4 | DB | `tbm_sec_firm_info` | `sec_firm_order`인데 뷰 없음 | `v_sec_firm_info` 생성 (`firm_id` alias) | 하 | ✅ 완료 |
| 5 | 백엔드 | ORM `db.query().filter()` | SQL 직접 안 보여서 디버깅 2배 | raw SQL (`_execute_raw_psycopg2_query`) | 중 | ✅ 완료 |
| 6 | 백엔드 | `/external/api/search` | recent/global/industry 다 search로 집중 | `/recent` 전용 엔드포인트 분리 | 하 | ✅ 완료 |
| 7 | 백엔드 | `mark-all-read` 422 | `list[str]`가 int 섞인 배열 reject | `str()` 캐스팅 + `embed=True` | 하 | ✅ 완료 |
| 8 | 백엔드 | router prefix 누락 | 12개 route 중복 decorator | `prefix="/external/api"` 추가 | 하 | ✅ 완료 |
| 9 | 백엔드 | `.KQ`/`.KS` 필터 | `\(\d{5,6}\.K[QS]\)` 패턴이 `/매수` 변형 못 잡음 | `\([^)]+\.K[QS][^)]*\)` 추가 | 하 | ✅ 완료 |
| 10 | 스크래퍼 | 한화(21) GA fallback | GA standalone은 있는데 서버 fallback 없음 | `_GA_FIRMS_ASYNC`에 추가 | 하 | ✅ 완료 |
| 11 | 문서 | 21개 마크다운 | LLM_* 6개, 운영문서 중복 | 10개로 통합 | 하 | ✅ 완료 |
| 12 | 코드 | `_TEMPLATE.py` 예제 | `sec_firm_order`, `article_board_order` 그대로 | `firm_id`, `board_id`로 교체 | 하 | 🔶 보류 |
| 13 | 코드 | `run/colab/bnk_scraper.py` | 동일 | 동일 | 하 | 🔶 보류 (호환 계약) |
| 14 | 코드 | `scripts/standalone_bnk_scraper.py` | 동일 | 동일 | 하 | 🔶 보류 (호환 계약) |
| 15 | 코드 | `scripts/standalone_ls_scraper.py` | `"key": key` | `"report_unique_key": key` | 하 | 🔶 보류 (호환 계약) |
| 16 | DB | `save_time` TEXT | 운영 DB 물리 컬럼에서 이미 제거됨 | 새 작업 없음 (`save_at` 사용) | 중 | ✅ 완료 |
| 17 | DB | `main_ch_send_yn` CHAR(1) | 운영 DB 물리 컬럼에서 이미 제거됨 | 새 작업 없음 (`telegram_sent` 사용) | 하 | ✅ 완료 |
| 18 | DB | `key` 컬럼 | 운영 DB 물리 컬럼에서 이미 제거됨 | 새 작업 없음 (`report_unique_key` 사용) | 하 | ✅ 완료 |
| 19 | DB | `reg_dt` TEXT `"YYYYMMDD"` | 운영 DB 물리 컬럼에서 이미 제거됨 | 새 작업 없음 (`report_date` 사용) | 중 | ✅ 완료 |
| 20 | DB | `download_status_yn` + `pdf_sync_status` + `sync_status` | 비슷한 컬럼 3개 | 통합 검토 | 상 | 🔲 검토 |
| 21 | DB | `gemini_summary` | DeepSeek/Gemini 여러 모델 쓰는데 컬럼명 고정 | `ai_summary` 또는 `llm_summary` | 중 | 🔲 검토 |
| 22 | DB | `firm_nm` | `nm` = name 축약 (5글자) | `firm_name` 뷰 alias | 하 | ✅ 완료 |
| 23 | DB | `mkt_tp` | `tp` = type 축약 | `market_type` 뷰 alias | 하 | ✅ 완료 |
| 24 | DB | `tbl_` vs `tbm_` prefix | `tbl_sec_reports` vs `tbm_sec_firm_info` | prefix 불일치 (마스터=m, 트랜잭션=l) | 하 | 🔲 주석 |
| 25 | 코드 | `tests/fnguide.py` | 1000줄, `sec_firm_order` 50회 하드코딩 | 리팩토링 또는 archive | 하 | 🔲 검토 |
| 26 | 코드 | `tests/MariaDB.py` + `MariaDB_bak.py` | 미사용 백업 | 삭제 | 하 | ✅ 완료 |
| 27 | 코드 | `run/fix_*.py` (12개) | 일회성 fix 스크립트 | 삭제 (archive/legacy 보존) | 하 | ✅ 완료 |
| 28 | 코드 | `run/scraper_af.py` | `sec_firm_order` 참조, 거의 미사용 | 삭제 | 하 | ✅ 완료 |
| 29 | 코드 | `scripts/import_*_artifact.py` | `sec_firm_order=0` 하드코딩 | `firm_id=0` | 하 | ✅ 완료 |
| 30 | 코드 | `modules/` + `scrapers/` | 29개씩 2개 디렉토리 분산 | 통합 검토 | 중 | 🔲 검토 |
| 31 | 코드 | `checkNewArticle` 비동기/동기 혼재 | 모듈마다 `async def` / `def` 달라서 호출 패턴 다름 | 표준화 | 중 | 🔲 검토 |
| 32 | 코드 | `models/db_factory.py` | 과거 SQLite/Postgres 분기 | `SecReportsManager` 단일 반환으로 정리 | 하 | ✅ 완료 |
| 33 | 백엔드 | `BASE_SELECT_SQL` 600자 | 모든 엔드포인트가 복붙 | `v_reports_api` 뷰로 대체 | 하 | ✅ 완료 |
| 34 | 백엔드 | `_execute_raw_psycopg2_query` | 매 요청마다 raw connection 새로 생성 | 커넥션풀 사용 | 중 | 🔲 검토 |
| 35 | 백엔드 | `routers/reports.py` | 아직 ORM (`db.query()`) | raw SQL 전환 | 중 | 🔲 검토 |
| 36 | 백엔드 | `routers/admin.py` | ORM + `func.max/count` | raw SQL 전환 | 중 | 🔲 검토 |
| 37 | 백엔드 | `routers/favorites.py` | ORM | raw SQL 전환 | 하 | 🔲 검토 |
| 38 | 코드 | `ConfigManager` 싱글톤 | 환경별 테스트 어려움 | DI 패턴 | 중 | 🔲 검토 |
| 39 | 코드 | `FirmInfo.firm_names` | 첫 접근에 DB 전체 로드 (29건) | 지연 로딩 또는 캐시 | 하 | 🔲 검토 |
| 40 | CI/CD | `deploy.yml` | blue/green 수동 전환. health check 실패 시 rollback 불확실 | 자동 rollback 강화 | 중 | 🔲 검토 |
| 41 | CI/CD | `.env` + `secrets.json` 이원화 | `generate_env.py` 없으면 secrets 누락 | 통합 또는 문서화 | 중 | 🔲 검토 |
| 42 | Infra | `ssh oci "docker exec"` 매번 타이핑 | 3개 repo 3개 컨테이너 매번 수동 | `scripts/ops_ssh.sh` 통합 | 하 | 🔶 진행 중 |
| 43 | Infra | scraper/backend/frontend deploy 각각 수동 확인 | deploy 후 API smoke test 자동화 안됨 | health check 스크립트 | 중 | 🔲 검토 |
| 44 | Depth | `enrich_data()` 100줄 | 3레벨 if/for/try 중첩 | 함수 분리 | 중 | 🔲 검토 |
| 45 | Depth | `_row_to_dict()` 100줄 | field 매핑에 early return 없음 | dataclass 또는 Pydantic | 중 | 🔲 검토 |
| 46 | Depth | `scraper.py main()` | 동기/비동기/LS/전체 분기 → 단일 함수 | 함수 분리 | 중 | 🔲 검토 |
| 47 | 코드 | SQLiteManager | 운영 DB 전환 후 legacy 잔재 | archive 브랜치 보존 후 main 제거 | 중 | ✅ 완료 |
| 48 | 코드 | `models/WebScraper.py` | `firm_id` 기준 if/elif 10개 체인 | dict 기반 dispatch | 중 | 🔲 검토 |
| 49 | 테스트 | `tests/ls.py`, `tests/diagnose_ls_urls.py` | legacy test | archive | 하 | 🔲 검토 |
| 50 | DB | `tbl_sec_reports` 컬럼 35개 | 정규화 부족 + 일부 enrichment/AI 컬럼 저사용 | 검토 | 상 | 🔲 검토 |
| 51 | DB | `report_unique_key` 인덱스 3개 | unique `idx_report_unique_uid`, unique `tb_sec_reports_uid_key`, non-unique `idx_report_unique_key` 중복 | DDL 실행 전 정리 후보로 검토 | 하 | 🔲 검토 |
| 52 | 코드 | standalone news | 뉴스가 별도 컨테이너로 이관됨 | workflow/core/entrypoint 제거 | 하 | ✅ 완료 |
| 53 | 코드 | `utils/json_util.py` / `utils/report_json_store.py` | telegram/local-json 처리 이중화 | 호환 계약 보존을 위해 보류 | 하 | 🔶 보류 (호환 계약) |
| 54 | 코드 | `validate_scrape_result.py` | 결과 검증 규칙 | 호환 계약 보존을 위해 보류 | 하 | 🔶 보류 (호환 계약) |
| 55 | 코드 | `modules/LS_0.py` / `modules/BNKfn_23.py` | LS 및 BNK 모듈 내 스크래핑 | 호환 계약 보존을 위해 보류 | 하 | 🔶 보류 (호환 계약) |
