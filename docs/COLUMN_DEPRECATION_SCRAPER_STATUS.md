# Column Deprecation — Scraper-side Status

> **작성일**: 2026-07-03
> **최종수정**: 2026-07-09 (운영 DB 드랍 상태 반영)
> **범위**: scraper 29개 모듈 + `ssh_library/reports.py` insert pipeline
> **구현스펙**: `COLUMN_DEPRECATION_IMPL_SPEC.json`

---

## 현재 실제 상태 — 2026-07-09 KST 운영 DB 기준

`public.tbl_sec_reports` 물리 컬럼은 현재 35개다.

```text
report_id, firm_id, board_id, firm_nm, article_title, article_url,
download_status_yn, download_url, writer, telegram_url, mkt_tp,
gemini_summary, summary_time, summary_model, archive_path, retry_count,
sync_status, pdf_url, pdf_sync_status, pdf_hash, tags, stock_names,
sector, fnguide_summary_id, target_price, rating, revision_type,
report_type, stock_tickers, report_date, telegram_sent,
report_unique_key, save_at, article_text, gdrive_pdf_url
```

`key`, `reg_dt`, `save_time`, `main_ch_send_yn`은 운영 DB에서 이미 물리 컬럼이 아니다.

| # | 옛 컬럼 | → 새 컬럼 | ssh_library INSERT | scraper 모듈 dict 키 | 상태 |
|---|---------|-----------|:---:|:---:|:---:|
| 1 | `save_time` | `save_at` | ✅ `save_at` + fallback | ✅ 29개 전부 `save_at` | **완료** |
| 2 | `reg_dt` | `report_date` | ✅ `report_date` + fallback | ✅ 29개 전부 `report_date` | **완료** |
| 3 | `main_ch_send_yn` | `telegram_sent` | ✅ | N/A | **완료** |
| 4 | `key` | `report_unique_key` | ✅ `report_unique_key` 단독 | ✅ 29개 전부 `key=` 제거 | **완료** |
| 5 | `article_url` | — | ✅ 운영 물리 컬럼 유지 | 유지 | **유지** |

### 변경 통계
- **ssh_library**: `reports.py` — INSERT 4컬럼 + SELECT 10메서드 이관 (34줄)
- **scraper 모듈**: 29개 모든 증권사 dict 키 이관 완료
- **소비자 코드**: canonical 컬럼 경로로 정리됨 (scraper.py, validate.py, DBfi_19, shinhan_core, test_core_contract)
- **총 변경**: 67 files, +309/-215

---

## 검증 체크리스트

- [x] `ssh_library/reports.py` INSERT 컬럼명 변경 + fallback 매핑
- [x] `ssh_library/reports.py` SELECT 쿼리들 신규 컬럼명으로 변경
- [x] scraper 29개 `save_time` → `save_at` dict 키 변경
- [x] scraper 29개 `reg_dt` → `report_date` dict 키 변경
- [x] scraper 29개 `key` 병행 설정 제거 (`report_unique_key` 만 유지)
- [ ] GA workflow 25/29 정상 실행 확인 (배포 후)
- [ ] `v_sec_reports_full` 뷰로 프론트 정상 조회 확인 (배포 후)
- [ ] 1주일 이상 `missing_save_at=0`, `missing_report_date=0` 확인 (운영 검증)

### Phase 3 — DB 컬럼 드랍 상태

운영 DB에서는 `save_time`, `reg_dt`, `main_ch_send_yn`, `key`가 이미 드랍되어 있다. 새 DDL 작업 항목으로 남기지 않는다.

`article_url`은 여전히 운영 물리 컬럼이다. 원문 페이지가 있는 증권사와 URL 조사/디버깅 경로에서 쓰이므로 이 문서에서는 드랍 대상으로 보지 않는다.

### 남은 DB 정리 후보

- `report_unique_key` 중복 인덱스 정리: 운영에는 unique `idx_report_unique_uid`, unique `tb_sec_reports_uid_key`, non-unique `idx_report_unique_key`가 함께 존재한다. DDL 실행 전에는 충돌 경로와 배포 의존성을 별도 점검한다.
