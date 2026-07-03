# Column Deprecation — Scraper-side Status

> **작성일**: 2026-07-03
> **최종수정**: 2026-07-03 (구현 완료)
> **범위**: scraper 29개 모듈 + `ssh_library/reports.py` insert pipeline
> **구현스펙**: `COLUMN_DEPRECATION_IMPL_SPEC.json`

---

## 현재 실제 상태 (코드 기준) — 2026-07-03 구현 완료

| # | 옛 컬럼 | → 새 컬럼 | ssh_library INSERT | scraper 모듈 dict 키 | 상태 |
|---|---------|-----------|:---:|:---:|:---:|
| 1 | `save_time` | `save_at` | ✅ `save_at` + fallback | ✅ 29개 전부 `save_at` | **완료** |
| 2 | `reg_dt` | `report_date` | ✅ `report_date` + fallback | ✅ 29개 전부 `report_date` | **완료** |
| 3 | `main_ch_send_yn` | `telegram_sent` | ✅ | N/A | **완료** |
| 4 | `key` | `report_unique_key` | ✅ `report_unique_key` 단독 | ✅ 29개 전부 `key=` 제거 | **완료** |
| 5 | `article_url` | — (제거) | ⚠️ INSERT 유지 (SELECT 의존) | 유지 | **과도기** |

### 변경 통계
- **ssh_library**: `reports.py` — INSERT 4컬럼 + SELECT 10메서드 이관 (34줄)
- **scraper 모듈**: 29개 모든 증권사 dict 키 이관 완료
- **소비자 코드**: 6개 파일 폴백 처리 (SQLiteManager, scraper.py, validate.py, DBfi_19, shinhan_core, test_core_contract)
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

### Phase 3 — DB 컬럼 드랍 (1주일 안정화 후)

```sql
ALTER TABLE tbl_sec_reports DROP COLUMN save_time;
ALTER TABLE tbl_sec_reports DROP COLUMN reg_dt;
ALTER TABLE tbl_sec_reports DROP COLUMN main_ch_send_yn;
ALTER TABLE tbl_sec_reports DROP COLUMN key;
ALTER TABLE tbl_sec_reports DROP COLUMN article_url;
```
