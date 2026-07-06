-- Canonical compatibility view for column migrations.
--
-- Canonical columns:
--   scraped_at        : save_at canonical timestamp
--   firm_id / firm_name / board_id : read-only aliases
--   market_type       : mkt_tp alias
--
-- DEPRECATED aliases (2026-07-06):
--   key  → report_unique_key  (물리컬럼 DROP, 뷰 alias만 유지 — 하위호환)
--   reg_dt → report_date       (물리컬럼 DROP, 뷰 alias만 유지 — 하위호환)
--   save_time → save_at        (물리컬럼 DROP, 뷰 alias만 유지 — 하위호환)
-- 새 코드에서는 report_unique_key, report_date, save_at 을 직접 사용할 것.
--

CREATE OR REPLACE VIEW public.v_sec_reports_canonical AS
SELECT
    r.*,
    r.report_unique_key AS key,
    r.report_date AS reg_dt,
    r.save_at AS save_time,
    r.save_at AS scraped_at,
    r.firm_nm AS firm_name,
    r.mkt_tp AS market_type
FROM public.tbl_sec_reports r;
