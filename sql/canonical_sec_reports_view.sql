-- Canonical compatibility view for column migrations.
--
-- Canonical columns:
--   scraped_at        : save_at first, legacy save_time fallback
--   firm_id / firm_name / board_id : read-only aliases
--   market_type       : mkt_tp alias
--

CREATE OR REPLACE VIEW public.v_sec_reports_canonical AS
SELECT
    r.*,
    COALESCE(
        r.save_at,
        CASE
            WHEN left(r.save_time, 10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (left(r.save_time, 10) || ' 00:00:00+09')::timestamptz
            ELSE NULL
        END
    ) AS scraped_at,
    r.firm_nm AS firm_name,
    r.mkt_tp AS market_type
FROM public.tbl_sec_reports r;
