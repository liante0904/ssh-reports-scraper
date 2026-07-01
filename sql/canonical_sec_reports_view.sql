-- Canonical compatibility view for column migrations.
--
-- Canonical columns:
--   scraped_at        : save_at first, legacy save_time fallback
--   firm_id           : firm_id read-only alias
--   board_id          : board_id read-only alias
--

CREATE OR REPLACE VIEW public.v_sec_reports_canonical AS
SELECT
    r.*,
    r.firm_id AS firm_id,
    r.board_id AS board_id,
    COALESCE(
        r.save_at,
        CASE
            WHEN left(r.save_time, 10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (left(r.save_time, 10) || ' 00:00:00+09')::timestamptz
            ELSE NULL
        END
    ) AS scraped_at
FROM public.tbl_sec_reports r;
