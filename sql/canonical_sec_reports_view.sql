-- Canonical compatibility view for column migrations.
--
-- Canonical columns:
--   report_key        : report_unique_key first, legacy key fallback
--   scraped_at        : save_at first, legacy save_time fallback
--   notification_sent : is_sent first, legacy main_ch_send_yn fallback
--
-- This view is for read/API/analysis migration. It is not a substitute for
-- unique indexes on the base table.

CREATE OR REPLACE VIEW public.v_sec_reports_canonical AS
SELECT
    r.*,
    COALESCE(NULLIF(r.report_unique_key, ''), NULLIF(r.key, '')) AS report_key,
    COALESCE(
        r.save_at,
        CASE
            WHEN left(r.save_time, 10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (left(r.save_time, 10) || ' 00:00:00+09')::timestamptz
            ELSE NULL
        END
    ) AS scraped_at,
    (
        COALESCE(r.is_sent, false)
        OR r.main_ch_send_yn = 'Y'
    ) AS notification_sent,
    CASE
        WHEN COALESCE(NULLIF(r.report_unique_key, ''), NULLIF(r.key, '')) IS NULL
        THEN 'missing'
        WHEN NULLIF(r.report_unique_key, '') IS NULL
        THEN 'legacy_key'
        WHEN NULLIF(r.key, '') IS NULL
        THEN 'report_unique_key'
        WHEN r.report_unique_key = r.key
        THEN 'aligned'
        ELSE 'mismatch'
    END AS report_key_status
FROM public.tbl_sec_reports r;
