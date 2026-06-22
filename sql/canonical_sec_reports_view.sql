-- Canonical compatibility view for column migrations.
--
-- Canonical columns:
--   report_key        : report_unique_key directly (legacy key removed)
--   scraped_at        : save_at first, legacy save_time fallback
--   notification_sent : is_sent first, legacy main_ch_send_yn fallback
--

CREATE OR REPLACE VIEW public.v_sec_reports_canonical AS
SELECT
    r.*,
    r.report_unique_key AS report_key,
    COALESCE(
        r.save_at,
        CASE
            WHEN left(r.save_time, 10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (left(r.save_time, 10) || ' 00:00:00+09')::timestamptz
            ELSE NULL
        END
    ) AS scraped_at,
    COALESCE(r.telegram_sent, false) AS notification_sent,
    'report_unique_key'::text AS report_key_status
FROM public.tbl_sec_reports r;
