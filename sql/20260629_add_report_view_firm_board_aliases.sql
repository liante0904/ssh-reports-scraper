ALTER TABLE tbl_sec_reports RENAME COLUMN sec_firm_order TO firm_id;
ALTER TABLE tbl_sec_reports RENAME COLUMN article_board_order TO board_id;

CREATE OR REPLACE VIEW public.v_sec_reports_canonical AS
SELECT
    r.*,
    r.firm_id AS sec_firm_order,
    r.board_id AS article_board_order,
    COALESCE(
        r.save_at,
        CASE
            WHEN left(r.save_time, 10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (left(r.save_time, 10) || ' 00:00:00+09')::timestamptz
            ELSE NULL
        END
    ) AS scraped_at
FROM public.tbl_sec_reports r;
