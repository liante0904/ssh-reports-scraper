--
-- 하위 호환성 뷰: tbl_sec_reports + 정규화 테이블 3종 JOIN
-- 2026-06-11
--
-- tbl_report_downloads는 pdf-archiver가 tbl_sec_reports에 직접 쓰므로
-- 별도 분리 불필요 → DROP 완료. download 관련 컬럼은 tbl_sec_reports에 그대로 유지.
--
-- DEPRECATED aliases (2026-07-06):
--   key  → report_unique_key  (물리컬럼 DROP, 뷰 alias만 유지 — 하위호환)
--   reg_dt → report_date       (물리컬럼 DROP, 뷰 alias만 유지 — 하위호환)
--   save_time → save_at        (물리컬럼 DROP, 뷰 alias만 유지 — 하위호환)
-- 새 코드에서는 report_unique_key, report_date, save_at 을 직접 사용할 것.
--
CREATE OR REPLACE VIEW v_sec_reports_full AS
SELECT
    -- 핵심 컬럼 (tbl_sec_reports)
    r.report_id, r.firm_id, r.board_id, r.firm_nm,
    r.firm_id AS firm_id, r.board_id AS board_id,
    r.article_title, r.article_url, r.report_unique_key AS key, r.report_unique_key,
    r.report_date AS reg_dt, r.report_date,
    r.save_at AS save_time, r.save_at,
    r.telegram_sent,
    r.telegram_url, r.writer, r.mkt_tp,
    r.download_url, r.pdf_url,
    -- pdf-archiver 관리 컬럼 (tbl_sec_reports에 직접 씀)
    r.download_status_yn, r.pdf_sync_status, r.pdf_hash,
    r.sync_status, r.retry_count, r.archive_path,

    -- tbl_report_enricher_tags
    COALESCE(t.tags, '[]'::jsonb) AS tags,
    COALESCE(t.stock_names, '[]'::jsonb) AS stock_names,
    COALESCE(t.stock_tickers, '[]'::jsonb) AS stock_tickers,
    COALESCE(t.sector, '') AS sector,

    -- tbl_report_ai_summaries
    s.gemini_summary, s.summary_time, s.summary_model,

    -- tbl_report_price_targets
    p.target_price, p.rating, p.revision_type, p.report_type,
    p.fnguide_summary_id

FROM tbl_sec_reports r
LEFT JOIN tbl_report_enricher_tags t ON r.report_id = t.report_id
LEFT JOIN tbl_report_ai_summaries s ON r.report_id = s.report_id
LEFT JOIN tbl_report_price_targets p ON r.report_id = p.report_id;
