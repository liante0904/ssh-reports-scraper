--
-- 하위 호환성 뷰: tbl_sec_reports 단일 테이블 (정규화 테이블 제거됨)
-- 2026-07-09: 유령 테이블 3종 DROP, LEFT JOIN 제거
--
-- DEPRECATED aliases (하위호환 유지):
--   key  → report_unique_key
--   reg_dt → report_date
--   save_time → save_at
-- 새 코드에서는 report_unique_key, report_date, save_at 을 직접 사용할 것.
--
CREATE OR REPLACE VIEW v_sec_reports_full AS
SELECT
    r.report_id, r.firm_id, r.board_id, r.firm_nm,
    r.article_title, NULL::text AS source_url,
    r.report_unique_key AS key, r.report_unique_key,
    r.report_date AS reg_dt, r.report_date,
    r.save_at AS save_time, r.save_at,
    r.telegram_sent,
    r.telegram_url, r.writer, r.mkt_tp,
    r.pdf_url AS pdf_file_url,
    r.download_status_yn, r.pdf_sync_status, r.pdf_hash,
    r.sync_status, r.retry_count, r.archive_path,
    r.tags, r.stock_names, r.stock_tickers, r.sector,
    r.gemini_summary, r.summary_time, r.summary_model,
    r.target_price, r.rating, r.revision_type, r.report_type,
    r.fnguide_summary_id
FROM tbl_sec_reports r;
