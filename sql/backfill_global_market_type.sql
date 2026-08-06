-- One-time repair for rows written before shared market classification.
--
-- Evidence priority matches models/market_classification.py:
--   1. A domestic .KS/.KQ ticker is never changed.
--   2. A recognised overseas exchange suffix is GLOBAL.
--   3. Hana's dedicated overseas boards (14/15/16) are GLOBAL.
--
-- Safe to rerun: only KR rows are candidates, and the UPDATE changes them to
-- GLOBAL. Run through the production PostgreSQL wrapper, then invalidate the
-- external API cache.
BEGIN;

WITH corrected AS (
    UPDATE tbl_sec_reports AS r
    SET mkt_tp = 'GLOBAL'
    WHERE r.mkt_tp = 'KR'
      AND r.article_title !~* '[(][^)]*[.]K[QS]([^A-Z]|$)'
      AND (
          (r.firm_id = 3 AND r.board_id IN (14, 15, 16))
          OR r.article_title ~* '[(][^)]*[.](US|JP|HK|CH|CN|TW|FP|GR|LN|NA|SW|AU|IN|SP|SS|ID)([^A-Z]|$)'
      )
    RETURNING r.firm_id, r.board_id
)
SELECT firm_id, board_id, COUNT(*) AS corrected_count
FROM corrected
GROUP BY firm_id, board_id
ORDER BY corrected_count DESC, firm_id, board_id;

COMMIT;
