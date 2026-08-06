-- One-time repair for rows written before shared market classification.
--
-- Evidence priority matches models/market_classification.py:
--   1. A dedicated overseas source board is GLOBAL, including a domestic
--      ticker mentioned for comparison in its title.
--   2. Else a recognised overseas exchange suffix is GLOBAL.
--   3. A .KS/.KQ ticker blocks only title-based inference.
--
-- Safe to rerun: only KR rows are candidates, and the UPDATE changes them to
-- GLOBAL. Run through the production PostgreSQL wrapper, then invalidate the
-- external API cache.
BEGIN;

WITH corrected AS (
    UPDATE tbl_sec_reports AS r
    SET mkt_tp = 'GLOBAL'
    WHERE r.mkt_tp = 'KR'
      AND (
          (r.firm_id, r.board_id) IN (
              (0, 9),
              (1, 3), (1, 5),
              (3, 14), (3, 15), (3, 16),
              (4, 7), (4, 11),
              (5, 2),
              (9, 2),
              (10, 3),
              (18, 2),
              (25, 4), (25, 5)
          )
          OR (
              r.article_title !~* '[(][^)]*[.]K[QS]([^A-Z]|$)'
              AND r.article_title ~* '[(][^)]*[.](US|JP|HK|CH|CN|TW|FP|GR|LN|NA|SW|AU|IN|SP|SS|ID)([^A-Z]|$)'
          )
      )
    RETURNING r.firm_id, r.board_id
)
SELECT firm_id, board_id, COUNT(*) AS corrected_count
FROM corrected
GROUP BY firm_id, board_id
ORDER BY corrected_count DESC, firm_id, board_id;

COMMIT;
