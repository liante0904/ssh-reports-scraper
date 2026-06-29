--
-- KB증권 백필: 기존 레코드(article_board_order=0) → pCategoryid 기반 재분류
-- 2026-06-11
--
-- ※ KB API의 pCategoryid가 DB에 저장되지 않았으므로, 타이틀 패턴으로 역추정
-- ※ CASE WHEN은 첫 매칭 우선 — 특정 브랜드명 → 해외 → 중소형주 → 기업분석 순

UPDATE tbl_sec_reports
SET article_board_order = CASE

  -- 1) KB Macro (pCategoryid=8 → board_order=1)
  WHEN article_title ~* '(^KB Macro|^KB 매크로)' THEN 1

  -- 2) KB Bond (pCategoryid=9 → board_order=2)
  WHEN article_title ~* '(^KB Bond|^KB 채권|^Global Fixed Income)' THEN 2

  -- 3) KB 데일리 (pCategoryid=13 → board_order=3)
  WHEN article_title ~* '(KB 데일리|KB Daily)' THEN 3

  -- 4) 모닝코멘트 (pCategoryid=23 → board_order=4)
  WHEN article_title ~* '(모닝코멘트|Morning Comment)' THEN 4

  -- 5) 원자재 (pCategoryid=11 → board_order=5)
  WHEN article_title ~* '(^원자재|원자재 이슈)' THEN 5

  -- 6) 이그전 (pCategoryid=22 → board_order=9)
  WHEN article_title ~* '^이그전' THEN 9

  -- 7) Core View (pCategoryid=12 → board_order=10)
  WHEN article_title ~* '^KB Core View' THEN 10

  -- 8) Asia Headline (pCategoryid=39 → board_order=11)
  WHEN article_title ~* '^KB Asia Market Headline' THEN 11

  -- 9) 이슈 플러스 (pCategoryid=189 → board_order=12)
  WHEN article_title ~* '(^KB 이슈 플러스|KB Issue Plus)' THEN 12

  -- 10) Tracker+ (pCategoryid=186 → board_order=8)
  WHEN article_title ~* 'Tracker\+' THEN 8

  -- 11) Global Insights / 해외 (pCategoryid=26 → board_order=7)
  WHEN mkt_tp = 'GLOBAL' THEN 7
  WHEN article_title ~* '(Asia Monitor|Global Insight|Asia Strategy|Global Macro|US Market Pulse|해외주식|China|Japan|Vietnam|Indonesia|FX Market|Emerging Market|Global ESG)' THEN 7

  -- 12) 중소형주 (pCategoryid=38 → board_order=6) — Global보다 뒤에: 글로벌 기사에 '중소형주'가 부제로 들어가는 경우 방지
  WHEN article_title ~* '(^세나테크놀로지|^중소형주)' THEN 6

  -- 0) 기업분석 (pCategoryid=37 → board_order=0) — fallback (종목명 + 코드 패턴)
  ELSE 0
END
WHERE sec_firm_order = 4
  AND article_board_order = 0;

-- 검증 쿼리
-- SELECT article_board_order, count(*)
-- FROM tbl_sec_reports WHERE sec_firm_order = 4
-- GROUP BY article_board_order ORDER BY article_board_order;
