--
-- KB증권 게시판 정보 (sec_firm_order=4)
-- pCategoryid → article_board_order 매핑 (2026-06-11 확정)
--

INSERT INTO public.tbm_sec_firm_board_info (sec_firm_order, article_board_order, board_nm, label_nm) VALUES
  (4, 0,  '기업분석',       'KB 기업분석'),        -- pCategoryid=37
  (4, 1,  'KB Macro',       'KB Macro'),            -- pCategoryid=8
  (4, 2,  'KB Bond',        'KB Bond'),             -- pCategoryid=9
  (4, 3,  'KB 데일리',      'KB 데일리'),           -- pCategoryid=13
  (4, 4,  '모닝코멘트',     'KB 모닝코멘트'),       -- pCategoryid=23
  (4, 5,  '원자재',          '원자재 전망'),         -- pCategoryid=11
  (4, 6,  '중소형주',        'KB 중소형주'),         -- pCategoryid=38
  (4, 7,  'Global Insights','KB Global Insights'),  -- pCategoryid=26 (해외, mkt_tp=GLOBAL)
  (4, 8,  'Tracker+',       'KB Tracker+'),         -- pCategoryid=186 (비상장)
  (4, 9,  '이그전',          'KB 이그전'),           -- pCategoryid=22
  (4, 10, 'Core View',      'KB Core View'),        -- pCategoryid=12
  (4, 11, 'Asia Headline',  'KB Asia Headline'),    -- pCategoryid=39
  (4, 12, '이슈 플러스',     'KB 이슈 플러스')       -- pCategoryid=189
ON CONFLICT (sec_firm_order, article_board_order) DO UPDATE SET
  board_nm = EXCLUDED.board_nm,
  label_nm = EXCLUDED.label_nm;
