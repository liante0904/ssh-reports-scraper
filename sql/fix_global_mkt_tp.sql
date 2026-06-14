-- mkt_tp=GLOBAL로 잘못 분류된 국내 종목 보정
-- .KQ/.KS 티커가 제목에 있으면 국내(KR)로 분류

UPDATE tbl_sec_reports SET mkt_tp = 'KR'
WHERE mkt_tp = 'GLOBAL' 
AND article_title ~ '\([0-9]{5,6}\.K[QS]\)';

UPDATE tbl_sec_reports SET mkt_tp = 'KR'
WHERE mkt_tp = 'GLOBAL' 
AND article_title ~* '코스피|코스닥|국내';
