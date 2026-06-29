--
-- FnGuide 매칭 성능 최적화 인덱스 + writer 분할 뷰
-- 2026-06-11
--

-- 1) report_date 인덱스 (reg_dt text → report_date date 마이그레이션 완료)
-- FnGuide 매처가 +-1일 범위 검색을 date 타입으로 직접 수행 가능
CREATE INDEX IF NOT EXISTS idx_reports_report_date ON tbl_sec_reports(report_date);

-- 2) (증권사, 게시판) 복합 인덱스
-- KB 게시판 13종 분류 완료 → 게시판 단위로 후보군 범위 축소
CREATE INDEX IF NOT EXISTS idx_reports_board ON tbl_sec_reports(sec_firm_order, article_board_order);

-- 3) writer 인덱스 — FnGuide 매처가 작성자 비교할 때 사용
CREATE INDEX IF NOT EXISTS idx_reports_writer ON tbl_sec_reports(writer);

-- 4) FnGuide 쪽 작성자 분할 정규화 뷰 (optional)
-- FnGuide의 author 필드는 "홍길동,김철수" 형태
-- 이 뷰를 통해 individual author로 검색 가능
CREATE OR REPLACE VIEW v_fnguide_authors AS
SELECT
    summary_id,
    report_title,
    company_name,
    provider,
    report_date,
    unnest(string_to_array(
        regexp_replace(
            regexp_replace(author, '[\(\[].*?[\)\]]', '', 'g'),  -- 괄호 안 제거
            '[,;&/]', ',', 'g'                                     -- 구분자 통일
        ), ','
    )) AS individual_author
FROM tbl_fnguide_report_summaries
WHERE author IS NOT NULL AND author != '';
