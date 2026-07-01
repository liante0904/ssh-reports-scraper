CREATE OR REPLACE VIEW public.v_sec_firm_info AS
SELECT
    f.*,
    f.sec_firm_order AS firm_id
FROM public.tbm_sec_firm_info f;

CREATE OR REPLACE VIEW public.v_sec_firm_board_info AS
SELECT
    b.*,
    b.sec_firm_order AS firm_id,
    b.article_board_order AS board_id
FROM public.tbm_sec_firm_board_info b;
