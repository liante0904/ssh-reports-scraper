--
-- tbm_sec_firm_info — 증권사 마스터 (2026-06-10: ga_enabled_yn 추가)
--

CREATE TABLE public.tbm_sec_firm_info (
    sec_firm_order integer NOT NULL,
    firm_nm text NOT NULL,
    telegram_update_yn text DEFAULT 'N'::text,
    comment_pdf_url text,
    ga_enabled_yn text DEFAULT 'N'::text
);

ALTER TABLE ONLY public.tbm_sec_firm_info
    ADD CONSTRAINT tbm_sec_firm_info_pkey PRIMARY KEY (sec_firm_order);

-- GA 이관 완료 증권사 (10개) — GA standalone이 primary, 서버는 KST 1/7/13/21시 full-scrape fallback
-- sec_firm_order IN (2:NH투자, 4:KB, 5:삼성, 6:상상인, 10:키움, 15:토스, 16:리딩, 21:한화, 22:한양, 28:흥국)
