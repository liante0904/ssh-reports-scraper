--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: tbl_sec_reports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tbl_sec_reports (
    report_id bigint NOT NULL,
    sec_firm_order integer,
    article_board_order integer,
    firm_nm text,
    article_title text,
    article_url text,
    main_ch_send_yn text,
    download_status_yn text DEFAULT ''::text,
    download_url text,
    save_time text,
    reg_dt text DEFAULT ''::text,
    writer text DEFAULT ''::text,
    key text,
    telegram_url text DEFAULT ''::text,
    mkt_tp text DEFAULT 'KR'::text,
    gemini_summary text,
    summary_time text,
    summary_model text,
    archive_path text,
    retry_count integer DEFAULT 0,
    sync_status integer DEFAULT 0,
    pdf_url text DEFAULT ''::text,
    pdf_sync_status integer DEFAULT 0
);


--
-- Name: tb_sec_reports_report_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.tb_sec_reports_report_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tb_sec_reports_report_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.tb_sec_reports_report_id_seq OWNED BY public.tbl_sec_reports.report_id;


--
-- Name: tbl_sec_reports report_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tbl_sec_reports ALTER COLUMN report_id SET DEFAULT nextval('public.tb_sec_reports_report_id_seq'::regclass);


--
-- Name: tbl_sec_reports tbl_sec_reports_key_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tbl_sec_reports
    ADD CONSTRAINT tbl_sec_reports_key_unique UNIQUE (key);


--
-- Name: tbl_sec_reports tbl_sec_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tbl_sec_reports
    ADD CONSTRAINT tbl_sec_reports_pkey PRIMARY KEY (report_id);


--
-- Name: idx_tb_sec_reports_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tb_sec_reports_key ON public.tbl_sec_reports USING btree (key);


--
-- Name: idx_tb_sec_reports_reg_dt; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tb_sec_reports_reg_dt ON public.tbl_sec_reports USING btree (reg_dt);


--
-- Name: idx_tb_sec_reports_reg_dt_sec_firm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tb_sec_reports_reg_dt_sec_firm ON public.tbl_sec_reports USING btree (reg_dt DESC, sec_firm_order);


--
-- Name: idx_tb_sec_reports_save_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tb_sec_reports_save_time ON public.tbl_sec_reports USING btree (save_time);


--
-- Name: idx_tb_sec_reports_search; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tb_sec_reports_search ON public.tbl_sec_reports USING btree (firm_nm, article_title, writer);


--
-- Name: tbl_sec_reports trg_set_ds_share_telegram_url; Type: TRIGGER; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.set_ds_share_telegram_url()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.sec_firm_order = 11
       AND (NEW.telegram_url IS NULL OR NEW.telegram_url = '') THEN
        NEW.telegram_url := 'https://ssh-oci.netlify.app/share?id=' || NEW.report_id::text;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_set_ds_share_telegram_url BEFORE INSERT OR UPDATE ON public.tbl_sec_reports FOR EACH ROW EXECUTE FUNCTION public.set_ds_share_telegram_url();
