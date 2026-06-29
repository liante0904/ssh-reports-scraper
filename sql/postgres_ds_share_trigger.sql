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

DROP TRIGGER IF EXISTS trg_set_ds_share_telegram_url ON public.tbl_sec_reports;

CREATE TRIGGER trg_set_ds_share_telegram_url
BEFORE INSERT OR UPDATE ON public.tbl_sec_reports
FOR EACH ROW
EXECUTE FUNCTION public.set_ds_share_telegram_url();
