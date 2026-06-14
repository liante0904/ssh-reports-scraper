"""SK Securities — config 기반 JSON POST."""
import sys, re, requests
from datetime import datetime, timezone, timedelta

def scrape_sks(cfg: dict) -> list[dict]:
    requests.packages.urllib3.disable_warnings()
    result = []
    urls = cfg.get("urls", [cfg.get("url","")])
    if isinstance(urls, str): urls = [urls]
    for board_order, url in enumerate(urls):
        if not url: continue
        payload = {"searchVal":"","searchType":"","page":1,"rowPerPage":cfg.get("row_per_page",2000),"_r_":"0.999"}
        try:
            resp = requests.post(url, params=payload, timeout=30, verify=False)
            resp.raise_for_status()
            items = resp.json().get(cfg.get("list_key","list"), [])
        except Exception: continue
        for item in items:
            try:
                pdfpath = item.get(cfg.get("pdf_key","PDFPATH"),"").strip()
                dl = ""
                if pdfpath:
                    dl = cfg.get("pdf_base","https://www.sks.co.kr") + cfg.get("pdf_path_prefix","/Upload/Research/") + pdfpath
                reg_dt = (item.get(cfg.get("date_key","RDATE"),"") or "").strip()
                reg_dt = re.sub(r"[-./]","",reg_dt)
                title = item.get(cfg.get("title_key","RSUBJECT"),"").strip()
                writer = item.get(cfg.get("writer_key","RWRITER"),"").strip()
                result.append(dict(sec_firm_order=cfg.get("sec_firm_order",26),
                    article_board_order=board_order,firm_nm=cfg.get("firm_nm","SK증권"),
                    reg_dt=reg_dt,download_url=dl,telegram_url=dl,pdf_url=dl,
                    article_title=title,writer=writer,
                    save_time=datetime.now(timezone(timedelta(hours=9))).isoformat(),
                    key=dl,report_unique_key=dl))
            except Exception: continue
    print(f"[sks] {len(result)} articles collected", file=sys.stderr)
    return result
