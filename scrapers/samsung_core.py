import sys
"""Samsung Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_samsung(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not url: continue
        try:
            resp = requests.get(url, headers=cfg["headers"], verify=False, timeout=30)
            resp.raise_for_status()
        except Exception: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select(cfg["item_sel"]):
            try:
                t_el = item.select_one(cfg["title_sel"])
                if not t_el: continue
                title = t_el.text.strip()
                a_href = item.a.get("href","").replace("javascript:downloadPdf(","").replace(")","").replace("'","")
                parts = a_href.split(",")
                if len(parts) < 3: continue
                path, reg_dt = parts[0].strip(), parts[2].strip().replace(";","")
                dl = cfg["url_tpl"].replace("{path}", path)
                author = "N/A"
                dds = item.select(cfg["author_sel"])
                if len(dds) > cfg["author_idx"]: author = dds[cfg["author_idx"]].text.strip()
                title = title.replace(f"({author})", "")
                result.append(dict(sec_firm_order=cfg["sec_firm_order"],article_board_order=board_order,
                    firm_nm=cfg["firm_nm"],reg_dt=reg_dt,download_url="",telegram_url=dl,
                    article_title=title,writer=author,key=dl,report_unique_key=dl,
                    save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue
    print(f"[samsung] {len(result)} articles collected", file=sys.stderr)
    return result
