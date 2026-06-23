import sys
"""Kyobo Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_kyobo(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, base_url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not base_url: continue
        for page in range(1, cfg.get("max_pages", 10) + 1):
            url = f"{base_url}&pageNum={page}"
            try:
                resp = requests.get(url, headers=cfg["headers"], timeout=30, verify=False)
                resp.raise_for_status()
            except Exception: break
            soup = BeautifulSoup(resp.text, "html.parser")
            if soup.select_one(cfg["empty_sel"]): break
            for row in soup.select(cfg["row_sel"]):
                try:
                    rd = row.select_one(f"td:nth-child({cfg['cell_date']})").get_text(strip=True).replace("/","")
                    tc = row.select_one(cfg["title_sel"])
                    if not tc: continue
                    title = tc.get_text(strip=True)
                    article_url = cfg["base_url"] + tc["href"]
                    cat = row.select_one(f"td:nth-child({cfg['cell_cat']})").get_text(strip=True)
                    atype = row.select_one(f"td:nth-child({cfg['cell_type']})").get_text(strip=True)
                    board = cfg["board_types"].get(atype, 4)
                    if atype in ("기업분석","산업분석"): title = f"{cat} : {title}"
                    writer = row.select_one(f"td:nth-child({cfg['cell_writer']}) a").get_text(strip=True)
                    atag = row.select_one(f"td:nth-child({cfg['cell_attach']}) a")
                    dl = ""
                    if atag:
                        dl = cfg["base_url"] + atag["href"].replace(cfg.get("attach_replace_from",""),"").replace(cfg.get("attach_replace_to",""),"").replace(cfg.get("path_replace_from",""),cfg.get("path_replace_to",""))
                    # trailing ') 방어: onclick=\"fn('url')\" 파싱 잔재 제거
                    dl = dl.rstrip(\"')\")
                    result.append(dict(sec_firm_order=cfg["sec_firm_order"],article_board_order=board,
                        firm_nm=cfg["firm_nm"],reg_dt=rd,download_url=dl,telegram_url=dl,
                        pdf_url=dl,article_title=title,writer=writer,key=dl,report_unique_key=dl,
                        save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
    print(f"[kyobo] {len(result)} articles collected", file=sys.stderr)
    return result
