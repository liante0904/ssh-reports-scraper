import sys
"""Mirae Asset Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_miraeasset(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    result = []
    for idx, url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not url: continue
        try:
            resp = requests.get(url, timeout=30, verify=False)
            resp.raise_for_status()
        except Exception: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select(cfg["row_sel"])[cfg.get("skip_rows",0):]
        for row in rows:
            try:
                rdt = re.sub(r"[-./]","",row.select_one(f"td:nth-child({cfg['cell_reg_dt']})").get_text(strip=True))
                title = row.select_one(f"td:nth-child({cfg['cell_title']})").get_text(strip=True)
                writer = row.select_one(f"td:nth-child({cfg['cell_writer']})").get_text(strip=True)
                dl = "없음"
                attach = row.select_one(cfg["attach_sel"])
                if attach:
                    m = re.search(cfg["attach_pattern"], attach["href"])
                    if m: dl = m.group(1)
                result.append(dict(firm_id=cfg["firm_id"],board_id=idx,
                    firm_nm=cfg["firm_nm"],reg_dt=rdt,writer=writer,download_url=dl,telegram_url=dl,
                    article_title=title,save_time=datetime.now(timezone(timedelta(hours=9))).isoformat(),key=dl,report_unique_key=dl))
            except Exception: continue
    print(f"[miraeasset] {len(result)} articles collected", file=sys.stderr)
    return result
