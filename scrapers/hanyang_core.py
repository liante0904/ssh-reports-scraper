import sys
"""Hanyang Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def scrape_hanyang(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    # 기본값 (GA standalone에서 list만 전달되는 경우 대비)
    cfg.setdefault("headers", {"User-Agent": "Mozilla/5.0"})
    cfg.setdefault("table_class", "board_list")
    cfg.setdefault("row_sel", "tbody tr")
    cfg.setdefault("cell_title", 1)
    cfg.setdefault("cell_reg_dt", 3)
    cfg.setdefault("cell_attach", 4)
    cfg.setdefault("base_url", "https://www.hygood.co.kr")
    cfg.setdefault("firm_id", 22)
    cfg.setdefault("firm_nm", "한양증권")
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not url: continue
        try:
            resp = requests.get(url, headers=cfg["headers"], verify=False, timeout=30)
            resp.raise_for_status()
        except Exception: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_=cfg["table_class"])
        if not table: continue
        for row in table.select(cfg["row_sel"]):
            try:
                cells = row.find_all("td")
                if len(cells) <= max(cfg["cell_title"],cfg["cell_reg_dt"],cfg.get("cell_attach",0)): continue
                link = cells[cfg["cell_title"]].find("a")
                if not link: continue
                title = link.get_text(strip=True)
                reg_dt = cells[cfg["cell_reg_dt"]].get_text(strip=True)
                dl = ""
                ac = cells[cfg["cell_attach"]].find("a") if "cell_attach" in cfg else None
                if ac: dl = urljoin(url, ac.get("href",""))
                result.append(dict(firm_id=cfg["firm_id"],board_id=board_order,
                    firm_nm=cfg["firm_nm"],report_date=re.sub(r"[-./]","",reg_dt),
                    article_title=title,article_url=dl,download_url=dl,telegram_url=dl,
                    pdf_url=dl,report_unique_key=dl,save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue
    print(f"[hanyang] {len(result)} articles collected", file=sys.stderr)
    return result
