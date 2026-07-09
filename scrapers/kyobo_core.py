import sys
"""Kyobo Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_kyobo(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    # 기본값 (GA standalone에서 list만 전달되는 경우 대비)
    cfg.setdefault("headers", {"User-Agent": "Mozilla/5.0"})
    cfg.setdefault("empty_sel", ".noData")
    cfg.setdefault("row_sel", "table tbody tr")
    cfg.setdefault("board_types", {})
    cfg.setdefault("base_url", "https://www.iprovest.com")
    cfg.setdefault("cell_date", 5)
    cfg.setdefault("title_sel", "td.subject a")
    cfg.setdefault("cell_cat", 3)
    cfg.setdefault("cell_type", 4)
    cfg.setdefault("cell_writer", 6)
    cfg.setdefault("cell_attach", 7)
    cfg.setdefault("attach_replace_from", "")
    cfg.setdefault("attach_replace_to", "")
    cfg.setdefault("path_replace_from", "")
    cfg.setdefault("path_replace_to", "")
    cfg.setdefault("firm_id", 24)
    cfg.setdefault("firm_nm", "교보증권")
    cfg.setdefault("max_pages", 10)
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
                    dl = dl.rstrip("\')")
                    result.append(dict(firm_id=cfg["firm_id"],board_id=board,
                        firm_nm=cfg["firm_nm"],report_date=rd,telegram_url=dl,
                        pdf_file_url=dl,article_title=title,writer=writer,report_unique_key=dl,
                        save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
    print(f"[kyobo] {len(result)} articles collected", file=sys.stderr)
    return result
