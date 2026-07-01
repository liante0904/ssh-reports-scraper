import sys
"""Yuanta Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_yuanta(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    # 기본값 (GA standalone에서 list만 전달되는 경우 대비)
    cfg.setdefault("board_codes", [])
    cfg.setdefault("headers", {"User-Agent": "Mozilla/5.0"})
    cfg.setdefault("row_sel", "table tbody tr")
    cfg.setdefault("cell_date", 5)
    cfg.setdefault("cell_title", "a")
    cfg.setdefault("stock_sel", ".stock")
    cfg.setdefault("writer_sel", ".writer a")
    cfg.setdefault("pdf_sel", "a")
    cfg.setdefault("pdf_attr", "href")
    cfg.setdefault("url_tpl", "https://www.myasset.com/myasset/research/rs_view/rs_view.cmd?cd007={code}&seq={seq}")
    cfg.setdefault("pdf_tpl", "{path}")
    cfg.setdefault("pages", 5)
    cfg.setdefault("firm_id", 27)
    cfg.setdefault("firm_nm", "유안타증권")
    requests.packages.urllib3.disable_warnings()
    result = []
    base_url = cfg.get("urls",[cfg.get("url","")])[0] if isinstance(cfg.get("urls"), list) else cfg.get("url","")
    if not base_url: return result
    for board_idx, code in enumerate(cfg["board_codes"]):
        for page in range(1, cfg.get("pages", 5) + 1):
            url = f"{base_url}?cd007={code}&pgCnt=100&page={page}"
            try:
                resp = requests.get(url, headers=cfg["headers"], timeout=30, verify=False)
                if resp.status_code != 200: break
            except Exception: break
            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.select(cfg["row_sel"])
            if not items: break
            for item in items:
                try:
                    cd = cfg["cell_date"]
                    post_date = item.select_one(f"td:nth-of-type({cd})").get_text(strip=True)
                    reg_dt = datetime.strptime(post_date, "%Y/%m/%d").strftime("%Y%m%d")
                    ttag = item.select_one(cfg["cell_title"])
                    title = ttag.get_text(strip=True)
                    if board_idx == 0:
                        stag = item.select_one(cfg["stock_sel"])
                        if stag: title = f"{stag.get_text(strip=True)}: {title}"
                    seq = ttag.get("data-seq","")
                    article_url = cfg["url_tpl"].replace("{code}",code).replace("{seq}",seq)
                    writers = [a.get_text(strip=True) for a in item.select(cfg["writer_sel"])]
                    writer = ", ".join(writers)
                    pt = item.select_one(cfg["pdf_sel"])
                    dl = ""
                    if pt and pt.has_attr(cfg["pdf_attr"]):
                        dl = cfg["pdf_tpl"].replace("{path}", pt[cfg["pdf_attr"]])
                    # dl이 비어있으면 article_url을 fallback으로 사용
                    if not dl: dl = article_url
                    result.append(dict(firm_id=cfg["firm_id"],board_id=board_idx,
                        firm_nm=cfg["firm_nm"],reg_dt=reg_dt,
                        download_url=dl,telegram_url=dl,pdf_url=dl,writer=writer,
                        key=article_url,report_unique_key=article_url,
                        article_title=title,save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
    print(f"[yuanta] {len(result)} articles collected", file=sys.stderr)
    return result
