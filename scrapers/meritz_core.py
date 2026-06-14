import sys
"""Meritz Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_meritz(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    hdr = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    result = []
    for board_order, base_url in enumerate(cfg.get("urls", [cfg.get("url","")])):
        if not base_url: continue
        for page in range(1, 4):
            url = base_url.replace("pageNum=1", f"pageNum={page}")
            try:
                resp = requests.get(url, headers=hdr, timeout=30, verify=False)
                resp.raise_for_status()
            except Exception: break
            soup = BeautifulSoup(resp.text, "html.parser")
            head_rows = soup.select(cfg["head_sel"])
            if not head_rows: break
            hmap = {th.get_text(strip=True): i for i, th in enumerate(head_rows)}
            rows = soup.select(cfg["row_sel"])
            if not rows: break
            for row in rows:
                try:
                    link = row.select_one(f'td:nth-child({hmap.get("제목",0)+1}) a')
                    if not link: continue
                    title = link.get_text(strip=True)
                    article_url = cfg["base_url"] + link["href"]
                    dc = "작성일" if "작성일" in hmap else "작성일시"
                    rd = re.sub(r"[-./]","",row.select_one(f'td:nth-child({hmap[dc]+1})').get_text(strip=True))
                    wc = "작성자" if "작성자" in hmap else "작성자명"
                    writer = row.select_one(f'td:nth-child({hmap[wc]+1})').get_text(strip=True)
                    # detail fetch for PDF
                    try:
                        dr = requests.get(article_url, timeout=15, verify=False)
                        ds = BeautifulSoup(dr.text, "html.parser")
                        dl_tag = ds.select_one(cfg["detail_sel"])
                        if dl_tag and "title" in dl_tag.attrs:
                            fn = dl_tag["title"].replace(cfg["detail_replace"],"").strip()
                            dl = cfg["detail_url_tpl"].replace("{fname}", fn)
                        else: dl = article_url
                    except Exception: dl = article_url
                    result.append(dict(sec_firm_order=cfg["sec_firm_order"],article_board_order=board_order,
                        firm_nm=cfg["firm_nm"],reg_dt=rd,article_url=article_url,
                        download_url=dl,telegram_url=dl,article_title=title,writer=writer,
                        key=dl,report_unique_key=dl,save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
    print(f"[meritz] {len(result)} articles collected", file=sys.stderr)
    return result
