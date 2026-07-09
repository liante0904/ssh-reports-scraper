import sys
"""Leading Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_leading(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    # 기본값 (GA standalone에서 list만 전달되는 경우 대비)
    cfg.setdefault("table_sel", "table.basic-tbl thead tr")
    cfg.setdefault("row_sel", "table.basic-tbl tbody tr")
    cfg.setdefault("base_url", "http://www.leading.co.kr")
    cfg.setdefault("firm_id", 16)
    cfg.setdefault("firm_nm", "리딩투자증권")
    cfg.setdefault("attach_header", "첨부")
    cfg.setdefault("date_header", "등록일")
    cfg.setdefault("title_header", "제목")
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not url: continue
        try:
            resp = requests.get(url, timeout=30, verify=False)
            resp.raise_for_status()
        except Exception: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        head_rows = soup.select(cfg["table_sel"])
        if not head_rows: continue
        headers = [th.text.strip() for th in head_rows[0].find_all("th")]
        for row in soup.select(cfg["row_sel"]):
            try:
                cols = []
                for idx, td in enumerate(row.find_all("td")):
                    if idx >= len(headers): break
                    hdr = headers[idx]
                    if hdr == cfg.get("attach_header","첨부"):
                        a_tag = td.find("a")
                        cols.append(a_tag.attrs.get("href","").strip() if a_tag else "")
                    else: cols.append(td.get_text(strip=True))
                if len(cols) < len(headers): continue
                rd = dict(zip(headers, cols))
                dl = "없음"
                if rd.get(cfg.get("attach_header","첨부")):
                    dl = cfg["base_url"] + rd[cfg["attach_header"]]
                result.append(dict(firm_id=cfg["firm_id"],board_id=board_order,
                    firm_nm=cfg["firm_nm"],report_date=re.sub(r"[-./]","",rd.get(cfg["date_header"],"")),
                    telegram_url=dl,pdf_file_url=dl,
                    article_title=rd.get(cfg["title_header"],"No Title"),
                    save_at=datetime.now(timezone(timedelta(hours=9))).isoformat(),report_unique_key=dl))
            except Exception: continue
    print(f"[leading] {len(result)} articles collected", file=sys.stderr)
    return result
