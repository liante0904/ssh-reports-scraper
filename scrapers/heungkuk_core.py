"""Heungkuk Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def _norm_date(text):
    if not text: return ""
    text = text.strip()
    m = re.search(r"(20\d{2})\D+(\d{1,2})\D+(\d{1,2})", text)
    if m: return f"{int(m.group(1)):04d}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    m = re.search(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\s+\d{2}:\d{2}:\d{2}\s+\w+\s+(20\d{2})\b", text, flags=re.IGNORECASE)
    if m:
        months = {m:i+1 for i,m in enumerate(["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"])}
        mon, dd, y = m.groups()
        return f"{int(y):04d}{months[mon.lower()]:02d}{int(dd):02d}"
    digits = re.sub(r"[^0-9]","",text)
    return digits[:8] if len(digits) >= 8 else ""

def scrape_heungkuk(cfg: dict) -> list[dict]:
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, list_url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not list_url: continue
        try:
            sess = requests.Session()
            sess.headers.update(cfg["headers"])
            resp = sess.get(list_url, timeout=20, verify=False)
            resp.raise_for_status()
            resp.encoding = cfg.get("encoding","euc-kr")
        except Exception: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        base = list_url.split("/research/")[0]
        bm = re.search(cfg["board_pattern"], list_url)
        bp = bm.group(1) if bm else "company"
        for tr in soup.select(cfg["table_sel"]):
            a = tr.select_one(cfg["link_sel"])
            if not a: continue
            km = re.search(cfg["onclick_pattern"], a.get("onclick",""))
            if not km: continue
            vk = int(km.group(1))
            title = re.sub(r"\s+"," ", a.get_text(" ",strip=True))
            cells = tr.find_all("td")
            if len(cells) < 5: continue
            writer = re.sub(r"\s+"," ",cells[2].get_text(" ",strip=True))
            rd = _norm_date(cells[3].get_text(" ",strip=True))
            pk = eval(cfg["pdf_formula"].replace("{view_key}",str(vk)))
            dl = cfg["download_tpl"].replace("{base}",base).replace("{pdf_key}",str(pk))
            au = cfg["view_tpl"].replace("{base}",base).replace("{board_path}",bp).replace("{view_key}",str(vk))
            result.append(dict(sec_firm_order=cfg["sec_firm_order"],article_board_order=board_order,
                firm_nm=cfg["firm_nm"],reg_dt=rd,download_url=dl,telegram_url=dl,pdf_url=dl,
                article_title=title,article_url=au,writer=writer,key=dl,report_unique_key=dl,
                save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
    print(f"[heungkuk] {len(result)} articles collected", file=sys.stderr)
    return result
