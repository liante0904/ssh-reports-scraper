import sys
"""Heungkuk Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from scrapers.config_guard import normalize_cfg

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
    cfg = normalize_cfg(cfg, firm_key="Heungkuk")
    cfg = {
        "headers": {"User-Agent": "Mozilla/5.0"},
        "board_pattern": r"/research/([^/]+)/list\.do",
        "table_sel": "table.data_list_x tbody tr",
        "link_sel": "td.left a",
        "onclick_pattern": r"key=(\d+)",
        "pdf_formula": "2 * {view_key} - 12039",
        "download_tpl": "{base}/download.do?type=Board&key={pdf_key}",
        "view_tpl": "{base}/research/{board_path}/view.do?key={view_key}",
        "sec_firm_order": 28,
        "firm_nm": "흥국증권",
        **cfg,
    }
    # 2026.06.24: PDF key 공식 시프트 (12059→12039). GA secret override 방지.
    cfg["pdf_formula"] = "2 * {view_key} - 12039"
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
        board_pat = cfg["board_pattern"].replace("\\\\","\\")
        bm = re.search(board_pat, list_url)
        bp = bm.group(1) if bm else "company"
        for tr in soup.select(cfg["table_sel"]):
            a = tr.select_one(cfg["link_sel"])
            if not a: continue
            onclick = a.get("onclick","")
            pat = cfg["onclick_pattern"].replace("\\\\","\\")
            km = re.search(pat, onclick)
            if not km:
                km = re.search(r"key=(\d+)", onclick)
            if not km: continue
            vk = int(km.group(1))
            title = re.sub(r"\s+"," ", a.get_text(" ",strip=True))
            cells = tr.find_all("td")
            if len(cells) < 5: continue
            writer = re.sub(r"\s+"," ",cells[2].get_text(" ",strip=True))
            analyst_key = ""
            analyst_link = cells[2].find("a")
            if analyst_link:
                am = re.search(r"key=(\d+)", analyst_link.get("href", ""))
                if am:
                    analyst_key = am.group(1)
            rd = _norm_date(cells[3].get_text(" ",strip=True))
            # Heungkuk key 체계는 불규칙하지만 GA에서는 시간 제한이 더 중요하다.
            # 정확한 filename 매칭이 필요할 때만 config로 HEAD scan을 명시적으로 켠다.
            pk = None
            if cfg.get("match_pdf_by_analyst") and analyst_key:
                for offset in range(12065, 12025, -1):
                    candidate = 2 * vk - offset
                    try:
                        import urllib.request
                        req = urllib.request.Request(
                            f"{base}/download.do?type=Board&key={candidate}",
                            headers={"User-Agent": cfg["headers"].get("User-Agent", "Mozilla/5.0")},
                            method="HEAD")
                        resp = urllib.request.urlopen(req, timeout=0.5)
                        disp = resp.getheader("Content-Disposition", "")
                        if ".pdf" in disp.lower() and analyst_key in disp:
                            pk = candidate
                            break
                    except Exception:
                        pass
            if pk is None:
                pk = eval(cfg["pdf_formula"].replace("{view_key}", str(vk)))
            dl = cfg["download_tpl"].replace("{base}",base).replace("{pdf_key}",str(pk))
            au = cfg["view_tpl"].replace("{base}",base).replace("{board_path}",bp).replace("{view_key}",str(vk))
            result.append(dict(sec_firm_order=cfg["sec_firm_order"],article_board_order=board_order,
                firm_nm=cfg["firm_nm"],reg_dt=rd,download_url=dl,telegram_url=dl,pdf_url=dl,
                article_title=title,article_url=au,writer=writer,key=dl,report_unique_key=dl,
                save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
    print(f"[heungkuk] {len(result)} articles collected", file=sys.stderr)
    return result
