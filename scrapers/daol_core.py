import sys
"""DAOL Securities — config 기반 HTML 파싱."""
import re, requests, urllib.parse as urlparse
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_daol(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not url: continue
        parsed = urlparse.urlparse(url)
        query = urlparse.parse_qs(parsed.query)
        base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        target = base + "?" + cfg["path_tpl"]
        form = dict(cfg["default_form"])
        form["curPage"] = "1"
        form["startDate"] = f"{datetime.now(timezone(timedelta(hours=9))).year}/01/01"
        form["endDate"] = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
        for k in cfg.get("form_keys",[]): form[k] = query.get(k,[""])[0]
        form["searchNm2"] = form.get("rGubun","")
        h = {**cfg["headers"],"Origin":cfg["origin"],"Referer":url,"Host":parsed.netloc}
        try:
            resp = requests.post(target, data=form, headers=h, timeout=30, verify=False)
            resp.raise_for_status()
        except Exception: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for row in soup.select(cfg["row_sel"]):
            cells = row.select("td")
            if len(cells) < 3: continue
            link = cells[1].select_one(cfg["cell_link"])
            if not link: continue
            title = link.get("title","")
            if cfg.get("skip_title") in title: continue
            rd = cells[0].get_text(strip=True).replace("/","")
            writer = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            parts = link["href"].split(cfg["link_split_pattern"])
            if len(parts) != 3: continue
            path = parts[0].split("'")[1]
            fn = parts[1].split("'")[1]
            dl = cfg["pdf_tpl"].replace("{path}",path).replace("{filename}",fn)
            result.append(dict(firm_id=cfg["firm_id"],board_id=board_order,
                firm_nm=cfg["firm_nm"],report_date=rd,download_url=dl,telegram_url=dl,
                article_title=title,writer=writer,report_unique_key=dl,
                save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
    print(f"[daol] {len(result)} articles collected", file=sys.stderr)
    return result
