import sys
"""Samsung Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from scrapers.legacy_url_config import normalize_legacy_url_config

def scrape_samsung(cfg: dict) -> list[dict]:
    cfg = normalize_legacy_url_config(cfg, firm_key="Samsung")
    requests.packages.urllib3.disable_warnings()
    result = []
    parse_errors = 0
    for board_order, url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not url: continue
        try:
            resp = requests.get(url, headers=cfg["headers"], verify=False, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            print(f"[samsung] request failed board={board_order} {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select(cfg["item_sel"])
        print(f"[samsung] board={board_order} selector_matches={len(items)}", file=sys.stderr)
        for item in items:
            try:
                t_el = item.select_one(cfg["title_sel"])
                if not t_el: continue
                title = t_el.text.strip()
                a_href = item.a.get("href","").replace("javascript:downloadPdf(","").replace(")","").replace("'","")
                parts = a_href.split(",")
                if len(parts) < 3: continue
                path, report_date = parts[0].strip(), parts[2].strip().replace(";","")
                dl = cfg["url_tpl"].replace("{path}", path)
                author = "N/A"
                dds = item.select(cfg["author_sel"])
                if len(dds) > cfg["author_idx"]: author = dds[cfg["author_idx"]].text.strip()
                title = title.replace(f"({author})", "")
                result.append(dict(firm_id=cfg["firm_id"],board_id=board_order,
                    firm_nm=cfg["firm_nm"],report_date=report_date,telegram_url=dl,
                    article_title=title,writer=author,report_unique_key=dl,
                    save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception as exc:
                parse_errors += 1
                if parse_errors == 1:
                    print(f"[samsung] parse failed board={board_order} {type(exc).__name__}: {exc}", file=sys.stderr)
    if parse_errors:
        print(f"[samsung] skipped malformed rows={parse_errors}", file=sys.stderr)
    print(f"[samsung] {len(result)} articles collected", file=sys.stderr)
    return result
