import sys
"""Mirae Asset Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from scrapers.legacy_url_config import normalize_legacy_url_config

def scrape_miraeasset(cfg: dict) -> list[dict]:
    cfg = normalize_legacy_url_config(cfg, firm_key="Miraeasset")
    requests.packages.urllib3.disable_warnings()
    result = []
    parse_errors = 0
    for idx, url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not url: continue
        try:
            resp = requests.get(url, timeout=30, verify=False)
            resp.raise_for_status()
        except Exception as exc:
            print(f"[miraeasset] request failed board={idx} {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select(cfg["row_sel"])[cfg.get("skip_rows",0):]
        print(f"[miraeasset] board={idx} selector_matches={len(rows)}", file=sys.stderr)
        for row in rows:
            try:
                rdt = re.sub(r"[-./]","",row.select_one(f"td:nth-child({cfg['cell_report_date']})").get_text(strip=True))
                title = row.select_one(f"td:nth-child({cfg['cell_title']})").get_text(strip=True)
                writer = row.select_one(f"td:nth-child({cfg['cell_writer']})").get_text(strip=True)
                dl = "없음"
                attach = row.select_one(cfg["attach_sel"])
                if attach:
                    m = re.search(cfg["attach_pattern"], attach["href"])
                    if m: dl = m.group(1)
                result.append(dict(firm_id=cfg["firm_id"],board_id=idx,
                    firm_nm=cfg["firm_nm"],report_date=rdt,writer=writer,telegram_url=dl,
                    article_title=title,save_at=datetime.now(timezone(timedelta(hours=9))).isoformat(),report_unique_key=dl))
            except Exception as exc:
                parse_errors += 1
                if parse_errors == 1:
                    print(f"[miraeasset] parse failed board={idx} {type(exc).__name__}: {exc}", file=sys.stderr)
    if parse_errors:
        print(f"[miraeasset] skipped malformed rows={parse_errors}", file=sys.stderr)
    print(f"[miraeasset] {len(result)} articles collected", file=sys.stderr)
    return result
