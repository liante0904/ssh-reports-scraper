import sys
"""Hyundai Motor Securities — config 기반."""
import time, requests
from datetime import datetime, timezone, timedelta
from scrapers.legacy_url_config import normalize_legacy_url_config

def scrape_hmsec(cfg: dict) -> list[dict]:
    cfg = normalize_legacy_url_config(cfg, firm_key="Hmsec")
    requests.packages.urllib3.disable_warnings()
    result = []
    parse_errors = 0
    for board_order, url in enumerate(cfg.get("urls", [cfg.get("url","")])):
        if not url: continue
        page, total_pages, max_p = 1, None, cfg.get("max_pages", 5)
        while page <= max_p:
            try:
                jres = requests.get(url, params={"curPage": page}, headers=cfg["headers"], timeout=30, verify=False).json()
            except Exception as exc:
                print(f"[hmsec] request failed board={board_order} page={page} {type(exc).__name__}: {exc}", file=sys.stderr)
                break
            items = jres.get(cfg["list_key"], [])
            print(f"[hmsec] board={board_order} page={page} api_items={len(items)}", file=sys.stderr)
            if not items: break
            if total_pages is None: total_pages = jres.get(cfg["paging_key"], {}).get("totalPages", 1)
            for item in items:
                try:
                    ik = cfg["item_keys"]; fn = item[ik["file"]]
                    dl = cfg["url_tpl"].replace("{file}", fn)
                    vu = cfg.get("viewer_tpl", dl).replace("{url}", dl)
                    result.append(dict(firm_id=9,board_id=board_order,firm_nm="현대차증권",
                        report_date=(item.get(ik["report_date"],"")).strip(),article_title=item[ik["title"]],
                        writer=(item.get(ik["writer"],"")).strip(),source_url=vu,pdf_file_url=dl,
                        telegram_url=vu,report_unique_key=vu,
                        save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception as exc:
                    parse_errors += 1
                    if parse_errors == 1:
                        print(f"[hmsec] parse failed board={board_order} page={page} {type(exc).__name__}: {exc}", file=sys.stderr)
            page += 1
            if total_pages and page > total_pages: break
            time.sleep(0.3)
    if parse_errors:
        print(f"[hmsec] skipped malformed rows={parse_errors}", file=sys.stderr)
    print(f"[hmsec] {len(result)} articles collected", file=sys.stderr)
    return result
