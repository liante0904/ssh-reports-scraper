import sys
"""TOSS Securities — config 기반."""
import re, requests
from datetime import datetime, timezone, timedelta
from scrapers.legacy_url_config import normalize_legacy_url_config

def scrape_toss(cfg: dict) -> list[dict]:
    cfg = normalize_legacy_url_config(cfg, firm_key="Toss")
    requests.packages.urllib3.disable_warnings()
    result = []
    parse_errors = 0
    for board_order, base_url in enumerate(cfg.get("urls", [cfg.get("url","")])):
        if not base_url: continue
        page, total_pages = 0, None
        while True:
            purl = re.sub(r"page=\d+", f"page={page}", base_url)
            if "page=" not in purl: purl += ("&" if "?" in purl else "?") + f"page={page}"
            try:
                resp = requests.get(purl, headers=cfg["headers"], verify=False, timeout=30)
                resp.raise_for_status(); jres = resp.json()
            except Exception as exc:
                print(f"[toss] request failed page={page} {type(exc).__name__}: {exc}", file=sys.stderr)
                break
            items = jres.get("result", {}).get("list", [])
            print(f"[toss] board={board_order} page={page} api_items={len(items)}", file=sys.stderr)
            if not items: break
            if total_pages is None: total_pages = jres.get("result", {}).get("pagingParam", {}).get("totalPageCount", 1)
            for item in items:
                try:
                    ik = cfg["item_keys"]
                    title = item.get(ik["title"], ""); report_date = item.get(ik["report_date"], "").split("T")[0]
                    writer = item.get(ik["writer"], "")
                    if not writer:
                        m = re.search(r"작성자[:\s]*([^<\n]+)", item.get(ik.get("contents",""), ""))
                        if m: writer = m.group(1).strip()
                    dl = ""
                    if item.get(ik.get("files","")):
                        dl = item[ik["files"]][0].get("filePath", "")
                    if not dl: dl = item.get("contentImage", "")
                    cat = item.get(ik.get("category",""), {}).get("categoryName", "")
                    mkt = "GLOBAL" if cfg.get("global_keyword","") in cat.lower() else "KR"
                    result.append(dict(firm_id=15,board_id=board_order,firm_nm="토스증권",
                        report_date=re.sub(r"[-./]","",report_date),telegram_url=dl,
                        article_title=title,writer=writer,mkt_tp=mkt,report_unique_key=dl,
                        save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception as exc:
                    parse_errors += 1
                    if parse_errors == 1:
                        print(f"[toss] parse failed board={board_order} page={page} {type(exc).__name__}: {exc}", file=sys.stderr)
            page += 1
            if total_pages and page >= total_pages: break
    if parse_errors:
        print(f"[toss] skipped malformed rows={parse_errors}", file=sys.stderr)
    print(f"[toss] {len(result)} articles collected", file=sys.stderr)
    return result
