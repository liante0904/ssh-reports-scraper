import sys
"""Hyundai Motor Securities — config 기반."""
import time, requests
from datetime import datetime, timezone, timedelta

def scrape_hmsec(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, url in enumerate(cfg.get("urls", [cfg.get("url","")])):
        if not url: continue
        page, total_pages, max_p = 1, None, cfg.get("max_pages", 5)
        while page <= max_p:
            try:
                jres = requests.get(url, params={"curPage": page}, headers=cfg["headers"], timeout=30, verify=False).json()
            except Exception: break
            items = jres.get(cfg["list_key"], [])
            if not items: break
            if total_pages is None: total_pages = jres.get(cfg["paging_key"], {}).get("totalPages", 1)
            for item in items:
                try:
                    ik = cfg["item_keys"]; fn = item[ik["file"]]
                    dl = cfg["url_tpl"].replace("{file}", fn)
                    vu = cfg.get("viewer_tpl", dl).replace("{url}", dl)
                    result.append(dict(sec_firm_order=9,article_board_order=board_order,firm_nm="현대차증권",
                        reg_dt=(item.get(ik["reg_dt"],"")).strip(),article_title=item[ik["title"]],
                        writer=(item.get(ik["writer"],"")).strip(),article_url=vu,pdf_url=dl,
                        download_url=dl,telegram_url=vu,key=vu,report_unique_key=vu,
                        save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
            page += 1
            if total_pages and page > total_pages: break
            time.sleep(0.3)
    print(f"[hmsec] {len(result)} articles collected", file=sys.stderr)
    return result
