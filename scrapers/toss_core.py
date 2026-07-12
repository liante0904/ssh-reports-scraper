import sys
"""TOSS Securities — config 기반."""
import re, requests
from datetime import datetime, timezone, timedelta

def scrape_toss(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    result = []
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
                except Exception: continue
            page += 1
            if total_pages and page >= total_pages: break
    print(f"[toss] {len(result)} articles collected", file=sys.stderr)
    return result
