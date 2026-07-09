import sys
"""Kiwoom Securities — config 기반."""
import re, requests
from datetime import datetime, timezone, timedelta

def scrape_kiwoom(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    now = datetime.now(timezone(timedelta(hours=9)))
    p = dict(cfg["payload"])
    p["stdate"] = p.get("stdate","{year}0101").replace("{year}0101",f"{now.year}0101")
    p["eddate"] = p.get("eddate","{today}").replace("{today}",now.strftime("%Y%m%d"))
    result = []
    for board_order, url in enumerate(cfg.get("urls", [cfg.get("url","")])):
        if not url: continue
        h = dict(cfg["headers"]); h["Referrer"] = url
        try:
            resp = requests.post(url, headers=h, data=p, verify=False, timeout=30)
            resp.raise_for_status()
            items = resp.json().get(cfg["list_key"], [])
        except Exception: continue
        for item in items:
            try:
                ik = cfg["item_keys"]
                dl = cfg["url_tpl"].replace("{menu_gb}",item.get(ik["menu_gb"],"")).replace("{atta_file}",item.get(ik["atta_file"],"")).replace("{report_date}",item.get(ik["report_date"],""))
                result.append(dict(firm_id=10,board_id=board_order,firm_nm="키움증권",
                    report_date=re.sub(r"[-./]","",item[ik["report_date"]]),article_title=item[ik["title"]],
                    writer=item.get(ik["writer"],""),telegram_url=dl,pdf_file_url=dl,report_unique_key=dl,
                    save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue
    print(f"[kiwoom] {len(result)} articles collected", file=sys.stderr)
    return result
