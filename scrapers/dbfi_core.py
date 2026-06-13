import sys
"""DB Financial Investment — config 기반."""
import re, requests
from datetime import datetime, timezone, timedelta

def scrape_dbfi(cfg: dict) -> list[dict]:
    requests.packages.urllib3.disable_warnings()
    result = []
    for item in cfg["url_paths"]:
        url_path, board_order = (item[0], item[1]) if isinstance(item,(list,tuple)) and len(item)>=2 else (item,0) if isinstance(item,str) else (None,0)
        if not url_path: continue
        h = {**cfg["headers"], "Referer": f"{cfg['base_url']}/mre/mre_CompanyAll_lst.do", "Accept":"application/json, text/javascript, */*; q=0.01"}
        try:
            resp = requests.post(cfg["base_url"] + url_path, headers=h, timeout=30, verify=False)
            resp.raise_for_status()
            items = resp.json().get(cfg["list_key"], [])[:50]
        except Exception: continue
        for row in items:
            ik = cfg["item_keys"]; rid = row[ik["rid"]]
            key = cfg["key_tpl"].replace("{base}",cfg["base_url"]).replace("{rid}",rid)
            result.append(dict(sec_firm_order=19,article_board_order=board_order,firm_nm="DB증권",
                reg_dt=row[ik["reg_dt"]][:8],article_url="",telegram_url="",pdf_url="",
                article_title=row[ik["title"]],writer=row[ik["writer"]],
                key=key,report_unique_key=key,save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
    print(f"[dbfi] {len(result)} articles collected", file=sys.stderr)
    return result
