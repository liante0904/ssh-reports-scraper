import sys
"""Sangsangin Securities — config 기반."""
import re, requests, os
from datetime import datetime, timezone, timedelta

def scrape_sangsangin(cfg: dict) -> list[dict]:
    requests.packages.urllib3.disable_warnings()
    cookies = dict(cfg.get("cookies", {}))
    cookies["SSISTOCK_JSESSIONID"] = os.getenv("SANGSANGIN_JSESSIONID", cookies.get("SSISTOCK_JSESSIONID",""))
    result = []
    for idx, cms in enumerate(cfg["boards"]):
        p = dict(cfg["payload"]); p["cmsCd"] = cms
        try:
            resp = requests.post(cfg["url"], headers=cfg["headers"], data=p, cookies=cookies, timeout=30, verify=False)
            resp.raise_for_status()
            jres = resp.json()
            for k in cfg["list_key"].split("."): jres = jres[int(k) if k.isdigit() else k]
        except Exception: continue
        for item in (jres if isinstance(jres, list) else []):
            try:
                ik = cfg["item_keys"]; nt = item[ik["nt_no"]]
                dl = cfg["url_tpl"].replace("{cms}",cms).replace("{nt_no}",nt)
                result.append(dict(sec_firm_order=6,article_board_order=idx,firm_nm="상상인증권",
                    reg_dt=re.sub(r"[-./]","",item[ik["reg_dt"]]),download_url=dl,
                    telegram_url=dl,pdf_url=dl,key=dl,report_unique_key=dl,
                    article_title=item[ik["title"]],save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue
    print(f"[sangsangin] {len(result)} articles collected", file=sys.stderr)
    return result
