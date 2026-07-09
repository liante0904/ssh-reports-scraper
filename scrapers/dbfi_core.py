import sys
"""DB Financial Investment — config 기반."""
import re, requests
from datetime import datetime, timezone, timedelta

def _first_value(row: dict, keys: list[str]) -> str:
    for key in keys:
        if key and row.get(key) is not None:
            return str(row.get(key, ""))
    return ""

def scrape_dbfi(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    cfg.setdefault("firm_id", 19)
    cfg.setdefault("firm_nm", "DB증권")
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
            report_date = _first_value(row, [
                ik.get("report_date"),
                ik.get("reg_dt"),
                ik.get("rdt"),
                "report_date",
                "reg_dt",
                "rdt",
            ])
            key = cfg["key_tpl"].replace("{base}",cfg["base_url"]).replace("{rid}",rid)
            result.append(dict(firm_id=cfg["firm_id"],board_id=board_order,firm_nm=cfg["firm_nm"],
                report_date=re.sub(r"[^0-9]","",report_date)[:8],telegram_url="",pdf_file_url="",
                article_title=row[ik["title"]],writer=row[ik["writer"]],
                report_unique_key=key,save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
    print(f"[dbfi] {len(result)} articles collected", file=sys.stderr)
    return result
