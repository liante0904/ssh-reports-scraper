import sys
"""IBK Securities — config 기반."""
import re, requests
from datetime import datetime, timezone, timedelta

def scrape_ibk(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    result = []
    for idx, board in enumerate(cfg["boards"]):
        h = dict(cfg["headers"]); h["Referer"] = f"https://m.ibks.com/iko/{board['screen']}.do"
        p = {"screen":board["screen"],"data":{"start_row":1,"end_row":50,"row_size":50,"pageNo":1,"search_value":""}}
        if "menu_tp" in board: p["data"]["menu_tp"] = board["menu_tp"]
        try:
            resp = requests.post(cfg.get("url", f"https://m.ibks.com/iko/{board['screen']}.do"), headers=h, json=p, timeout=30, verify=False)
            resp.raise_for_status()
            jres = resp.json()
            for k in cfg["list_key"].split("."): jres = jres.get(k, [])
        except Exception: continue
        for item in (jres if isinstance(jres, list) else []):
            try:
                ik = cfg["item_keys"]; fn = item.get(ik["file"],"")
                path = "invrespect" if idx == 0 else board["path"]
                dl = cfg["url_tpl"].replace("{path}",path).replace("{file}",fn)
                mkt = "GLOBAL" if board["name"] in cfg.get("global_boards",[]) else "KR"
                result.append(dict(sec_firm_order=25,article_board_order=idx,firm_nm="IBK투자증권",
                    reg_dt=re.sub(r"[-./]","",item.get(ik["reg_dt"],"")),download_url=dl,
                    article_title=item.get(ik["title"],"").strip(),writer=item.get(ik["writer"],"").strip(),
                    telegram_url=dl,pdf_url=dl,key=dl,report_unique_key=dl,mkt_tp=mkt,
                    save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue
    print(f"[ibk] {len(result)} articles collected", file=sys.stderr)
    return result
