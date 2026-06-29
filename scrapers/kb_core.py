import sys
"""KB Securities — config 기반 스크래핑 코어."""
import re, requests
from datetime import datetime, timezone, timedelta

DEFAULT_KB_CFG = {
    "url": "https://rc.kbsec.com/ajax/categoryReportList.json",
    "payload": {
        "pageNo": 1,
        "pageSize": 500,
        "templateid": "",
        "lowTempId": "",
        "folderid": "",
        "callGbn": "RCLIST"
    },
    "list_key": "response.reportList",
    "item_keys": {
        "cat_id": "pCategoryid",
        "doc_id": "documentid",
        "title": "docTitle",
        "subtitle": "docTitleSub",
        "reg_dt": "publicDate",
        "writer": "analystNm"
    },
    "url_tpl": "http://rdata.kbsec.com/pdf_data/{doc_id}.pdf",
    "global_cat": 26,
    "category_map": {}
}

def scrape_kb(cfg: dict, from_date: str = None, to_date: str = None) -> list[dict]:
    # backward compat: URL list → config dict
    if isinstance(cfg, list):
        url = cfg[0] if cfg else DEFAULT_KB_CFG["url"]
        cfg = {"url": url}
    elif isinstance(cfg, str):
        cfg = {"url": cfg}

    # Merge default values if missing
    for k, v in DEFAULT_KB_CFG.items():
        if k not in cfg:
            cfg[k] = v
        elif isinstance(v, dict) and isinstance(cfg[k], dict):
            for sub_k, sub_v in v.items():
                if sub_k not in cfg[k]:
                    cfg[k][sub_k] = sub_v

    requests.packages.urllib3.disable_warnings()
    if from_date is None: from_date = datetime(datetime.now(timezone(timedelta(hours=9))).year, 1, 1).strftime("%Y%m%d")
    if to_date is None: to_date = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    p = dict(cfg["payload"])
    p["registdateFrom"] = from_date; p["registdateTo"] = to_date
    resp = requests.post(cfg["url"], json=p, timeout=30, verify=False)
    resp.raise_for_status()
    jres = resp.json()
    for k in cfg["list_key"].split("."): jres = jres.get(k, [])
    items = jres if isinstance(jres, list) else []
    result = []
    for item in items:
        try:
            ik = cfg["item_keys"]; cat = item.get(ik["cat_id"], 0)
            board = cfg.get("category_map",{}).get(str(cat), 0)
            mkt = "GLOBAL" if cat == cfg.get("global_cat", -1) else "KR"
            doc_id = item.get(ik["doc_id"], "")
            dl = cfg["url_tpl"].replace("{doc_id}", str(doc_id))
            title = item.get(ik["title"], ""); sub = item.get(ik["subtitle"], "")
            if title and title not in sub: title = f"{title} : {sub}"
            elif sub: title = sub
            result.append(dict(sec_firm_order=4,article_board_order=board,firm_nm="KB증권",
                reg_dt=re.sub(r"[-./]","",str(item.get(ik["reg_dt"],""))),
                writer=item.get(ik["writer"],""),download_url=dl,telegram_url=dl,pdf_url=dl,
                article_title=title,mkt_tp=mkt,key=dl,report_unique_key=dl,
                save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
        except Exception: continue
    print(f"[kb] {len(result)} articles collected", file=sys.stderr)
    return result
