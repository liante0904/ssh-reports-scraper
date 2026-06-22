import sys
"""NH Investment — config 기반 스크래핑 코어."""
import requests
from datetime import datetime, timezone, timedelta
from scrapers.config_guard import normalize_cfg, require_keys

DEFAULT_NHQV_CFG = {
    "url": "https://m.nhqv.com/research/commonTr.json",
    "headers": {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    },
    "payload": {
        "trName": "H3211",
        "rshPprDruTmSt": "00000000",
        "rshPprNo": ""
    },
    "count_path": "H3211.H3211OutBlock1.0.iqrCnt",
    "list_path": "H3211.H3211OutBlock2",
    "item_keys": {
        "pdf_url": "hpgeFleUrlCts",
        "reg_dt": "rshPprDruDtNm",
        "writer": "rshPprDruEmpFnm",
        "title": "rshPprTilCts"
    },
    "page_size": 11
}

def scrape_nhqv(cfg: dict, target_date: str = None) -> list[dict]:
    # backward compat: URL list → config dict
    cfg = normalize_cfg(cfg, firm_key="NHQV")

    if "url" not in cfg and "urls" in cfg:
        cfg["url"] = cfg["urls"][0] if cfg["urls"] else DEFAULT_NHQV_CFG["url"]

    # Merge default values if missing
    for k, v in DEFAULT_NHQV_CFG.items():
        if k not in cfg:
            cfg[k] = v
        elif isinstance(v, dict) and isinstance(cfg[k], dict):
            for sub_k, sub_v in v.items():
                if sub_k not in cfg[k]:
                    cfg[k][sub_k] = sub_v

    require_keys(
        cfg,
        ("url", "headers", "payload", "count_path", "list_path", "item_keys", "page_size"),
        firm_key="NHQV",
    )
    requests.packages.urllib3.disable_warnings()
    if target_date is None:
        KST = timezone(timedelta(hours=9)); now = datetime.now(KST)
        wd = now.weekday()
        if wd == 5: target_date = (now + timedelta(days=2)).strftime("%Y%m%d")
        elif wd == 6: target_date = (now + timedelta(days=1)).strftime("%Y%m%d")
        else: target_date = now.strftime("%Y%m%d")
    p = dict(cfg["payload"]); p["rshPprDruDtSt"] = target_date; p["rshPprDruDtEd"] = target_date
    result = []
    while True:
        resp = requests.post(cfg["url"], headers=cfg["headers"], data=p, timeout=30, verify=False)
        resp.raise_for_status(); jres = resp.json()
        ik = cfg["item_keys"]
        def _jp(path, d=jres):
            for k in path.split("."): d = d[int(k) if k.isdigit() else k]
            return d
        cnt = int(_jp(cfg["count_path"]))
        if cnt == 0: break
        for a in _jp(cfg["list_path"]):
            u = a.get(ik["pdf_url"])
            if not u: continue
            result.append(dict(sec_firm_order=2,article_board_order=0,firm_nm="NH투자증권",
                reg_dt=a[ik["reg_dt"]].replace(".",""),writer=a.get(ik["writer"],""),
                telegram_url=u,pdf_url=u,article_title=a[ik["title"]],
                key=u,report_unique_key=u,save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
        if cnt >= cfg["page_size"]: p["rshPprNo"] = _jp(cfg["list_path"])[-1]["rshPprNo"]
        else: break
    print(f"[nhqv] {len(result)} articles collected", file=sys.stderr)
    return result
