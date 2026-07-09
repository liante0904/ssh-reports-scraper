import sys
"""Hanwha Securities — config 기반 XML 파싱."""
import os, re, requests, urllib.parse
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET

def scrape_hanwha(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    # 기본값 (GA standalone에서 list만 전달되는 경우 대비)
    cfg.setdefault("headers", {"User-Agent": "Mozilla/5.0"})
    cfg.setdefault("item_keys", {})
    cfg.setdefault("url_tpl", "")
    # env override for backfill — hard-override, not setdefault (config may embed max_pages)
    if "HANWHA_MAX_PAGES" in os.environ:
        cfg["max_pages"] = int(os.environ["HANWHA_MAX_PAGES"])
    else:
        cfg.setdefault("max_pages", 50)
    cfg.setdefault("page_size", 100)
    cfg.setdefault("firm_id", 21)
    cfg.setdefault("firm_nm", "한화투자증권")
    cfg.setdefault("xml_item_tag", "item")
    requests.packages.urllib3.disable_warnings()
    result = []
    base_url = cfg.get("urls",[cfg.get("url","")])[0] if isinstance(cfg.get("urls"),list) else cfg.get("url","")
    if not base_url: return result
    for page_val in range(1, cfg.get("max_pages",50)+1):
        params = {"pageSize":cfg.get("page_size",100),"mode":"depth2","ch_gbn":"iOS","pageVal":page_val}
        full_url = f"{base_url}?{urllib.parse.urlencode(params)}"
        try:
            resp = requests.get(full_url, headers=cfg["headers"], verify=False, timeout=30)
            if resp.status_code != 200: break
            root = ET.fromstring(resp.text)
        except Exception: break
        blocks = root.findall(f".//{cfg['xml_item_tag']}")
        if not blocks: break
        for block in blocks:
            try:
                ik = cfg["item_keys"]
                rd = (block.find(ik["date"]).text or "").replace("-","").replace(".","").replace("/","")
                depth3 = block.find(ik["depth3"]).text or ""
                title = block.find(ik["title"]).text or "No Title"
                writer = block.find(ik["writer"]).text or "Unknown"
                fn = block.find(ik["file"]).text or ""
                sn = block.find(ik["store"]).text or ""
                dp = block.find(ik["dir"]).text or ""
                mkt = "GLOBAL" if depth3 == cfg.get("global_depth3","") else "KR"
                dl = ""
                if fn and sn and dp:
                    dl = cfg["url_tpl"].replace("{file}",urllib.parse.quote(fn)).replace("{store}",urllib.parse.quote(sn)).replace("{dir}",urllib.parse.quote(dp))
                if not dl:
                    continue
                result.append(dict(firm_id=cfg["firm_id"],board_id=0,
                    firm_nm=cfg["firm_nm"],report_date=re.sub(r"[-./]","",rd),
                    article_title=title,writer=writer,mkt_tp=mkt,report_unique_key=dl,
                    telegram_url=dl,pdf_file_url=dl,save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue
    print(f"[hanwha] {len(result)} articles collected", file=sys.stderr)
    return result
