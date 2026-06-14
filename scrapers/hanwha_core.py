import sys
"""Hanwha Securities — config 기반 XML 파싱."""
import re, requests, urllib.parse
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET

def scrape_hanwha(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
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
                result.append(dict(sec_firm_order=cfg["sec_firm_order"],article_board_order=0,
                    firm_nm=cfg["firm_nm"],reg_dt=re.sub(r"[-./]","",rd),download_url=dl,
                    article_title=title,writer=writer,mkt_tp=mkt,key=dl,report_unique_key=dl,
                    telegram_url=dl,pdf_url=dl,save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue
    print(f"[hanwha] {len(result)} articles collected", file=sys.stderr)
    return result
