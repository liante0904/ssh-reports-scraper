import sys
"""Hana Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def _adjust_date(reg_dt, time_str):
    reg_date = datetime.strptime(reg_dt, "%Y%m%d")
    m = re.match(r"(오전|오후)?\s*(\d{1,2}):(\d{2})", time_str.strip())
    if not m: return reg_dt
    period, hour, minute = m.groups(); hour = int(hour)
    if period == "오후" and hour != 12: hour += 12
    elif period == "오전" and hour == 12: hour = 0
    reg_date += timedelta(hours=hour, minutes=int(minute))
    if reg_date.hour >= 10: reg_date += timedelta(days=1)
    while reg_date.weekday() >= 5: reg_date += timedelta(days=1)
    return reg_date.strftime("%Y%m%d")

def scrape_hana(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    # 기본값 (GA standalone에서 list만 전달되는 경우 대비)
    cfg.setdefault("list_sel", "#rschPprList > ul > li")
    cfg.setdefault("title_sel", ".titArea strong")
    cfg.setdefault("url_sel", ".fileArea a")
    cfg.setdefault("date_sel", ".dateArea .date")
    cfg.setdefault("writer_sel", ".writerArea .writer")
    cfg.setdefault("time_sel", ".dateArea .time")
    cfg.setdefault("base_url", "https://www.hanaw.com")
    cfg.setdefault("sec_firm_order", 3)
    cfg.setdefault("firm_nm", "하나증권")
    cfg.setdefault("global_boards", [])
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, base_url in enumerate(cfg.get("urls", [cfg.get("url","")])):
        if not base_url: continue
        for page in range(1, 4):
            url = f"{base_url}&curPage={page}"
            try:
                resp = requests.get(url, timeout=30, verify=False)
                resp.raise_for_status()
            except Exception: break
            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.select(cfg["list_sel"])
            if not items: break
            for item in items:
                try:
                    title = item.select_one(cfg["title_sel"]).get_text()
                    dl = cfg["base_url"] + item.select_one(cfg["url_sel"])["href"]
                    rd = item.select_one(cfg["date_sel"]).get_text()
                    rd = re.sub(r"[-./]","",rd)
                    writer = item.select_one(cfg["writer_sel"]).get_text()
                    ts = item.select_one(cfg["time_sel"]).get_text()
                    mkt = "GLOBAL" if board_order in cfg.get("global_boards",[]) else "KR"
                    result.append(dict(sec_firm_order=cfg["sec_firm_order"],article_board_order=board_order,
                        firm_nm=cfg["firm_nm"],reg_dt=_adjust_date(rd,ts),download_url=dl,
                        telegram_url=dl,pdf_url=dl,article_title=title,writer=writer,
                        key=dl,report_unique_key=dl,mkt_tp=mkt,save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
    print(f"[hana] {len(result)} articles collected", file=sys.stderr)
    return result
