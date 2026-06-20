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
    # 2026.06 Hana 사이트 구조: li.mb4(a.more_btn) + li.mb7.m-info(span) + li.mb7.contn(body)
    cfg.setdefault("list_sel", "li.mb4:has(a.more_btn)")
    cfg.setdefault("title_sel", "a.more_btn")
    cfg.setdefault("url_sel", "div.pdf a[href*='download.cmd']")
    cfg.setdefault("date_sel", "li.mb7.m-info span.txtbasic:not(.r-side-bar)")
    cfg.setdefault("writer_sel", "li.mb7.m-info span.m-name")
    cfg.setdefault("time_sel", "li.mb7.m-info span.r-side-bar")
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
            # 2026.06 새 구조: title=li.mb4>a.more_btn, meta=li.mb7.m-info, dl=div.pdf a
            title_links = [a for a in soup.select("a.more_btn") if a.get_text(strip=True) != "더보기"]
            pdf_links = {a["href"]: a for a in soup.select("div.pdf a[href*='download.cmd']")}
            writers = soup.select("li.mb7.m-info span.m-name")
            dates = soup.select("li.mb7.m-info span.txtbasic:not(.r-side-bar)")
            times = soup.select("li.mb7.m-info span.r-side-bar")

            for i, a in enumerate(title_links):
                try:
                    title = a.get_text(strip=True)
                    # download 링크 찾기 (title과 같은 인덱스)
                    dl_keys = list(pdf_links.keys())
                    dl = cfg["base_url"] + dl_keys[i] if i < len(dl_keys) else ""
                    rd = dates[i].get_text(strip=True) if i < len(dates) else ""
                    rd = re.sub(r"[-./]","",rd)
                    writer = writers[i].get_text(strip=True) if i < len(writers) else ""
                    ts = times[i].get_text(strip=True) if i < len(times) else ""
                    mkt = "GLOBAL" if board_order in cfg.get("global_boards",[]) else "KR"
                    result.append(dict(sec_firm_order=cfg["sec_firm_order"],article_board_order=board_order,
                        firm_nm=cfg["firm_nm"],reg_dt=_adjust_date(rd,ts),download_url=dl,
                        telegram_url=dl,pdf_url=dl,article_title=title,writer=writer,
                        key=dl,report_unique_key=dl,mkt_tp=mkt,save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
    print(f"[hana] {len(result)} articles collected", file=sys.stderr)
    return result
