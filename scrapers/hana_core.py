import sys
"""Hana Securities — config 기반 HTML 파싱."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def _adjust_date(report_date, time_str):
    reg_date = datetime.strptime(report_date, "%Y%m%d")
    m = re.match(r"(오전|오후)?\s*(\d{1,2}):(\d{2})", time_str.strip())
    if not m: return report_date
    period, hour, minute = m.groups(); hour = int(hour)
    if period == "오후" and hour != 12: hour += 12
    elif period == "오전" and hour == 12: hour = 0
    reg_date += timedelta(hours=hour, minutes=int(minute))
    # 2026-07-31 fix: 다음 영업일 cutoff를 10시 → 17시로 수정.
    # 장 마감(15:30) 이후 등록된 리포트는 익영업일 날짜를 부여.
    # 기존 hour>=10 은 오전 리포트까지 익일로 밀어내는 버그.
    # 17시 이후 → +1일 → 주말이면 월요일로 스킵.
    if reg_date.hour >= 17: reg_date += timedelta(days=1)
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
    cfg.setdefault("firm_id", 3)
    cfg.setdefault("firm_nm", "하나증권")
    cfg.setdefault("global_boards", [])
    cfg.setdefault("request_timeout", 15)  # GA 환경 빠른 실패를 위해 축소
    requests.packages.urllib3.disable_warnings()
    result = []
    url_count = len(cfg.get("urls", [cfg.get("url","")]))
    fail_streak = 0   # 연속 실패 카운터 — 조기 abort
    aborted = False   # double-break 플래그
    for board_order, base_url in enumerate(cfg.get("urls", [cfg.get("url","")])):
        if not base_url or aborted: continue
        for page in range(1, 2):  # 1페이지만 (새 구조에서 충분)
            url = f"{base_url}&curPage={page}"
            try:
                resp = requests.get(url, timeout=cfg["request_timeout"], verify=False)
                resp.raise_for_status()
            except Exception as e:
                fail_streak += 1
                print(f"[hana] FAIL [{board_order}] {type(e).__name__}: {url}", file=sys.stderr)
                # 연속 2개 URL 실패 → 네트워크 차단으로 간주, 남은 URL 스킵
                if fail_streak >= 2:
                    print(f"[hana] ABORT: {fail_streak} consecutive failures, skipping remaining {url_count - board_order - 1} URLs", file=sys.stderr)
                    aborted = True
                    break
                continue
            fail_streak = 0  # 성공 시 리셋
            soup = BeautifulSoup(resp.text, "html.parser")
            # 2026.06 새 구조: title=li.mb4>a.more_btn, meta=li.mb7.m-info, dl=div.pdf a
            title_links = [a for a in soup.select("a.more_btn") if a.get_text(strip=True) != "더보기"]
            if not title_links: break  # 빈 페이지 → 다음 board로
            pdf_links = {a["href"]: a for a in soup.select("div.pdf a[href*='download.cmd']")}
            writers = soup.select("li.mb7.m-info span.m-name")
            dates = soup.select("li.mb7.m-info span.txtbasic:not(.r-side-bar)")
            times = soup.select("li.mb7.m-info span.r-side-bar")
            # 2026.06 "더보기" 요약 텍스트 — li.mb7.contn (title_links와 1:1 인덱스 매칭)
            contns = soup.select("li.mb7.contn")

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
                    article_text = contns[i].get_text(strip=True) if i < len(contns) else ""
                    mkt = "GLOBAL" if board_order in cfg.get("global_boards",[]) else "KR"
                    result.append(dict(firm_id=cfg["firm_id"],board_id=board_order,
                        firm_nm=cfg["firm_nm"],report_date=_adjust_date(rd,ts),
                        telegram_url=dl,pdf_file_url=dl,article_title=title,writer=writer,
                        article_text=article_text,
                        report_unique_key=dl,mkt_tp=mkt,save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
    print(f"[hana] {len(result)} articles collected", file=sys.stderr)
    return result
