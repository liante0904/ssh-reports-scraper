import sys
"""Eugene Securities — 순수 스크래핑 코어."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

def scrape_eugene(url: str) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    result = []
    # eugenefn API call - single URL list fetch
    try:
        resp = requests.get(url, headers=headers, timeout=30, verify=False)
        resp.raise_for_status()
    except Exception:
        return result
    soup = BeautifulSoup(resp.text, "html.parser")
    for row in soup.select("table.board_list tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 5: continue
        title_cell = cells[1].find("a")
        if not title_cell: continue
        title = title_cell.get_text(strip=True)
        source_url = title_cell.get("href","")
        if source_url and not source_url.startswith("http"):
            source_url = "https://www.eugenefn.com" + source_url
        report_date = re.sub(r"[-./]","",cells[3].get_text(strip=True))
        writer = cells[4].get_text(strip=True) if len(cells) > 4 else ""
        # 2026.06.21 fix: GA Import 중복제거 및 DB 업서트 시 식별값으로 사용될 key, report_unique_key 설정 추가
        result.append({"firm_id":12,"board_id":0,
            "firm_nm":"유진투자증권","report_date":report_date,"article_title":title,
            "telegram_url":source_url,
            "report_unique_key":source_url,
            "save_at":datetime.now(timezone(timedelta(hours=9))).isoformat()})
    print(f"[eugene] {len(result)} articles collected", file=sys.stderr)
    return result
