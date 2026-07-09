import sys
"""DS Securities — 순수 스크래핑 코어."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def scrape_ds(urls: list[str]) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    result = []
    for board_order, url in enumerate(urls):
        try:
            resp = requests.get(url, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
        except Exception:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for row in soup.select("table tbody tr"):
            cells = row.find_all("td")
            if len(cells) < 5: continue
            title_cell = cells[1].find("a")
            if not title_cell: continue
            title = title_cell.get_text(strip=True)
            article_url = urljoin(url, title_cell.get("href",""))
            report_date = re.sub(r"[-./]","",cells[3].get_text(strip=True))
            writer = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            pdf_cell = cells[5].find("a") if len(cells) > 5 else None
            download_url = urljoin(url, pdf_cell.get("href","")) if pdf_cell else article_url
            # 2026.06.21 fix: GA Import 중복제거 및 DB 업서트 시 식별값으로 사용될 key, report_unique_key 설정 추가
            result.append({"firm_id":11,"board_id":board_order,
                "firm_nm":"DS투자증권","report_date":report_date,"article_title":title,
                "telegram_url":download_url,"pdf_file_url":download_url,
                "report_unique_key":download_url,
                "save_at":datetime.now(timezone(timedelta(hours=9))).isoformat()})
    print(f"[ds] {len(result)} articles collected", file=sys.stderr)
    return result
