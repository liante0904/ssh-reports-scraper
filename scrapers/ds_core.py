"""DS Securities — 순수 스크래핑 코어."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def scrape_ds(urls: list[str]) -> list[dict]:
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
            reg_dt = re.sub(r"[-./]","",cells[3].get_text(strip=True))
            writer = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            pdf_cell = cells[5].find("a") if len(cells) > 5 else None
            download_url = urljoin(url, pdf_cell.get("href","")) if pdf_cell else article_url
            result.append({"sec_firm_order":11,"article_board_order":board_order,
                "firm_nm":"DS투자증권","reg_dt":reg_dt,"article_title":title,
                "download_url":download_url,"telegram_url":download_url,"pdf_url":download_url,
                "writer":writer,"key":download_url,"report_unique_key":download_url,
                "save_time":datetime.now(timezone(timedelta(hours=9))).isoformat()})
    print(f"[ds] {len(result)} articles collected", file=sys.stderr)
    return result
