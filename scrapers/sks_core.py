import sys
"""SK Securities — 순수 스크래핑 코어."""
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup


def scrape_sks(urls: list[str]) -> list[dict]:
    requests.packages.urllib3.disable_warnings()
    result = []

    for board_order, target_url in enumerate(urls):
        try:
            resp = requests.get(target_url, timeout=30, verify=False)
            resp.raise_for_status()
        except Exception:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        for row in soup.select("table.board_list tbody tr"):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            link = cells[1].find("a")
            if not link:
                continue
            title = link.get_text(strip=True)
            reg_dt = cells[3].get_text(strip=True).replace(".", "").replace("-", "")
            writer = cells[4].get_text(strip=True)
            pdf_link = cells[5].find("a")
            download_url = ""
            if pdf_link:
                download_url = pdf_link.get("href", "")
                if download_url and not download_url.startswith("http"):
                    from urllib.parse import urljoin
                    download_url = urljoin(target_url, download_url)

            result.append({
                "sec_firm_order": 26, "article_board_order": board_order,
                "firm_nm": "SK증권", "reg_dt": reg_dt,
                "download_url": download_url, "telegram_url": download_url,
                "pdf_url": download_url, "article_title": title, "writer": writer,
                "save_time": datetime.now(timezone(timedelta(hours=9))).isoformat(),
            })

    print(f"[sks] {len(result)} articles collected", file=sys.stderr)
    return result
