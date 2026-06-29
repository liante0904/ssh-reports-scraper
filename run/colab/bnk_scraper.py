#!/usr/bin/env python3
"""BNK Securities — Google Colab 전용 스크래퍼.

BNK증권은 서버 IP + GA IP 모두 차단됨 → Google Colab IP로 우회.

Colab 사용법:
  1. https://colab.research.google.com/ → 새 노트북
  2. 이 파일 붙여넣고 실행 (URL은 Secrets에 or 하드코딩)
  3. 결과 JSON 다운로드 → 서버 incoming/ga-scrapes/ 로 업로드
"""
import json
import os
import re
import sys
import requests
from datetime import datetime
from bs4 import BeautifulSoup

SEC_FIRM_ORDER = 23
FIRM_NM = "BNK투자증권"
URLS_ENV_KEY = "BNK_URLS_JSON"

# BNK 기본 URL (Colab에서 직접 써도 됨)
DEFAULT_URLS = [
    "https://www.bnkfn.co.kr/research/analysingCompany.jspx",
    "https://www.bnkfn.co.kr/research/analysingIssue.jspx",
    "https://www.bnkfn.co.kr/research/economyAnalyse.jspx",
    "https://www.bnkfn.co.kr/research/marketOverview2.jspx",
]


def scrape_bnk(urls: list[str]) -> list[dict]:
    requests.packages.urllib3.disable_warnings()
    headers = {
        "Accept": "*/*",
        "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    result = []

    for article_board_order, target_url in enumerate(urls):
        try:
            resp = requests.get(target_url, headers=headers, verify=False, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"[{FIRM_NM}] board {article_board_order} fetch failed: {e}", file=sys.stderr)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="table01")
        if not table:
            print(f"[{FIRM_NM}] board {article_board_order}: table.table01 not found", file=sys.stderr)
            continue

        rows = table.select("tbody tr")
        print(f"[{FIRM_NM}] board {article_board_order}: {len(rows)} rows", file=sys.stderr)

        for row in rows:
            try:
                cells = row.find_all("td")
                if len(cells) < 6:
                    continue

                article_link = cells[1].find("a")
                writer = cells[2].get_text(strip=True)
                if not article_link:
                    continue

                title = article_link.get_text(strip=True)
                reg_dt = cells[4].get_text(strip=True)

                # onclick에서 PDF URL 추출
                onclick = article_link.get("onclick", "")
                m = re.search(r"viewAction\(this, '\d+', '(/uploads/[^']+)', '([^']+)'\);", onclick)
                article_url = ""
                if m:
                    article_url = f"https://www.bnkfn.co.kr{m.group(1)}/{m.group(2)}"

                result.append({
                    "sec_firm_order": SEC_FIRM_ORDER,
                    "article_board_order": article_board_order,
                    "firm_nm": FIRM_NM,
                    "reg_dt": re.sub(r"[-./]", "", reg_dt),
                    "article_title": title,
                    "article_url": article_url,
                    "download_url": article_url,
                    "telegram_url": article_url,
                    "pdf_url": article_url,
                    "writer": writer,
                    "key": article_url,
                    "report_unique_key": article_url,
                    "save_time": datetime.now().isoformat(),
                })
            except Exception:
                continue

    return result


if __name__ == "__main__":
    raw = os.getenv(URLS_ENV_KEY, "")
    urls = json.loads(raw) if raw else DEFAULT_URLS
    if not urls:
        print(f"[{FIRM_NM}] FATAL: no URLs", file=sys.stderr)
        sys.exit(1)

    print(f"[{FIRM_NM}] Google Colab mode — {len(urls)} boards", file=sys.stderr)
    result = scrape_bnk(urls)
    print(f"[{FIRM_NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
