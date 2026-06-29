#!/usr/bin/env python3
"""
BNK투자증권 스크래퍼 - 진단 강화 버전
- DNS resolution, TCP connection, TLS, HTTP response 각 단계별 진단
- traceback 전체 출력
"""

import asyncio
import json
import re
import sys
import traceback
import os
import socket
import ssl
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config

BNK_URLS = config.get_urls("BNKfn_23")
if not BNK_URLS:
    raise RuntimeError("Missing BNKfn_23 URLs. Populate ~/secrets/ssh-reports-scraper/secrets.json first.")

HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

BOARD_NAMES = {
    0: "기업분석",
    1: "이슈분석",
    2: "경제분석",
    3: "시장동향",
}


def diagnose_host(host: str, port: int = 443) -> dict:
    """호스트 진단: DNS -> TCP -> TLS 단계별 확인"""
    result = {"host": host, "port": port}

    # 1. DNS resolution
    try:
        ip = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        result["dns"] = f"OK ({ip[0][4][0]})"
    except Exception as e:
        result["dns"] = f"FAIL: {e}"
        return result

    # 2. TCP connection
    import time
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    t0 = time.time()
    try:
        sock.connect((host, port))
        result["tcp"] = f"OK ({time.time() - t0:.2f}s)"
    except Exception as e:
        result["tcp"] = f"FAIL ({time.time() - t0:.2f}s): {type(e).__name__}: {e}"
        sock.close()
        return result
    sock.close()

    # 3. TLS handshake (basic)
    try:
        ctx = ssl.create_default_context()
        sock = ctx.wrap_socket(socket.socket(socket.AF_INET), server_hostname=host)
        sock.settimeout(10)
        t0 = time.time()
        sock.connect((host, port))
        result["tls"] = f"OK ({time.time() - t0:.2f}s), cert issued to: {sock.getpeercert().get('subject', 'N/A')}"
        sock.close()
    except Exception as e:
        result["tls"] = f"FAIL: {type(e).__name__}: {e}"

    return result


async def fetch_url_verbose(session: aiohttp.ClientSession, url: str, retries: int = 3) -> BeautifulSoup | None:
    """URL을 가져오고 상세 에러 로깅"""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30, connect=15)) as resp:
                print(f"  [DEBUG] HTTP {resp.status} {resp.reason} for {url}", file=sys.stderr)
                resp.raise_for_status()
                html = await resp.text()
                print(f"  [DEBUG] Got {len(html)} bytes from {url}", file=sys.stderr)
                return BeautifulSoup(html, "html.parser")
        except aiohttp.ClientConnectorError as e:
            last_error = e
            print(f"  [DEBUG] Connection error (attempt {attempt}/{retries}): {type(e).__name__}: {e}", file=sys.stderr)
        except aiohttp.ClientError as e:
            last_error = e
            print(f"  [DEBUG] Client error (attempt {attempt}/{retries}): {type(e).__name__}: {e}", file=sys.stderr)
        except asyncio.TimeoutError as e:
            last_error = e
            print(f"  [DEBUG] Timeout (attempt {attempt}/{retries}): {type(e).__name__}", file=sys.stderr)
        except Exception as e:
            last_error = e
            print(f"  [DEBUG] Unexpected error (attempt {attempt}/{retries}): {type(e).__name__}: {e}", file=sys.stderr)

        if attempt < retries:
            await asyncio.sleep(2 * attempt)

    print(f"  [ERROR] All {retries} attempts failed for {url}", file=sys.stderr)
    if last_error:
        traceback.print_exception(type(last_error), last_error, last_error.__traceback__, file=sys.stderr)
    return None


async def scrape_bnk() -> list[dict]:
    articles: list[dict] = []

    # TCPConnector with ssl=False to debug TLS issues
    connector = aiohttp.TCPConnector(ssl=False, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_url_verbose(session, url) for url in BNK_URLS]
        soups = await asyncio.gather(*tasks, return_exceptions=True)

        for board_order, (soup, url) in enumerate(zip(soups, BNK_URLS)):
            if isinstance(soup, Exception) or soup is None:
                print(f"[WARN] Skipping board {board_order} ({url}): {type(soup).__name__}", file=sys.stderr)
                continue

            table = soup.find("table", class_="table01")
            if not table:
                print(f"[WARN] Table not found in {url}", file=sys.stderr)
                continue

            rows = table.select("tbody tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 6:
                    continue
                article_link = cells[1].find("a")
                writer = cells[2].get_text(strip=True)
                if not article_link:
                    continue
                article_title = article_link.get_text(strip=True)
                onclick_attr = article_link.get("onclick", "")
                match = re.search(
                    r"viewAction\(this, '\d+', '(/uploads/[^']+)', '([^']+)'\);",
                    onclick_attr,
                )
                article_url = ""
                if match:
                    article_url = f"https://www.bnkfn.co.kr{match.group(1)}/{match.group(2)}"
                reg_dt = cells[4].get_text(strip=True)
                articles.append({
                    "sec_firm_order": 23,
                    "article_board_order": board_order,
                    "board_name": BOARD_NAMES.get(board_order, f"board_{board_order}"),
                    "firm_nm": "BNK투자증권",
                    "reg_dt": re.sub(r"[-./]", "", reg_dt),
                    "article_title": article_title,
                    "article_url": article_url,
                    "download_url": article_url,
                    "telegram_url": article_url,
                    "writer": writer,
                    "save_time": datetime.now().isoformat(),
                    "key": article_url,
                })

    return articles


def main():
    print("[INFO] Starting BNK scraper diagnostics...", file=sys.stderr)

    # DNS/TCP/TLS 진단
    print("\n=== DNS/TCP/TLS Diagnostics ===", file=sys.stderr)
    for url in BNK_URLS:
        from urllib.parse import urlparse
        host = urlparse(url).hostname
        result = diagnose_host(host)
        print(f"  {host}: DNS={result.get('dns','?')} TCP={result.get('tcp','?')} TLS={result.get('tls','?')}", file=sys.stderr)

    # 스크래핑 시도
    print("\n=== Scraping Attempt ===", file=sys.stderr)
    articles = asyncio.run(scrape_bnk())
    print(f"\n[INFO] Scraped {len(articles)} articles", file=sys.stderr)

    output = {
        "scraped_at": datetime.now().isoformat(),
        "source": "github-actions",
        "count": len(articles),
        "articles": articles,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
