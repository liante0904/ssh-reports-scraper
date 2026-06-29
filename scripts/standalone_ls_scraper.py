#!/usr/bin/env python3
"""
LS증권 Standalone Scraper — GitHub Actions 전용
================================================
- LS증권 목록 2페이지 + 상세 페이지 스크래핑
- DB 접근 없이 순수 HTTP 크롤링만 수행
- 결과는 JSON으로 stdout 출력 (artifact 업로드용)

서버 측 import 스크립트(import_ls_artifact.py)가 이 JSON을 받아
DB insert + msg URL 복구(reconstruct_msg_url_from_db) 등
DB 의존적 후처리를 담당한다.

사용법:
  python3 scripts/standalone_ls_scraper.py [--max-pages 2] [--no-detail]
"""

import argparse
import asyncio
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import aiohttp
import requests
from bs4 import BeautifulSoup

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
SEC_FIRM_ORDER = 0
FIRM_NM = "LS증권"

BOARD_URLS = config.get_urls("LS_0")
if not BOARD_URLS:
    raise RuntimeError("Missing LS_0 URLs. Populate ~/secrets/ssh-reports-scraper/secrets.json first.")

BOARD_NAMES = [
    "기업분석", "산업분석", "투자전략", "Quant", "Credit",
    "해외주식", "시황", "Small cap", "계량분석", "파생",
]
LS_PUBLIC_ORIGIN = os.getenv("LS_PUBLIC_ORIGIN", "https://www.ls-sec.co.kr")
LS_HOME_URL = f"{LS_PUBLIC_ORIGIN}/"
LS_FRONT_BOARD_PREFIX = f"{LS_PUBLIC_ORIGIN}/EtwFrontBoard/"
LS_UPLOAD_PREFIX = f"{LS_PUBLIC_ORIGIN}/upload/EtwBoardData"
LS_MSG_ORIGIN = os.getenv("LS_MSG_ORIGIN", "https://msg.ls-sec.co.kr")
LS_MSG_PREFIX = f"{LS_MSG_ORIGIN}/"
LS_MSG_EUM_PREFIX = f"{LS_MSG_ORIGIN}/eum"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": LS_HOME_URL,
}

DETAIL_HEADERS = {
    **HEADERS,
    "Connection": "keep-alive",
}

# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------

def clean_url(url: str) -> str:
    """board_no, board_seq 필수 파라미터만 남기고 정리"""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    required = {k: qs.get(k, [""])[0] for k in ("board_no", "board_seq")}
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(required), ""))


def upload_filename_to_cdn_url(filename: str) -> Optional[str]:
    """
    upload filename → msg.ls-sec.co.kr CDN URL 변환
    패턴: {emp_id}_{seq}_{date}.ext → K_{date}_{emp_id}_{seq}.pdf
    """
    basename = os.path.basename(filename)
    m = re.match(r"^(\d+)_(\d+)_(\d{8})\.", basename)
    if m:
        emp_id, seq, date_str = m.group(1), m.group(2), m.group(3)
        return f"{LS_MSG_EUM_PREFIX}/K_{date_str}_{emp_id}_{seq}.pdf"
    return None


def make_fallback_url(attach_file_name: str, reg_dt: str) -> str:
    """upload/ fallback URL 생성"""
    url_param_0 = "B" + reg_dt[:6]
    safe_name = requests.utils.quote(attach_file_name)
    return f"{LS_UPLOAD_PREFIX}/{url_param_0}/{safe_name}"


# ---------------------------------------------------------------------------
# 목록 스크래핑
# ---------------------------------------------------------------------------

def fetch_list_page(url: str, retries: int = 3) -> Optional[BeautifulSoup]:
    """목록 페이지 1회 요청"""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, verify=False, timeout=30)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except Exception as e:
            print(f"  [WARN] 목록 페이지 요청 실패 (attempt {attempt}/{retries}): {e}", file=sys.stderr)
            if attempt < retries:
                time.sleep(2 * attempt)
    return None


def scrape_list(max_pages: int = 2) -> list[dict]:
    """LS증권 목록 2페이지 스크래핑 → 기사 기본 정보 리스트"""
    import urllib3
    urllib3.disable_warnings()

    articles: list[dict] = []
    seen_keys: set[str] = set()

    for p in range(1, max_pages + 1):
        page_has_data = False

        for board_order, base_url in enumerate(BOARD_URLS):
            target_url = base_url if p == 1 else base_url.replace("&page=1", f"&currPage={p}")

            print(f"  [LIST] board={board_order} page={p} url={target_url[:80]}...", file=sys.stderr)
            time.sleep(2)  # polite delay

            soup = fetch_list_page(target_url)
            if not soup:
                continue

            rows = soup.select("#contents > table > tbody > tr")
            print(f"  [LIST] board={board_order} page={p} → {len(rows)} rows", file=sys.stderr)

            for row in rows:
                try:
                    cells = row.select("td")
                    if len(cells) < 4:
                        continue

                    writer = cells[2].get_text(strip=True)
                    str_date = cells[3].get_text(strip=True)
                    a_tag = row.select_one("a")
                    if not a_tag:
                        continue

                    raw_href = a_tag["href"].replace("amp;", "")
                    article_url = LS_FRONT_BOARD_PREFIX + raw_href
                    key = clean_url(article_url).replace("&currPage=1", "")

                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    title_text = a_tag.get_text(strip=True)
                    title = title_text[title_text.find("]") + 1:].strip() if "]" in title_text else title_text

                    articles.append({
                        "sec_firm_order": SEC_FIRM_ORDER,
                        "article_board_order": board_order,
                        "board_name": BOARD_NAMES[board_order],
                        "firm_nm": FIRM_NM,
                        "reg_dt": re.sub(r"[-./]", "", str_date),
                        "article_title": title,
                        "writer": writer,
                        "key": key,
                        "list_article_url": article_url,
                        "article_url": "",
                        "download_url": "",
                        "telegram_url": "",
                        "pdf_url": "",
                        "save_time": datetime.now().isoformat(),
                    })
                    page_has_data = True
                except Exception as e:
                    print(f"  [WARN] row parse error: {e}", file=sys.stderr)

        if not page_has_data:
            print(f"  [LIST] page={p} empty, stopping pagination", file=sys.stderr)
            break

    return articles


# ---------------------------------------------------------------------------
# 상세 페이지 스크래핑
# ---------------------------------------------------------------------------

async def fetch_detail(session: aiohttp.ClientSession, url: str, retries: int = 3) -> Optional[str]:
    """상세 페이지 HTML 요청"""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, headers=DETAIL_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as e:
            print(f"  [WARN] detail fetch attempt {attempt}/{retries}: {e}", file=sys.stderr)
            if attempt < retries:
                await asyncio.sleep(2 * attempt)
    return None


async def probe_url(session: aiohttp.ClientSession, url: str, timeout: int = 10) -> bool:
    """HEAD 요청으로 URL 유효성 검사"""
    try:
        async with session.head(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            return resp.status == 200
    except Exception:
        return False


async def process_detail_article(
    session: aiohttp.ClientSession,
    article: dict,
    sem: asyncio.Semaphore,
) -> dict:
    """기사 1건의 상세 페이지를 가져와 제목/작성자/첨부파일 URL 추출"""
    async with sem:
        detail_url = article["list_article_url"]

        # PDF direct URL이면 즉시 할당
        if ".pdf" in detail_url.lower():
            article["article_url"] = detail_url
            article["telegram_url"] = detail_url
            article["pdf_url"] = detail_url
            article["download_url"] = detail_url
            return article

        html = await fetch_detail(session, detail_url)
        if not html:
            return article

        soup = BeautifulSoup(html, "html.parser")
        trs = soup.select("tr")
        attach_file_name = ""

        for tr in trs:
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue

            th_text = th.get_text(strip=True)
            td_text = td.get_text(strip=True)

            if th_text == "제목":
                article["article_title"] = td_text
            elif th_text == "필명" and td_text:
                article["writer"] = td_text
            elif th_text == "첨부파일":
                # 업로드 파일명 추출
                for a_tag in tr.select("td a"):
                    txt = a_tag.get_text(strip=True)
                    if re.search(r"\d+_\d+_\d{8}\.\w+$", txt):
                        attach_file_name = txt
                        break
                # img alt/src fallback
                if not attach_file_name:
                    img = soup.select_one(
                        "#contents > div.tbViewCon > div > html > body > p > img, "
                        "#contents > div.tbViewCon > div > p > img"
                    )
                    if img:
                        attach_file_name = img.get("alt") or os.path.basename(img.get("src", ""))

        # URL 결정
        resolved_url = ""

        # 1순위: CDN URL (upload filename → msg.ls-sec.co.kr)
        if attach_file_name:
            cdn_url = upload_filename_to_cdn_url(attach_file_name)
            if cdn_url and await probe_url(session, cdn_url):
                resolved_url = cdn_url
                print(f"  [DETAIL][CDN] {cdn_url}", file=sys.stderr)

        # 2순위: upload/ fallback
        if not resolved_url and attach_file_name:
            resolved_url = make_fallback_url(attach_file_name, article["reg_dt"])
            print(f"  [DETAIL][fallback] {resolved_url}", file=sys.stderr)

        if resolved_url:
            article["article_url"] = resolved_url
            article["telegram_url"] = resolved_url
            article["pdf_url"] = resolved_url
            article["download_url"] = resolved_url

        article["ATTACH_FILE_NAME"] = attach_file_name
        return article


async def scrape_detail(articles: list[dict], concurrency: int = 3) -> list[dict]:
    """모든 기사에 대해 상세 페이지 요청 (비동기 병렬)"""
    target_articles = [a for a in articles if ".pdf" not in a["list_article_url"].lower()]
    pdf_articles = [a for a in articles if ".pdf" in a["list_article_url"].lower()]

    if not target_articles:
        print(f"[DETAIL] {len(pdf_articles)} PDF direct, 0 detail needed", file=sys.stderr)
        return articles

    print(f"[DETAIL] {len(target_articles)} articles to fetch detail pages (concurrency={concurrency})", file=sys.stderr)

    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(ssl=False, limit=concurrency)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [process_detail_article(session, a, sem) for a in target_articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  [ERROR] detail article {i}: {result}", file=sys.stderr)
            traceback.print_exception(type(result), result, result.__traceback__, file=sys.stderr)
        else:
            target_articles[i] = result

    return target_articles + pdf_articles


# ---------------------------------------------------------------------------
# 진단
# ---------------------------------------------------------------------------

def run_diagnostics():
    """네트워크 진단"""
    import socket
    print("=== Diagnostics ===", file=sys.stderr)
    for url in [BOARD_URLS[0], LS_MSG_PREFIX, LS_HOME_URL]:
        host = urlparse(url).hostname
        port = urlparse(url).port or 443
        try:
            ip = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
            print(f"  DNS {host}: {ip[0][4][0]}", file=sys.stderr)
        except Exception as e:
            print(f"  DNS {host}: FAIL ({e})", file=sys.stderr)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="LS증권 Standalone Scraper")
    parser.add_argument("--max-pages", type=int, default=2, help="목록 페이지 수 (기본 2)")
    parser.add_argument("--no-detail", action="store_true", help="상세 페이지 스크래핑 건너뛰기")
    parser.add_argument("--detail-concurrency", type=int, default=3, help="상세 페이지 동시 요청 수 (기본 3)")
    args = parser.parse_args()

    import urllib3
    urllib3.disable_warnings()

    run_diagnostics()

    # ── Phase 1: 목록 스크래핑 ──
    print(f"\n=== Phase 1: List Scraping (max_pages={args.max_pages}) ===", file=sys.stderr)
    articles = scrape_list(max_pages=args.max_pages)
    print(f"[LIST] Total: {len(articles)} articles", file=sys.stderr)

    if not articles:
        output = {
            "scraped_at": datetime.now().isoformat(),
            "source": "github-actions",
            "count": 0,
            "articles": [],
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    # ── Phase 2: 상세 페이지 스크래핑 ──
    if not args.no_detail:
        print(f"\n=== Phase 2: Detail Scraping ===", file=sys.stderr)
        articles = asyncio.run(scrape_detail(articles, concurrency=args.detail_concurrency))

    # 해결된 URL 통계
    resolved = sum(1 for a in articles if a.get("telegram_url"))
    cdn_resolved = sum(1 for a in articles if a.get("telegram_url", "").startswith(LS_MSG_PREFIX))
    print(f"[RESULT] resolved={resolved}/{len(articles)} (CDN={cdn_resolved})", file=sys.stderr)

    output = {
        "scraped_at": datetime.now().isoformat(),
        "source": "github-actions",
        "count": len(articles),
        "resolved_urls": resolved,
        "cdn_resolved": cdn_resolved,
        "articles": articles,
    }

    # stdout → JSON (artifact)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
