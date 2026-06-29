from loguru import logger
import os
import time
import re
import requests
import urllib3
from datetime import datetime
from bs4 import BeautifulSoup
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo
from models.ConfigManager import config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# WARP SOCKS5 프록시 설정
SOCKS_PROXY = os.getenv("SOCKS_PROXY_URL", "socks5h://localhost:9091")
PROXIES = {
    'http': SOCKS_PROXY,
    'https': SOCKS_PROXY,
}
BNK_DIRECT_RETRIES = int(os.getenv("BNK_DIRECT_RETRIES", "2"))
BNK_WARP_RETRIES = int(os.getenv("BNK_WARP_RETRIES", "3"))


def _fetch_url(url, headers, use_warp=False):
    """직접 혹은 WARP 프록시로 URL 요청 (HTML 반환)"""
    kwargs = dict(headers=headers, verify=False, timeout=30)
    if use_warp:
        kwargs['proxies'] = PROXIES
        kwargs['timeout'] = 45
    try:
        resp = requests.get(url, **kwargs)
        resp.raise_for_status()
        return resp.text
    except Exception:
        return None


def _fetch_url_with_retry(url, headers):
    """직접 → WARP 순서로 요청 재시도"""
    for attempt in range(1, BNK_DIRECT_RETRIES + 1):
        html = _fetch_url(url, headers)
        if html:
            return html
        if attempt < BNK_DIRECT_RETRIES:
            time.sleep(attempt)
    logger.warning(f"BNK: 직접 연결 실패, WARP 프록시로 재시도: {url}")
    for attempt in range(1, BNK_WARP_RETRIES + 1):
        html = _fetch_url(url, headers, use_warp=True)
        if html:
            logger.success(f"BNK: WARP 경유 성공: {url}")
            return html
        if attempt < BNK_WARP_RETRIES:
            time.sleep(attempt)
    return None


def BNK_checkNewArticle():
    sec_firm_order = 23
    TARGET_URL_TUPLE = config.get_urls("BNKfn_23")
    json_data_list = []

    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ko,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    for article_board_order, url in enumerate(TARGET_URL_TUPLE):
        try:
            html = _fetch_url_with_retry(url, headers)
            if not html:
                logger.warning(f"BNK: 최종 요청 실패: {url}")
                continue

            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", class_="table01")

            if not table:
                logger.warning(f"BNK: Table 'table01' not found in {url}")
                continue

            rows = table.select("tbody tr")
            logger.info(f"BNK: {url} → {len(rows)}개 행 발견")

            firm_info = FirmInfo(
                sec_firm_order=sec_firm_order,
                article_board_order=article_board_order
            )

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
                match = re.search(r"viewAction\(this, '\d+', '(/uploads/[^']+)', '([^']+)'\);", onclick_attr)
                article_url = ""
                if match:
                    base_path = match.group(1)
                    file_name = match.group(2)
                    article_url = f"https://www.bnkfn.co.kr{base_path}/{file_name}"

                reg_dt = cells[4].get_text(strip=True)

                json_data_list.append({
                    "sec_firm_order": sec_firm_order,
                    "article_board_order": article_board_order,
                    "firm_nm": firm_info.get_firm_name(),
                    "reg_dt": re.sub(r"[-./]", "", reg_dt),
                    "article_title": article_title,
                    "article_url": article_url,
                    "download_url": article_url,
                    "telegram_url": article_url,
                    "writer": writer,
                    "save_time": datetime.now().isoformat(),
                    "key": article_url,
                    "report_unique_key": article_url
                })

            time.sleep(0.5)

        except Exception as e:
            logger.error(f"BNK: 예외 발생 ({url}): {e}")

    if not json_data_list:
        logger.error(f"BNK: 모든 URL({len(TARGET_URL_TUPLE)}개)에서 0건 수집. 사이트 접근 불가 또는 구조 변경 확인 필요.")

    return json_data_list


if __name__ == "__main__":
    articles = BNK_checkNewArticle()
    for article in articles:
        logger.debug(article)
