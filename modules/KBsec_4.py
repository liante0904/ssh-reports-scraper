# -*- coding:utf-8 -*-
"""KB증권 — 서버 모듈 (scraper.py fallback).

공통 코어 scrapers/kb_core.py 사용 → GA standalone과 로직 통일.
"""
import asyncio
import os
import sys

from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.kb_core import scrape_kb


async def KB_checkNewArticle():
    """KB증권 데이터 수집 (async wrapper → sync core).

    scraper.py에서 async function으로 취급되도록 coroutine 반환.
    실제 HTTP는 scrapers.kb_core.scrape_kb() (requests 기반, thread executor).
    """
    cfg = config.get_urls("KBsec_4")
    if not cfg:
        logger.warning("No config for KBsec_4")
        return []

    logger.debug(f"KB Scraper Start: KB증권 via scrapers.kb_core")

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, scrape_kb, cfg)
        logger.info(f"KB Scraper: Found {len(result)} articles")
        return result
    except Exception as e:
        logger.error(f"Error scraping KB Securities: {e}")
        return []


# Legacy (유지)
def KB_decode_url(url):
    import urllib.parse as urlparse
    import base64
    url = url.replace('&amp;', '&')
    parsed_url = urlparse.urlparse(url)
    query_params = urlparse.parse_qs(parsed_url.query)
    id_value = query_params.get('id', [None])[0]
    encoded_url = query_params.get('url', [None])[0]
    if id_value is None or encoded_url is None:
        logger.warning('Invalid URL for decoding: id or url is missing')
        return "Invalid URL: id or url is missing"
    try:
        encoded_url = encoded_url.replace('&amp;', '&')
        decoded_url = base64.b64decode(encoded_url).decode('utf-8')
        return decoded_url
    except Exception as e:
        logger.error(f"Error decoding url: {e}")
        return f"Error decoding url: {e}"


async def main():
    result = await KB_checkNewArticle()
    logger.info(f"Total articles fetched: {len(result)}")


if __name__ == "__main__":
    asyncio.run(main())
