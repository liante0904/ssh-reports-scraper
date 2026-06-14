# -*- coding:utf-8 -*-
"""NH투자증권 — 서버 모듈 (scraper.py fallback).
공통 코어 scrapers/nhqv_core.py 사용 → GA standalone과 로직 통일."""
import asyncio
import os
import sys

from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.nhqv_core import scrape_nhqv


async def NHQV_checkNewArticle(target_date=None):
    cfg = config.get_urls("NHQV_2")
    if not cfg:
        logger.warning("No config for NHQV_2")
        return []

    logger.debug("NHQV Scraper Start: NH투자증권 via scrapers.nhqv_core")

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, scrape_nhqv, cfg, target_date)
        logger.info(f"NHQV Scraper: Found {len(result)} articles")
        return result
    except Exception as e:
        logger.error(f"Error scraping NHQV: {e}")
        return []


# Legacy (유지)
def is_weekday(date_obj):
    return date_obj.weekday() < 5


def generate_workdays(start_date, end_date):
    current_date = start_date
    weekdays = []
    while current_date <= end_date:
        if is_weekday(current_date):
            weekdays.append(current_date.strftime('%Y%m%d'))
        current_date += __import__('datetime').timedelta(days=1)
    return weekdays


async def main():
    import datetime
    result = await NHQV_checkNewArticle(datetime.datetime.now().strftime('%Y%m%d'))
    logger.info(f"Total NHQV articles fetched: {len(result)}")


if __name__ == "__main__":
    asyncio.run(main())
