# -*- coding:utf-8 -*-
"""한양증권 — 서버 모듈. scrapers/hanyang_core.py 사용."""
import asyncio, os, sys
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.hanyang_core import scrape_hanyang


async def Hanyang_checkNewArticle():
    cfg = config.get_urls("Hygood_22")
    if not cfg:
        logger.warning("No config for Hygood_22")
        return []

    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, scrape_hanyang, cfg)
    except Exception as e:
        logger.error(f"Hanyang error: {e}")
        return []
