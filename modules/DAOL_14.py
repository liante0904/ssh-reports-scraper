"""다올투자증권 — 서버 모듈. scrapers/daol_core.py 사용."""
import asyncio
import os
import sys

from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.daol_core import scrape_daol


async def DAOL_checkNewArticle():
    cfg = config.get_urls("DAOL_14")
    if not cfg:
        logger.warning("No config for DAOL_14")
        return []

    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, scrape_daol, cfg)
    except Exception as e:
        logger.error(f"DAOL error: {e}")
        return []
