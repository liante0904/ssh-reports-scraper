# -*- coding:utf-8 -*-
"""한화투자증권 — 서버 모듈. scrapers/hanwha_core.py 사용."""
import asyncio, os, sys
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.hanwha_core import scrape_hanwha


async def Hanwha_checkNewArticle():
    urls = {"urls": config.get_urls("Hanwhawm_21")}
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, scrape_hanwha, urls[0])
    except Exception as e:
        logger.error(f"Hanwha error: {e}")
        return []
