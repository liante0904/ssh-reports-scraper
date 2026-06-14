# -*- coding:utf-8 -*-
"""키움증권 — 서버 모듈. scrapers/kiwoom_core.py 사용."""
import asyncio, os, sys
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.kiwoom_core import scrape_kiwoom


async def Kiwoom_checkNewArticle():
    urls = {"urls": config.get_urls("Kiwoom_10")}
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, scrape_kiwoom, urls)
    except Exception as e:
        logger.error(f"Kiwoom error: {e}")
        return []
