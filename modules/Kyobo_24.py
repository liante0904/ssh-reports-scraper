"""교보증권 — 서버 모듈. scrapers/kyobo_core.py 사용."""
import asyncio, os, sys
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.kyobo_core import scrape_kyobo

async def Kyobo_checkNewArticle(full_fetch=False):
    urls = {"urls": config.get_urls("Kyobo_24")}
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, scrape_kyobo, urls)
    except Exception as e:
        logger.error(f"Kyobo error: {e}")
        return []
