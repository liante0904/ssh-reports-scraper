"""교보증권 — 서버 모듈. scrapers/kyobo_core.py 사용."""
import asyncio, os, sys
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.kyobo_core import scrape_kyobo

async def Kyobo_checkNewArticle(full_fetch=False):
    cfg = config.get_urls("Kyobo_24")
    if not cfg:
        logger.warning("No config for Kyobo_24")
        return []

    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, scrape_kyobo, cfg)
    except Exception as e:
        logger.error(f"Kyobo error: {e}")
        return []
