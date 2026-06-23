"""신한투자증권 — 서버 모듈. scrapers/shinhan_core.py로 delegate."""
import asyncio, os, sys
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.ConfigManager import config
from scrapers.shinhan_core import scrape_shinhan

async def ShinHanInvest_checkNewArticle():
    """신한투자증권 — core delegate."""
    urls = config.get_urls("ShinHanInvest_1")
    if not urls: return []
    cfg = {
        "url": urls[0],
        "str_boards": "giperiodicaldaily|gistockchart|plananalysis|gicompanyanalyst|giindustry|gieconomy|fxmarket|commodity|gibond|foreignbond",
        "bbs_boards": ["foreignstock","giresearchIPO","gieconomy","gicomment","gibond","foreignbond","gifuture","alternative"],
    }
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, scrape_shinhan, cfg)
    except Exception as e:
        logger.error(f"ShinHan error: {e}")
        return []
