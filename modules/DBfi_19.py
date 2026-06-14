"""DB증권 — 서버 모듈. scrapers/dbfi_core.py 사용."""
import asyncio,os,sys
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from models.ConfigManager import config
from scrapers.dbfi_core import scrape_dbfi
from scrapers.dbfi_core import scrape_dbfi as _dbfi_scrape

dbfi_cfg = config.get_urls("DBfi_19")
BASE_URL = dbfi_cfg["base_url"]
VIEWER_BASE = dbfi_cfg["viewer_base_url"]
URL_PATHS = dbfi_cfg["url_paths"]

async def DBfi_checkNewArticle():
    cfg = config.get_urls("DBfi_19")
    if not cfg: logger.warning("No URLs for DBfi_19"); return []
    loop = asyncio.get_event_loop()
    try: return await loop.run_in_executor(None, scrape_dbfi, cfg)
    except Exception as e: logger.error(f"DBfi error: {e}"); return []
