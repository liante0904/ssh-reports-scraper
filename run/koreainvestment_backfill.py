#!/usr/bin/env python3
"""한국투자증권 백필 — 2026-04-13 이후 누락 레포트 전체 수집 → DB import."""
import os, sys, asyncio

os.environ["KOREAMAX_PAGES"] = "50"  # 50페이지까지 수집 (충분한 커버리지)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv()

from loguru import logger
from utils.logger_util import setup_logger
setup_logger("koreainvestment_backfill")

from modules.Koreainvestment_13 import Koreainvestment_selenium_checkNewArticle
from models.db_factory import get_db

async def main():
    logger.info("=== 한국투자증권 백필 시작 (KOREAMAX_PAGES=50) ===")
    articles = await Koreainvestment_selenium_checkNewArticle()
    logger.info(f"수집 완료: {len(articles)}건")

    if not articles:
        logger.warning("수집된 아티클이 없습니다")
        return

    # key 기준 중복 제거 (같은 아티클이 여러 페이지/카테고리에 등장 가능)
    seen = set()
    deduped = []
    for a in articles:
        k = a.get("report_unique_key")
        if k and k not in seen:
            seen.add(k)
            deduped.append(a)
    logger.info(f"중복 제거: {len(articles)} → {len(deduped)}건")

    db = get_db()
    ins, upd = db.insert_json_data_list(deduped)
    logger.success(f"DB import: {ins} inserted, {upd} updated, {len(deduped) - ins - upd} skipped")
    logger.info("=== 백필 완료 ===")

if __name__ == "__main__":
    asyncio.run(main())
