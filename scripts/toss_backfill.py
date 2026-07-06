#!/usr/bin/env python3
"""토스증권 백필 — 누락 게시판(코멘트, English) 추가 후 전체 수집 → DB import."""
import os, sys, asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv()

from loguru import logger
from utils.logger_util import setup_logger
setup_logger("toss_backfill")

from modules.TOSSinvest_15 import TOSSinvest_checkNewArticle
from models.db_factory import get_db

async def main():
    logger.info("=== 토스증권 백필 시작 (3개 게시판) ===")
    articles = TOSSinvest_checkNewArticle()
    logger.info(f"수집 완료: {len(articles)}건")

    if not articles:
        logger.warning("수집된 아티클이 없습니다")
        return

    # key 기준 중복 제거
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
