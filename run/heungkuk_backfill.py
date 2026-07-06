#!/usr/bin/env python3
"""흥국증권 백필 — 누락 레포트 전체 수집 → DB import."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv()
from loguru import logger
from utils.logger_util import setup_logger
setup_logger("heungkuk_backfill")
from modules.Heungkuk_28 import Heungkuk_checkNewArticle
from models.db_factory import get_db

logger.info("=== 흥국증권 백필 시작 ===")
articles = Heungkuk_checkNewArticle()
logger.info(f"수집 완료: {len(articles)}건")

if articles:
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
    logger.success(f"DB import: {ins} inserted, {upd} updated")
logger.info("=== 백필 완료 ===")
