import asyncio
import os
import sys
from datetime import datetime, timedelta
from loguru import logger
from dotenv import load_dotenv

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# PostgreSQL 강제 설정
os.environ["DB_BACKEND"] = "postgres"
load_dotenv(override=False)

from models.SecReportsManager import SecReportsManager
from tests.db_test_utils import postgres_available

if not postgres_available():
    import pytest
    pytest.skip("PostgreSQL에 연결할 수 없어 recent verification 테스트를 건너뜁니다.", allow_module_level=True)

async def verify_recent_postgres_data():
    """
    PostgreSQL에서 어제와 오늘 적재된 데이터를 조회하여 검증합니다.
    """
    db = SecReportsManager()
    
    # 날짜 범위 설정
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"Verifying PostgreSQL data for: {yesterday} ~ {today}")

    # 1. 날짜별 건수 조회
    query_count = f"""
    SELECT DATE(save_at) as date, COUNT(*) as cnt
    FROM {db.table_name}
    WHERE DATE(save_at) BETWEEN %s AND %s
    GROUP BY DATE(save_at)
    ORDER BY date DESC
    """
    
    # 2. 최신 데이터 샘플 조회
    query_sample = f"""
    SELECT firm_nm, article_title, save_at, telegram_url
    FROM {db.table_name}
    WHERE DATE(save_at) BETWEEN %s AND %s
    ORDER BY save_at DESC
    LIMIT 5
    """

    try:
        counts = db._fetchall(query_count, (yesterday, today))
        samples = db._fetchall(query_sample, (yesterday, today))

        logger.success("================ PostgreSQL Recent Data Report ================")
        if not counts:
            logger.warning("No data found for the specified range in PostgreSQL.")
        else:
            for row in counts:
                logger.info(f"📅 Date: {row['date']} | Count: {row['cnt']} records")
        
        logger.info("---------------------------------------------------------------")
        logger.info("Latest 5 Samples:")
        for i, s in enumerate(samples, 1):
            logger.debug(f"{i}. [{s['firm_nm']}] {s['article_title'][:50]}...")
            logger.debug(f"   Time: {s['save_at']} | URL: {s['telegram_url']}")
        logger.success("===============================================================")

    except Exception as e:
        logger.error(f"PostgreSQL verification failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(verify_recent_postgres_data())
