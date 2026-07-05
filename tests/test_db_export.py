import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from loguru import logger

# 프로젝트 루트 경로를 sys.path에 추가하여 models, utils 접근 가능하게 함
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.db_factory import get_db
from tests.db_test_utils import postgres_available

if not postgres_available():
    import pytest
    pytest.skip("PostgreSQL에 연결할 수 없어 DB export 테스트를 건너뜁니다.", allow_module_level=True)

async def export_recent_reports_to_json():
    """
    어제와 오늘 PostgreSQL에 적재된(save_at 기준) 리포트를 조회하여 JSON으로 저장합니다.
    """
    logger.info("Starting PostgreSQL report export test...")
    
    db = get_db()
    table_name = getattr(db, 'main_table_name', 'tbl_sec_reports')
    
    # 날짜 설정 (어제, 오늘)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    date_today_str = today.strftime('%Y-%m-%d')
    date_yesterday_str = yesterday.strftime('%Y-%m-%d')
    
    logger.info(f"Target date range: {date_yesterday_str} ~ {date_today_str}")

    # SQL 쿼리 작성 (save_at 필드 기준)
    # DATE(save_at) 함수를 사용하여 날짜 부분만 비교
    query = f"""
    SELECT * 
    FROM {table_name} 
    WHERE DATE(save_at) BETWEEN ? AND ?
    ORDER BY save_at DESC
    """
    params = (date_yesterday_str, date_today_str)

    try:
        # 데이터 조회 실행
        results = await db.execute_query(query, params)
        
        if not results:
            logger.warning(f"No data found for the range {date_yesterday_str} ~ {date_today_str}.")
            return

        logger.success(f"Successfully fetched {len(results)} records.")

        # JSON 저장 경로 설정
        json_dir = os.path.join(os.path.dirname(__file__), '..', 'json')
        os.makedirs(json_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"exported_reports_{timestamp}.json"
        file_path = os.path.join(json_dir, file_name)

        # JSON 파일 저장
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

        logger.info(f"Data exported to: {os.path.abspath(file_path)}")
        
        # 샘플 출력 (첫 번째 데이터 요약)
        sample = results[0]
        logger.debug(f"Sample data (Latest): [{sample.get('firm_nm')}] {sample.get('article_title')} ({sample.get('save_at')})")

    except Exception as e:
        logger.error(f"Failed to export data: {e}")
        raise

if __name__ == "__main__":
    # 비동기 실행
    asyncio.run(export_recent_reports_to_json())
