import asyncio
import json
import os
import sys
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
    pytest.skip("PostgreSQL에 연결할 수 없어 export 테스트를 건너뜁니다.", allow_module_level=True)

async def export_postgres_to_json():
    """
    PostgreSQL의 모든 데이터를 JSON으로 추출합니다.
    """
    logger.info("Starting PostgreSQL data export for reverse sync...")
    
    db = SecReportsManager()
    
    # 모든 데이터 조회
    query = f'SELECT * FROM {db.table_name} ORDER BY save_at DESC'

    try:
        results = db._fetchall(query)
        
        if not results:
            logger.warning("No data found in PostgreSQL.")
            return None

        logger.success(f"Successfully fetched {len(results)} records from PostgreSQL.")

        # JSON 저장
        json_dir = os.path.join(os.path.dirname(__file__), '..', 'json')
        os.makedirs(json_dir, exist_ok=True)
        
        file_path = os.path.join(json_dir, "postgres_reports_export.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

        logger.info(f"Data exported to: {os.path.abspath(file_path)}")
        return file_path

    except Exception as e:
        logger.error(f"Failed to export PostgreSQL data: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(export_postgres_to_json())
