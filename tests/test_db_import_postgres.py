import asyncio
import json
import os
import sys
import glob
from loguru import logger
from dotenv import load_dotenv

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 환경 변수 강제 설정 (임포트 전에 수행)
os.environ["DB_BACKEND"] = "postgres"
load_dotenv(override=False)

from models.SecReportsManager import SecReportsManager
from tests.db_test_utils import postgres_available

if not postgres_available():
    import pytest
    pytest.skip("PostgreSQL에 연결할 수 없어 import 테스트를 건너뜁니다.", allow_module_level=True)

async def import_json_to_postgres():
    """
    json/ 폴더의 가장 최신 추출 파일을 찾아 PostgreSQL에 인서트합니다.
    """
    logger.info("Starting PostgreSQL data import test (Forced)...")
    
    # 2. 최신 JSON 파일 찾기
    json_dir = os.path.join(os.path.dirname(__file__), '..', 'json')
    json_files = glob.glob(os.path.join(json_dir, "exported_reports_*.json"))
    
    if not json_files:
        logger.error("No exported JSON files found in json/ directory.")
        return

    latest_file = max(json_files, key=os.path.getctime)
    logger.info(f"Latest JSON file found: {os.path.basename(latest_file)}")

    # 3. 데이터 로드
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            data_list = json.load(f)
        logger.info(f"Loaded {len(data_list)} records from JSON.")
    except Exception as e:
        logger.error(f"Failed to load JSON file: {e}")
        return

    # 4. PostgreSQL 인서트
    db = SecReportsManager()
    
    try:
        # PostgreSQL 인서트 수행
        ins, upd = db.insert_json_data_list(data_list)
        
        logger.success("====================================================")
        logger.success(f"PostgreSQL Sync Result (SUCCESS):")
        logger.success(f" - New records inserted: {ins}")
        logger.success(f" - Existing records updated: {upd}")
        logger.success("====================================================")

    except Exception as e:
        logger.error(f"PostgreSQL insertion failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(import_json_to_postgres())
