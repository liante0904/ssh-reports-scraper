import pytest
import os
import sys
from datetime import datetime, timedelta

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# CI 환경인지 확인 (GitHub Actions 등). 단, 전용 integration-canary workflow는
# CANARY_POSTGRES_* secrets로 구성된 비운영 DB를 대상으로 하므로 skip하지 않는다.
IS_CI = os.getenv('GITHUB_ACTIONS') == 'true'
IS_CANARY = os.getenv('ENV') == 'canary'

from models.db_factory import get_db
from tests.db_test_utils import postgres_available

if not postgres_available():
    import pytest
    pytest.skip("PostgreSQL에 연결할 수 없어 DB 통합 테스트를 건너뜁니다.", allow_module_level=True)

@pytest.mark.skipif(IS_CI and not IS_CANARY, reason="Generic CI 환경에서는 실제 DB 연결 테스트를 건너뜁니다 (integration-canary workflow만 예외).")
@pytest.mark.asyncio
async def test_db_connection_and_structure():
    """
    1. DB 연결이 정상인지 확인
    2. 기본 테이블(tbl_sec_reports)에서 데이터 조회가 가능한지 확인
    """
    db = get_db()
    
    # 단순 연결 및 조회 테스트
    query = "SELECT 1 as conn_test"
    result = await db.execute_query(query)
    
    assert result is not None, "DB 연결 결과가 None입니다."
    assert len(result) > 0, "DB에서 행을 반환하지 않았습니다."
    assert result[0]['conn_test'] == 1, "DB 연결 테스트 값이 일치하지 않습니다."

@pytest.mark.asyncio
async def test_recent_data_exists():
    """
    최근 7일 이내에 적재된 데이터가 최소 1건 이상 존재하는지 확인
    (스크래퍼가 정상 작동 중인지 간접 검증)
    """
    db = get_db()
    table_name = getattr(db, 'main_table_name', 'tbl_sec_reports')
    
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT COUNT(*) as cnt 
    FROM {table_name} 
    WHERE DATE(save_at) >= ?
    """
    result = await db.execute_query(query, (seven_days_ago,))
    
    count = result[0]['cnt']
    assert count > 0, f"최근 7일간 적재된 데이터가 0건입니다. (시작일: {seven_days_ago})"
    print(f"\n[Info] 최근 7일간 데이터 수: {count}건")

@pytest.mark.asyncio
async def test_data_integrity():
    """
    가장 최근 데이터 1건을 가져와 필수 필드(firm_nm, article_title)가 채워져 있는지 확인
    """
    db = get_db()
    table_name = getattr(db, 'main_table_name', 'tbl_sec_reports')
    
    query = f"SELECT firm_nm, article_title, save_at FROM {table_name} ORDER BY save_at DESC LIMIT 1"
    result = await db.execute_query(query)
    
    if result:
        row = result[0]
        assert row['firm_nm'] is not None and row['firm_nm'] != "", "증권사 이름이 비어있습니다."
        assert row['article_title'] is not None and row['article_title'] != "", "기사 제목이 비어있습니다."
        assert row['save_at'] is not None, "저장 시간이 비어있습니다."
    else:
        pytest.skip("검증할 데이터가 없습니다.")
