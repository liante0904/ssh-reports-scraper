import os
import sys
from datetime import datetime, timedelta
from loguru import logger

# 상위 디렉터리를 모듈 경로에 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.db_factory import get_db

def reset_yesterday_status():
    db = get_db()
    
    # 오늘이 2026-04-26이므로 어제는 2026-04-25입니다.
    # 만약 실행 시점의 실제 어제를 구하고 싶다면 아래와 같이 설정합니다.
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # 2026-04-25로 명시적으로 지정 (사용자 요청 시점 기준)
    target_date = "2026-04-25"
    
    logger.info(f"Target date for reset: {target_date}")
    
    # PostgreSQL 쿼리 실행
    # "main_ch_send_yn"을 'N'으로 변경하여 재발송 대상으로 만듭니다.
    table_name = getattr(db, "MAIN_TABLE", getattr(db, "table_name", "tbl_sec_reports"))
    sql = f"""
        UPDATE {table_name}
        SET "main_ch_send_yn" = 'N', "is_sent" = false
        WHERE DATE("save_time") = %s
    """
    
    try:
        result = db._execute(sql, (target_date,))
        if result["status"] == "success":
            logger.success(f"Successfully reset {result['affected_rows']} reports for {target_date}.")
        else:
            logger.error(f"Failed to reset status: {result}")
    except Exception as e:
        logger.error(f"Error during status reset: {e}")

if __name__ == "__main__":
    reset_yesterday_status()
