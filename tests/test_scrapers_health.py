import pytest
import asyncio
import os
import sys
import traceback
from loguru import logger

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 테스트 수행 시 운영 데이터 보호 및 실제 알림 소음 방지를 위해 환경 변수 강제 설정
os.environ["DB_BACKEND"] = "static"
os.environ["ENV"] = "dev"

# 텔레그램 알림용
from utils.telegram_util import send_system_alert
from tests.scraper_registry import active_health_checks

@pytest.mark.parametrize("name, mod_path, func_name, is_async", active_health_checks())
@pytest.mark.asyncio
async def test_scraper_health(name, mod_path, func_name, is_async):
    """
    각 증권사 스크래퍼의 임포트 및 헬스 체크를 수행합니다.
    """
    logger.info(f"Checking health for: {name} ({mod_path})")
    
    try:
        # 1. 임포트 테스트 (KeyError 등 초기화 에러 잡기)
        import importlib
        module = importlib.import_module(mod_path)
        func = getattr(module, func_name)
        
        # 2. 실행 테스트 (네트워크/구조 에러 잡기)
        if is_async:
            result = await func()
        else:
            result = await asyncio.to_thread(func)
        
        assert result is not None, f"{name}: 결과가 None입니다."
        assert isinstance(result, list), f"{name}: 리스트 형식이 아닙니다."
        
        logger.success(f"{name}: OK ({len(result)} articles)")

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"{name} 헬스체크 실패:\n{tb}")
        
        # 실제 운영 환경일 때만 알림 발송 (테스트 시 소음 방지)
        from models.ConfigManager import config
        if config.ENV == 'production':
            error_msg = f"❌ **{name} 스크래퍼 점검 필요!**\n\n"
            error_msg += f"**에러:** `{str(e)}`\n"
            error_msg += f"**위치:**\n```{tb[-300:]}```"
            await send_system_alert(error_msg)
            
        pytest.fail(f"{name} failed: {str(e)}")
