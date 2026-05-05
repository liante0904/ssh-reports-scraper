import os
import sys
from loguru import logger
from datetime import datetime

def setup_logger(log_name="scraper"):
    """
    서비스 전체의 로깅 설정을 초기화합니다.
    - LOG_BASE_DIR 환경변수 우선, 없으면 ~/logs 사용
    - {LOG_BASE_DIR}/YYYYMMDD/YYYYMMDD_파일명.log 형식을 따름
    - 매일 자정 로테이션 및 ZIP 압축
    - 30일 경과 로그 자동 삭제
    """
    # LOG_BASE_DIR 환경변수 우선 (Docker 컨테이너 내 /log 마운트 경로 대응)
    base_log_dir = os.getenv("LOG_BASE_DIR", os.path.expanduser("~/logs"))
    log_format_path = os.path.join(base_log_dir, "{time:YYYYMMDD}", "{time:YYYYMMDD}_" + log_name + ".log")

    # 기존 핸들러 제거
    logger.remove()

    # 1. 콘솔 출력 (INFO 레벨 이상)
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

    # 2. 파일 출력 (DEBUG 레벨 이상)
    logger.add(
        log_format_path,
        rotation="00:00",
        retention="30 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        encoding="utf-8"
    )

    return logger

# 초기화 실행
if __name__ == "__main__":
    setup_logger("test")
    logger.info("Logging system initialized.")
