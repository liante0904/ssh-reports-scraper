import asyncio
import aiohttp
import sys
import os
from loguru import logger
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.db_factory import get_db
from models.ConfigManager import config

async def check_url_validity(session, url, semaphore):
    """URL의 유효성을 체크 (HTTP 200 여부)"""
    async with semaphore:
        try:
            # LS증권은 특정 IP 대역이나 User-Agent에 민감할 수 있으므로 헤더 설정
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://www.ls-sec.co.kr/"
            }
            async with session.head(url, headers=headers, timeout=10, allow_redirects=True, ssl=False) as response:
                return response.status == 200
        except Exception as e:
            # logger.error(f"Error checking {url}: {e}")
            return False

async def verify_ls_urls():
    db = get_db()
    
    # 1. 대상 데이터 추출 (msg. 방식이 아닌 LS 리포트)
    query = """
        SELECT report_id, article_title, telegram_url, report_date
        FROM tbl_sec_reports
        WHERE firm_nm = 'LS증권'
          AND telegram_url NOT LIKE 'https://msg.ls-sec.co.kr/%'
          AND telegram_url LIKE 'https://www.ls-sec.co.kr/upload/%'
        ORDER BY report_date DESC
    """
    
    records = db._fetchall(query)
    total = len(records)
    logger.info(f"검증 대상 LS Fallback URL (upload/ 방식): {total}건")
    
    if total == 0:
        logger.info("검증할 데이터가 없습니다.")
        return

    # 최근 50건에 대해서만 실제 HTTP 체크 수행 (서버 부하 방지 및 샘플 검증)
    sample_size = min(50, total)
    samples = records[:sample_size]
    
    semaphore = asyncio.Semaphore(5) # 동시 요청 제한
    success_count = 0
    
    logger.info(f"최근 {sample_size}건에 대해 실제 파일 존재 여부를 확인합니다...")
    
    async with aiohttp.ClientSession() as session:
        tasks = [check_url_validity(session, r['telegram_url'], semaphore) for r in samples]
        results = await asyncio.gather(*tasks)
        
        for i, is_valid in enumerate(results):
            record = samples[i]
            status = "✅ [OK]" if is_valid else "❌ [FAILED]"
            logger.info(f"{status} | {record['report_date']} | {record['article_title'][:30]}... | {record['telegram_url']}")
            if is_valid:
                success_count += 1

    logger.info("=" * 50)
    logger.info(f"검증 요약 (최근 {sample_size}건 샘플)")
    logger.info(f"- 성공: {success_count}건")
    logger.info(f"- 실패: {sample_size - success_count}건")
    logger.info(f"- 성공률: {(success_count / sample_size) * 100:.1f}%")
    logger.info("=" * 50)

if __name__ == "__main__":
    asyncio.run(verify_ls_urls())
