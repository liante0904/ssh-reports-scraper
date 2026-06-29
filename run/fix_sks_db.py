#!/usr/bin/env python3
"""
SK증권 리포트 다운로드 URL 후처리 스크립트

문제:
  SK증권 모듈(SKS_26.py)에서 수집된 URL은 https://www.sks.co.kr/data1/research/qna_file/...
  형태로 저장되어 있으나, 다운로드 시 실패하는 케이스가 다수 존재.
  (article_url = telegram_url 로 동일, 재방문 불가)

전략:
  1. sec_firm_order=26(SK증권) 레코드 조회
  2. telegram_url HEAD 검증
  3. HTTP→HTTPS 변환 시도
  4. 통계 보고
"""
import asyncio
import aiohttp
import os
import sys
from datetime import datetime
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.db_factory import get_db

SEC_FIRM_ORDER = 26
FIRM_NAME = "SK증권"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.sks.co.kr/",
}
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=30)


async def head_test(session: aiohttp.ClientSession, url: str) -> int | None:
    try:
        async with session.head(url, headers=HEADERS, timeout=10, ssl=False) as resp:
            return resp.status
    except Exception:
        return None


async def fix_sks_urls():
    db = get_db()

    records = await db.execute_query("""
        SELECT report_id, "telegram_url", "pdf_url",
               "article_title", "writer", "key"
        FROM "tbl_sec_reports"
        WHERE "sec_firm_order" = %s
        ORDER BY "report_id" DESC
    """, (SEC_FIRM_ORDER,))

    if not records:
        logger.info(f"[{FIRM_NAME}] 처리할 레코드가 없습니다.")
        return

    total = len(records)
    logger.info(f"[{FIRM_NAME}] 전체: {total}건")

    ok_200 = 0
    recovered_https = 0
    fail = 0
    empty = 0

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=FETCH_TIMEOUT,
    ) as session:
        for i, r in enumerate(records, 1):
            report_id = r["report_id"]
            url = r.get("telegram_url") or r.get("pdf_url") or ""

            if not url:
                empty += 1
                continue

            status = await head_test(session, url)

            if status == 200:
                ok_200 += 1
                if i % 500 == 0:
                    logger.info(f"[{FIRM_NAME}] 진행 {i}/{total} (OK={ok_200})")
                continue

            # 실패 → HTTP→HTTPS 시도
            if url.startswith("http://"):
                https_url = url.replace("http://", "https://")
                https_status = await head_test(session, https_url)
                if https_status == 200:
                    await db.update_telegram_url(
                        record_id=report_id,
                        telegram_url=https_url,
                        article_title=r.get("article_title"),
                        pdf_url=https_url,
                    )
                    recovered_https += 1
                    ok_200 += 1
                    logger.success(f"  [{report_id}] HTTPS 복구: {https_url[:90]}")
                    continue

            fail += 1
            if fail <= 3:
                logger.warning(f"  [{report_id}] HEAD 실패 (status={status}): {url[:90]}")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"[{FIRM_NAME}] HEAD 검증 완료")
    logger.info(f"  전체:       {total}건")
    logger.info(f"  OK(200):    {ok_200}건")
    logger.info(f"  - HTTPS복구: {recovered_https}건")
    logger.info(f"  실패:       {fail}건")
    logger.info(f"  빈 URL:     {empty}건")
    logger.info("=" * 60)


if __name__ == "__main__":
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "fix_sks_db.log")

    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(log_file, level="DEBUG",
               rotation="10 MB", retention="14 days", encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    logger.info(f"[{FIRM_NAME}] PDF URL HEAD 검증 시작")
    asyncio.run(fix_sks_urls())
    logger.info(f"[{FIRM_NAME}] PDF URL HEAD 검증 종료")
