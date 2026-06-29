#!/usr/bin/env python3
"""
IM증권 리포트 다운로드 URL 후처리 스크립트

문제:
  IM증권 모듈(iMfnsec_18.py)에서 수집된 URL은 https://www.imfnsec.com/upload/... 형태로
  저장되어 있으나, 다운로드 시 실패하는 케이스가 다수 존재.
  article_url이 BASE_URL(=https://m.imfnsec.com:442)로 저장되어 재방문 불가.

전략:
  1. sec_firm_order=18(IM증권) 레코드 조회
  2. telegram_url HEAD 검증
  3. fallback 경로 패턴 검증
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

SEC_FIRM_ORDER = 18
FIRM_NAME = "IM증권"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15",
    "Referer": "https://www.imfnsec.com/",
}
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=30)


async def head_test(session: aiohttp.ClientSession, url: str) -> int | None:
    try:
        async with session.head(url, headers=HEADERS, timeout=10, ssl=False) as resp:
            return resp.status
    except Exception:
        return None


async def fix_im_urls():
    db = get_db()

    records = await db.execute_query("""
        SELECT report_id, "telegram_url", "pdf_url", "article_url",
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
            else:
                fail += 1
                if i <= 5:
                    logger.warning(f"  [{report_id}] HEAD 실패 (status={status}): {url[:90]}")

            if i % 1000 == 0:
                logger.info(f"[{FIRM_NAME}] 진행 {i}/{total} (OK={ok_200}, 실패={fail})")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"[{FIRM_NAME}] HEAD 검증 완료")
    logger.info(f"  전체:   {total}건")
    logger.info(f"  OK(200): {ok_200}건")
    logger.info(f"  실패:   {fail}건")
    logger.info(f"  빈 URL: {empty}건")
    logger.info("=" * 60)


if __name__ == "__main__":
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "fix_im_db.log")

    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(log_file, level="DEBUG",
               rotation="10 MB", retention="14 days", encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    logger.info(f"[{FIRM_NAME}] PDF URL HEAD 검증 시작")
    asyncio.run(fix_im_urls())
    logger.info(f"[{FIRM_NAME}] PDF URL HEAD 검증 종료")
