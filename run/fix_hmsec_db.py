#!/usr/bin/env python3
"""
현대차증권 리포트 다운로드 URL 후처리 스크립트

문제:
  현대차증권 모듈(Hmsec_9.py)에서 telegram_url을 SynapDocViewServer URL로 저장.
  (https://docs.hmsec.com/SynapDocViewServer/job?fid=...&sync=true&fileType=URL&filePath=...)
  실제 다운로드 가능한 PDF URL은 pdf_url 필드에 별도 저장되어 있음.
  (https://www.hmsec.com/documents/research/...)

전략:
  1. sec_firm_order=9(현대차증권) 레코드 조회
  2. telegram_url이 SynapDocViewServer URL인 경우, pdf_url로 교체
  3. HEAD 검증 후 DB 업데이트
"""
import asyncio
import aiohttp
import os
import sys
from datetime import datetime
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.db_factory import get_db

SEC_FIRM_ORDER = 9
FIRM_NAME = "현대차증권"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.hmsec.com/",
}
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=30)


async def head_test(session: aiohttp.ClientSession, url: str) -> int | None:
    try:
        async with session.head(url, headers=HEADERS, timeout=10, ssl=False) as resp:
            return resp.status
    except Exception:
        return None


async def fix_hmsec_urls():
    db = get_db()

    records = await db.execute_query("""
        SELECT report_id, "telegram_url", "pdf_url", "download_url",
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

    # 통계
    synap_viewer_count = 0  # telegram_url이 SynapDocViewer인 건
    already_direct = 0       # 이미 직접 PDF URL인 건
    fixed_count = 0          # fix 완료
    head_fail = 0            # HEAD 검증 실패

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=FETCH_TIMEOUT,
    ) as session:
        for i, r in enumerate(records, 1):
            report_id = r["report_id"]
            tg = r.get("telegram_url") or ""
            pdf = r.get("pdf_url") or ""
            dl = r.get("download_url") or ""

            # SynapDocViewServer URL인지 확인
            is_synap = "SynapDocViewServer" in tg

            if not is_synap:
                already_direct += 1
                continue

            synap_viewer_count += 1

            # 직접 PDF URL 찾기 (pdf_url 우선, 없으면 download_url)
            direct_pdf = pdf or dl
            if not direct_pdf:
                logger.warning(f"  [{report_id}] pdf_url과 download_url 모두 없음")
                continue

            # 이미 동일하면 skip
            if direct_pdf == tg:
                already_direct += 1
                continue

            # HEAD 검증
            status = await head_test(session, direct_pdf)
            if status != 200:
                # HTTP→HTTPS 시도
                if direct_pdf.startswith("http://"):
                    https_pdf = direct_pdf.replace("http://", "https://")
                    https_status = await head_test(session, https_pdf)
                    if https_status == 200:
                        direct_pdf = https_pdf
                        status = https_status

            if status != 200:
                head_fail += 1
                logger.warning(f"  [{report_id}] PDF URL HEAD 실패 (status={status}): {direct_pdf[:90]}")
                continue

            # DB 업데이트
            await db.update_telegram_url(
                record_id=report_id,
                telegram_url=direct_pdf,
                article_title=r.get("article_title"),
                pdf_url=direct_pdf,
            )
            fixed_count += 1
            logger.success(f"  [{report_id}] 교체 완료: SynapDocViewer → {direct_pdf[:90]}")

            if i % 200 == 0:
                logger.info(f"[{FIRM_NAME}] 진행 {i}/{total} (수정={fixed_count}, viewer={synap_viewer_count})")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"[{FIRM_NAME}] URL 복구 완료")
    logger.info(f"  전체:                {total}건")
    logger.info(f"  SynapDocViewer URL:  {synap_viewer_count}건")
    logger.info(f"  - 직접 PDF로 교체:    {fixed_count}건")
    logger.info(f"  - HEAD 실패:         {head_fail}건")
    logger.info(f"  이미 직접 URL:       {already_direct}건")
    logger.info("=" * 60)


if __name__ == "__main__":
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "fix_hmsec_db.log")

    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(log_file, level="DEBUG",
               rotation="10 MB", retention="14 days", encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    logger.info(f"[{FIRM_NAME}] PDF URL 후처리 시작")
    asyncio.run(fix_hmsec_urls())
    logger.info(f"[{FIRM_NAME}] PDF URL 후처리 종료")
