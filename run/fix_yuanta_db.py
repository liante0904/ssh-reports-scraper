#!/usr/bin/env python3
"""
유안타증권 리포트 다운로드 URL 후처리 스크립트

문제:
  유안타증권 모듈(Yuanta_27.py)에서 PDF URL을 http:// 로 수집하여
  다운로드가 실패하는 케이스가 있음. (https:// 로 접근해야 정상 동작)

전략:
  1. sec_firm_order=27(유안타) 레코드 조회
  2. telegram_url이 http://file.myasset.com/ 인 경우 https:// 로 변환
  3. HEAD 검증 후 DB 업데이트
  4. 빈 URL은 article_url 재방문하여 PDF URL 재추출
"""
import asyncio
import aiohttp
import os
import re
import sys
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.db_factory import get_db

SEC_FIRM_ORDER = 27
FIRM_NAME = "유안타증권"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.myasset.com/",
}
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=30)


async def head_test(session: aiohttp.ClientSession, url: str) -> int | None:
    try:
        async with session.head(url, headers=HEADERS, timeout=10, ssl=False) as resp:
            return resp.status
    except Exception:
        return None


async def extract_pdf_from_article(session: aiohttp.ClientSession, article_url: str) -> str | None:
    """article_url 상세 페이지에서 PDF 링크(data-seq) 추출"""
    try:
        async with session.get(article_url, headers=HEADERS, timeout=FETCH_TIMEOUT, ssl=False) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")
            # PDF download link 찾기
            pdf_tag = soup.select_one('a.ico.acrobat')
            if pdf_tag and pdf_tag.has_attr('data-seq'):
                pdf_path = pdf_tag['data-seq']
                # https version
                return f"https://file.myasset.com/sitemanager/upload/{pdf_path}"
    except Exception:
        pass
    return None


async def fix_yuanta_urls():
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

    ok_count = 0
    recovered_https = 0
    recovered_article = 0
    empty_url = 0
    fail_count = 0

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=FETCH_TIMEOUT,
    ) as session:
        for i, r in enumerate(records, 1):
            report_id = r["report_id"]
            tg_url = r.get("telegram_url") or ""
            pdf_url = r.get("pdf_url") or ""
            article_url = r.get("article_url") or ""
            title = r.get("article_title", "")[:50]

            if not tg_url and not pdf_url:
                empty_url += 1
                # article_url로 재추출 시도
                if article_url:
                    new_pdf = await extract_pdf_from_article(session, article_url)
                    if new_pdf:
                        await db.update_telegram_url(
                            record_id=report_id,
                            telegram_url=new_pdf,
                            article_title=r.get("article_title"),
                            pdf_url=new_pdf,
                        )
                        recovered_article += 1
                        ok_count += 1
                        logger.success(f"  [{report_id}] article 재추출 성공: {new_pdf[:90]}")
                    else:
                        logger.warning(f"  [{report_id}] article 재추출 실패: {article_url[:80]}")
                continue

            target_url = tg_url or pdf_url

            # 1. HEAD 검증
            status = await head_test(session, target_url)
            if status == 200:
                ok_count += 1
                if i % 500 == 0:
                    logger.info(f"[{FIRM_NAME}] 진행 {i}/{total} (OK={ok_count})")
                continue

            # 2. 실패 → HTTP→HTTPS 변환
            if target_url.startswith("http://file.myasset.com/"):
                https_url = target_url.replace("http://", "https://")
                https_status = await head_test(session, https_url)
                if https_status == 200:
                    await db.update_telegram_url(
                        record_id=report_id,
                        telegram_url=https_url,
                        article_title=r.get("article_title"),
                        pdf_url=https_url,
                    )
                    recovered_https += 1
                    ok_count += 1
                    logger.success(f"  [{report_id}] HTTPS 복구: {https_url[:90]}")
                    continue

            # 3. HTTPS도 실패 → article_url 재방문
            if article_url:
                new_pdf = await extract_pdf_from_article(session, article_url)
                if new_pdf and new_pdf != target_url:
                    new_status = await head_test(session, new_pdf)
                    if new_status == 200:
                        await db.update_telegram_url(
                            record_id=report_id,
                            telegram_url=new_pdf,
                            article_title=r.get("article_title"),
                            pdf_url=new_pdf,
                        )
                        recovered_article += 1
                        ok_count += 1
                        logger.success(f"  [{report_id}] article 재추출 복구: {new_pdf[:90]}")
                        continue

            fail_count += 1
            if i % 200 == 0:
                logger.info(f"[{FIRM_NAME}] 진행 {i}/{total} (OK={ok_count}, HTTPS복구={recovered_https}, article복구={recovered_article}, 실패={fail_count})")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"[{FIRM_NAME}] URL 복구 완료")
    logger.info(f"  전체:           {total}건")
    logger.info(f"  정상(유지):      {ok_count - recovered_https - recovered_article}건")
    logger.info(f"  HTTPS 복구:      {recovered_https}건")
    logger.info(f"  article 재추출:  {recovered_article}건")
    logger.info(f"  빈 URL:          {empty_url}건")
    logger.info(f"  복구 실패:       {fail_count}건")
    logger.info("=" * 60)


if __name__ == "__main__":
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "fix_yuanta_db.log")

    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(log_file, level="DEBUG",
               rotation="10 MB", retention="14 days", encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    logger.info(f"[{FIRM_NAME}] PDF URL 후처리 시작")
    asyncio.run(fix_yuanta_urls())
    logger.info(f"[{FIRM_NAME}] PDF URL 후처리 종료")
