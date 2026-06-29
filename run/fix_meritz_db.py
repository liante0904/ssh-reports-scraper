#!/usr/bin/env python3
"""
메리츠증권 리포트 PDF URL 후처리 스크립트

문제:
  MERITZ_20 모듈에서 게시글 상세 페이지에 <a[title]> 태그가 없거나
  네트워크 오류 등으로 PDF URL 추출에 실패하면
  telegram_url / download_url / pdf_url 이 article_url(게시글 상세 URL)로
  fallback 되어 저장된 케이스들이 있음.

전략:
  1. DB에서 sec_firm_order=20(메리츠) 이면서
     telegram_url에 "WorkFlow" 경로가 없는 레코드들을 조회
  2. 각 레코드의 article_url(상세 페이지)를 재방문하여
     <a[title]> 태그에서 PDF 파일명 추출
  3. 올바른 WorkFlow PDF URL 재구성
  4. DB 업데이트 (telegram_url, pdf_url, download_url)
"""
import asyncio
import aiohttp
import os
import sys
import re
from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.db_factory import get_db

# ── 상수 ──────────────────────────────────────────────────────────────────────

SEC_FIRM_ORDER = 20
FIRM_NAME = "메리츠증권"
WORKFLOW_BASE = "https://home.imeritz.com/include/resource/research/WorkFlow"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://home.imeritz.com/",
}

FETCH_TIMEOUT = aiohttp.ClientTimeout(total=30)
CONCURRENCY = 5  # 동시 요청 제한 (서버 부하 방지)


# ── PDF URL 추출 ──────────────────────────────────────────────────────────────

async def extract_pdf_url(session: aiohttp.ClientSession, article_url: str) -> str | None:
    """
    메리츠증권 게시글 상세 페이지를 방문하여 PDF 다운로드 URL을 추출한다.

    정상: <a title="파일명.pdf 파일 다운로드"> 태그 발견
      → https://home.imeritz.com/include/resource/research/WorkFlow/파일명.pdf

    실패: None 반환
    """
    try:
        async with session.get(article_url, headers=HEADERS, timeout=FETCH_TIMEOUT) as resp:
            if resp.status != 200:
                logger.warning(f"HTTP {resp.status} on {article_url}")
                return None
            html = await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Failed to fetch {article_url}: {e}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # 1순위: <a title="파일명.pdf 파일 다운로드">
    download_tag = soup.select_one('a[title]')
    if download_tag and download_tag.get('title'):
        file_name = download_tag['title']
        file_name = file_name.replace(" 파일 다운로드", "").strip()
        if file_name:
            pdf_url = f"{WORKFLOW_BASE}/{file_name}"
            logger.debug(f"Extracted PDF URL via a[title]: {pdf_url}")
            return pdf_url

    # 2순위: 확장자 .pdf를 가진 <a> 태그 (title 속성 없는 경우)
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href.lower().endswith('.pdf'):
            # 상대 경로 처리
            if href.startswith('/'):
                pdf_url = f"https://home.imeritz.com{href}"
            elif href.startswith('http'):
                pdf_url = href
            else:
                pdf_url = f"{article_url.rstrip('/')}/{href}"
            logger.debug(f"Extracted PDF URL via href .pdf: {pdf_url}")
            return pdf_url

    # 3순위: onclick="downloadFile(...)" 패턴에서 파일명 추출
    onclick_match = re.search(
        r"downloadFile\s*\(\s*['\"]([^'\"]+\.pdf)['\"]",
        html,
        re.IGNORECASE,
    )
    if onclick_match:
        raw_name = onclick_match.group(1)
        pdf_url = f"{WORKFLOW_BASE}/{raw_name}"
        logger.debug(f"Extracted PDF URL via downloadFile(): {pdf_url}")
        return pdf_url

    logger.warning(f"No PDF URL found in {article_url}")
    return None


# ── 메인 로직 ──────────────────────────────────────────────────────────────────

async def fix_meritz_urls():
    db = get_db()

    # 1. 대상 데이터 조회
    #    - sec_firm_order=20 (메리츠)
    #    - telegram_url이 WorkFlow 경로가 아닌 경우 (fallback 상태)
    #    - 또는 telegram_url이 NULL/빈 문자열인 경우
    query = """
        SELECT report_id, "article_title", "article_url", "telegram_url", "pdf_url",
               "download_url", "writer", "reg_dt", "key"
        FROM "tbl_sec_reports"
        WHERE "sec_firm_order" = %s
          AND (
              "telegram_url" NOT LIKE '%%/WorkFlow/%%'
              OR "telegram_url" IS NULL
              OR "telegram_url" = ''
          )
          AND "article_url" IS NOT NULL
          AND "article_url" != ''
        ORDER BY "report_id" DESC
    """
    records = await db.execute_query(query, (SEC_FIRM_ORDER,))

    if not records:
        logger.info(f"[{FIRM_NAME}] 복구할 데이터가 없습니다.")
        return

    total = len(records)
    logger.info(f"[{FIRM_NAME}] 복구 대상: {total}건")

    # 통계
    updated_count = 0
    skip_no_article_url = 0
    skip_extract_fail = 0

    # 2. 상세 페이지 재방문 → PDF URL 재추출
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def process_one(article: dict) -> bool:
        nonlocal updated_count, skip_no_article_url, skip_extract_fail

        report_id = article["report_id"]
        article_url = article.get("article_url", "")
        old_tg = article.get("telegram_url", "") or ""
        old_pdf = article.get("pdf_url", "") or ""

        if not article_url:
            logger.warning(f"[report_id={report_id}] article_url 없음, 건너뜀")
            skip_no_article_url += 1
            return False

        logger.info(
            f"[{report_id}] 재처리 중: {article.get('article_title', '')[:50]}"
        )

        async with semaphore:
            pdf_url = await extract_pdf_url(session, article_url)

        if not pdf_url:
            logger.warning(
                f"[report_id={report_id}] PDF URL 추출 실패: {article_url}"
            )
            skip_extract_fail += 1
            return False

        # 3. 추출한 PDF URL이 기존과 동일하면 skip
        if pdf_url == old_tg:
            logger.debug(f"[report_id={report_id}] 이미 올바른 URL, skip")
            return False

        # 4. DB 업데이트 (telegram_url, pdf_url, download_url 모두 동일한 PDF URL로)
        success = await db.update_telegram_url(
            record_id=report_id,
            telegram_url=pdf_url,
            article_title=article.get("article_title"),
            pdf_url=pdf_url,
        )

        if success and success.get("status") == "success":
            logger.success(f"[report_id={report_id}] 복구 완료: {pdf_url}")
            updated_count += 1
            return True
        else:
            logger.error(f"[report_id={report_id}] DB 업데이트 실패")
            return False

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=FETCH_TIMEOUT,
    ) as session:
        for i, article in enumerate(records, 1):
            await process_one(article)
            # 진행률 로그
            if i % 10 == 0 or i == total:
                logger.info(
                    f"[{FIRM_NAME}] 진행률: {i}/{total} "
                    f"(성공={updated_count}, 추출실패={skip_extract_fail})"
                )
            # 서버 부하 방지를 위한 짧은 대기
            await asyncio.sleep(0.3)

    # 5. 최종 결과 요약
    logger.info("=" * 60)
    logger.info(f"[{FIRM_NAME}] URL 복구 작업 완료")
    logger.info(f"  - 전체 대상:      {total}건")
    logger.info(f"  - 복구 성공:      {updated_count}건")
    logger.info(f"  - 추출 실패:      {skip_extract_fail}건")
    logger.info(f"  - article_url 없음: {skip_no_article_url}건")
    logger.info("=" * 60)


if __name__ == "__main__":
    # 로그 설정 (파일 + 콘솔)
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "fix_meritz_db.log")

    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(log_file, level="DEBUG",
               rotation="10 MB", retention="14 days", encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    logger.info("메리츠증권 PDF URL 후처리 시작")
    asyncio.run(fix_meritz_urls())
    logger.info("메리츠증권 PDF URL 후처리 종료")
