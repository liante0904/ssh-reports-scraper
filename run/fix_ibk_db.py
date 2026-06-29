#!/usr/bin/env python3
"""
IBK투자증권 리포트 다운로드 URL 후처리 스크립트

문제:
  IBK 모듈(IBKs_25.py)은 게시판(board_idx)별로 URL 경로(path)가 다름.
  과거에는 URL_INFO 에 있는 path 를 그대로 썼지만
  현재 코드는 board_idx=0(전략/시황) 에 한해 "invreport" → "invrespect" 로
  하드코딩 오버라이드 하고 있음.
  이로 인해 구버전 수집 데이터는 경로가 틀릴 수 있음.

전략:
  1. sec_firm_order=25(IBK) 레코드 조회
  2. article_board_order 값으로 올바른 path_name 결정
  3. 현재 URL의 경로가 올바른지 비교
  4. 경로가 틀렸으면 올바른 경로로 URL 재조립 + HEAD 검증
  5. DB 업데이트
"""
import asyncio
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime
from loguru import logger

import requests
import urllib3
urllib3.disable_warnings()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.db_factory import get_db


# ── IBK URL_INFO 매핑 (IBKs_25.py 와 동일) ──────────────────────────────────

# (board_idx, board_name, 올바른 path_name)
# board_idx=0 은 현재 코드상 "invrespect" 가 정답
BOARD_PATH_MAP = [
    (0, "전략/시황",     "invrespect"),     # ★ URL_INFO["path"]="invreport" 지만 코드에서 invrespect 로 오버라이드
    (1, "기업분석",     "busreport"),
    (2, "산업분석",     "indreport"),
    (3, "경제/채권",    "comment"),
    (4, "해외기업분석", "overseasreport"),
    (5, "글로벌ETF",    "overseasreport"),
]

# board_idx → 올바른 path_name lookup
CORRECT_PATH = {idx: path for idx, _, path in BOARD_PATH_MAP}
BOARD_NAME   = {idx: name for idx, name, _ in BOARD_PATH_MAP}

# 모든 가능한 path 목록 (게시판 경로 탐색 fallback 용)
ALL_PATHS = list(set(path for _, _, path in BOARD_PATH_MAP))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://m.ibks.com/",
}
TIMEOUT = 10


# ── URL 경로 교정 ─────────────────────────────────────────────────────────────

def correct_url(old_url: str, board_idx: int) -> str | None:
    """
    IBK 다운로드 URL의 경로를 올바른 path_name 으로 교정한다.
    예: .../invreport/xxx.pdf → .../invrespect/xxx.pdf  (board_idx=0)
    """
    if not old_url:
        return None

    correct_path = CORRECT_PATH.get(board_idx)
    if not correct_path:
        return None

    # 현재 URL에서 path segment 추출
    m = re.search(r'(https://download\.ibks\.com/emsdata/tradeinfo/)([^/]+)/(.+)', old_url)
    if not m:
        return None

    base = m.group(1)
    current_path = m.group(2)
    filename = m.group(3)

    if current_path == correct_path:
        return None  # 이미 올바름

    new_url = f"{base}{correct_path}/{filename}"
    return new_url


def test_url(url: str) -> bool:
    """HEAD 요청으로 URL 유효성 확인"""
    try:
        r = requests.head(url, headers=HEADERS, timeout=TIMEOUT, verify=False)
        return r.status_code == 200
    except Exception:
        return False


# ── 메인 로직 ─────────────────────────────────────────────────────────────────

async def fix_ibk_urls():
    db = get_db()

    # 1. IBK 레코드 조회 (download.ibks.com URL 만)
    records = await db.execute_query("""
        SELECT report_id, "article_board_order", "telegram_url", "pdf_url",
               "download_url", "article_title", "writer"
        FROM "tbl_sec_reports"
        WHERE "sec_firm_order" = 25
          AND "telegram_url" LIKE 'https://download.ibks.com/%%'
        ORDER BY "report_id" DESC
    """)

    if not records:
        logger.info("[IBK] 처리할 레코드가 없습니다.")
        return

    total = len(records)
    logger.info(f"[IBK] 전체 IBK 레코드: {total}건")

    # 2. board_idx 별 통계
    board_stats = {}
    for r in records:
        bidx = r["article_board_order"]
        board_stats[bidx] = board_stats.get(bidx, 0) + 1

    logger.info("[IBK] 게시판별 분포:")
    for bidx in sorted(board_stats.keys()):
        name = BOARD_NAME.get(bidx, f"board_{bidx}")
        cnt = board_stats[bidx]
        correct = CORRECT_PATH.get(bidx, "?")
        logger.info(f"  board {bidx} ({name:8s}) → path={correct:15s} : {cnt:>6}건")

    # 3. 경로 교정
    updated_count = 0
    skip_correct = 0
    skip_test_fail = 0
    skip_no_change = 0

    for idx, r in enumerate(records, 1):
        report_id = r["report_id"]
        board_idx = r["article_board_order"]
        old_tg = r["telegram_url"] or ""

        new_tg = correct_url(old_tg, board_idx)

        if new_tg is None:
            skip_correct += 1
            continue

        if new_tg == old_tg:
            skip_no_change += 1
            continue

        # HEAD 검증
        logger.info(f"[{idx}/{total}] report_id={report_id} board={board_idx}: 테스트 중...")
        if not test_url(new_tg):
            # 1차 실패 → 게시판 경로를 모두 탐색하여 유효한 경로 찾기
            m = re.search(r'(https://download\.ibks\.com/emsdata/tradeinfo/)([^/]+)/(.+)', old_tg)
            found_by_probe = None
            if m:
                base = m.group(1)
                filename = m.group(3)
                for probe_path in ALL_PATHS:
                    if probe_path == CORRECT_PATH.get(board_idx):
                        continue  # 이미 위에서 실패한 경로
                    probe_url = f"{base}{probe_path}/{filename}"
                    if test_url(probe_url):
                        found_by_probe = probe_url
                        logger.info(f"  → 경로 탐색 발견: {probe_path}/{filename}")
                        break

            if found_by_probe:
                new_tg = found_by_probe
                logger.info(f"  → 경로 교정: {old_tg.split('/')[-1]} → {new_tg.split('/')[-2]}/{new_tg.split('/')[-1]}")
            else:
                logger.warning(f"  → 모든 경로 HEAD 실패, 구 URL 유지: {old_tg[:90]}")
                skip_test_fail += 1
                continue

        # 4. DB 업데이트 (telegram_url, pdf_url, download_url 전부 동일하게)
        success = await db.update_telegram_url(
            record_id=report_id,
            telegram_url=new_tg,
            article_title=r.get("article_title"),
            pdf_url=new_tg,
        )

        if success and success.get("status") == "success":
            logger.success(f"  ✓ 복구: {old_tg.split('/')[-1]} → {new_tg.split('/')[-2]}/{new_tg.split('/')[-1]}")
            updated_count += 1
        else:
            logger.error(f"  ✗ DB 업데이트 실패: report_id={report_id}")

        # 진행률
        if idx % 500 == 0 or idx == total:
            logger.info(f"[IBK] 진행률: {idx}/{total} (수정={updated_count}, skip={skip_correct}, test실패={skip_test_fail})")

        await asyncio.sleep(0.05)

    # 5. 최종 요약
    logger.info("")
    logger.info("=" * 60)
    logger.info("[IBK] URL 경로 교정 완료")
    logger.info(f"  전체 대상:   {total}건")
    logger.info(f"  수정 완료:   {updated_count}건")
    logger.info(f"  이미 올바름: {skip_correct}건")
    logger.info(f"  테스트 실패: {skip_test_fail}건")
    logger.info("=" * 60)


if __name__ == "__main__":
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "fix_ibk_db.log")

    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(log_file, level="DEBUG",
               rotation="10 MB", retention="14 days", encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    logger.info("IBK투자증권 URL 후처리 시작")
    asyncio.run(fix_ibk_urls())
    logger.info("IBK투자증권 URL 후처리 종료")
