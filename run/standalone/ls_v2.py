#!/usr/bin/env python3
"""
LS증권 GA standalone v2 — key 기반 신규 필터링 + detail 포함.

=== GA v1과의 차이 ===
v1: list scraping만 실행 → download_url/telegram_url 비어있음 → 서버에서 LS_detail()로 후처리
v2: API로 기존 key 조회 → 신규 건만 LS_detail() → 완전한 JSON → 서버는 바로 insert

=== 실행 흐름 ===
1. API에서 기존 LS key + writer 목록 다운로드
2. LS_checkNewArticle()로 모든 게시판 list scraping
3. 신규 = scraped_keys - existing_keys
4. 신규 건만 LS_detail() → 상세페이지에서 PDF URL 추출
5. 완전한 JSON stdout 출력 → GA artifact → SCP → 서버 insert
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from modules.LS_0 import LS_checkNewArticle, LS_detail
from models.FirmInfo import FirmInfo

NM = "LS증권"

# ── Config ──
LS_LIST_MAX_PAGES = int(os.getenv("LS_LIST_MAX_PAGES", "2"))
LS_DETAIL_TIMEOUT = int(os.getenv("LS_DETAIL_TIMEOUT", "120"))


def fetch_existing_keys() -> tuple[set, dict]:
    """GitHub Release에서 암호화된 LS key 파일 다운로드 → 복호화 → key set 반환"""
    import subprocess, tempfile

    enc_key = os.getenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET", "")
    if not enc_key:
        print(f"[{NM}] WARN: TELEGRAM_BOT_TOKEN not set", file=sys.stderr)
        return set(), {}

    enc_file = "data/ls_existing_keys.enc"
    json_file = "data/ls_existing_keys.json"
    os.makedirs("data", exist_ok=True)

    # GitHub Release에서 다운로드
    try:
        subprocess.run([
            "gh", "release", "download", "ls-keys-data",
            "--repo", "liante0904/ssh-reports-scraper",
            "--pattern", "*.enc", "--dir", "data",
        ], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print(f"[{NM}] WARN: Release download failed", file=sys.stderr)
        return set(), {}

    # 복호화 (기존 공유키 사용 — 새 시크릿 없음)
    try:
        subprocess.run([
            "openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-d",
            "-pass", f"pass:{enc_key[:64]}",
            "-in", enc_file, "-out", json_file,
        ], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print(f"[{NM}] WARN: Decryption failed", file=sys.stderr)
        return set(), {}

    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        keys = set(data.get("keys", []))
        key_writer_map = data.get("key_writer_map", {})
        print(f"[{NM}] Keys loaded: {len(keys)} existing", file=sys.stderr)
        return keys, key_writer_map
    except Exception as e:
        print(f"[{NM}] WARN: {e}", file=sys.stderr)
        return set(), {}


def filter_new_articles(all_articles: list[dict], existing_keys: set) -> list[dict]:
    """기존 key 제외 → 신규 article만 반환"""
    new_articles = []
    for a in all_articles:
        k = a.get("key") or a.get("report_unique_key")
        if k and k not in existing_keys:
            new_articles.append(a)
    skipped = len(all_articles) - len(new_articles)
    if skipped:
        print(f"[{NM}] Filter: {skipped} existing, {len(new_articles)} new articles", file=sys.stderr)
    return new_articles


async def fetch_details(new_articles: list[dict]) -> list[dict]:
    """신규 건만 LS_detail() → PDF URL 추출"""
    if not new_articles:
        return []

    KST = timezone(timedelta(hours=9))
    now = datetime.now(KST).strftime("%Y%m%d")

    # 오늘 날짜 건만 detail 처리 (과거 건은 서버 enrichment에 맡김)
    today_articles = [
        a for a in new_articles
        if str(a.get("reg_dt", "")).startswith(now[:6])  # 당월
    ]
    if len(today_articles) < len(new_articles):
        print(f"[{NM}] Detail: {len(today_articles)} today articles (skipping {len(new_articles) - len(today_articles)} older)",
              file=sys.stderr)

    if not today_articles:
        # 오늘자 신규가 없으면 전체 반환 (URL 빈값)
        return new_articles

    firm_info = FirmInfo(sec_firm_order=0, article_board_order=0)
    try:
        enriched = await asyncio.wait_for(
            LS_detail(today_articles, firm_info=firm_info),
            timeout=LS_DETAIL_TIMEOUT,
        )
        print(f"[{NM}] Detail: {len(enriched)} articles enriched with PDF URLs", file=sys.stderr)
    except asyncio.TimeoutError:
        print(f"[{NM}] WARN: LS_detail timeout ({LS_DETAIL_TIMEOUT}s). Returning articles without PDF URLs.",
              file=sys.stderr)
        enriched = today_articles

    # detail 처리 안 한 구버전 건 + 처리된 오늘 건 합치기
    older = [a for a in new_articles if a not in today_articles]
    result = enriched + older

    # save_time 갱신
    for a in result:
        a["save_time"] = datetime.now(KST).isoformat()

    return result


if __name__ == "__main__":
    print(f"[{NM}] LS GA v2 started", file=sys.stderr)

    # 1. 기존 key 조회
    existing_keys, key_writer_map = fetch_existing_keys()

    # 2. List scraping (기존 LS_checkNewArticle 그대로)
    all_articles = LS_checkNewArticle(page=1, max_pages=LS_LIST_MAX_PAGES)
    print(f"[{NM}] List: {len(all_articles)} articles scraped (all boards)", file=sys.stderr)

    # 3. 신규 필터링
    new_articles = filter_new_articles(all_articles, existing_keys)

    # 4. Detail fetching (신규 건만)
    final_articles = asyncio.run(fetch_details(new_articles))

    # 5. 출력
    with_pdf = sum(1 for a in final_articles if a.get("download_url"))
    print(f"[{NM}] Done: {len(final_articles)} articles ({with_pdf} with PDF URLs)", file=sys.stderr)
    json.dump(final_articles, sys.stdout, ensure_ascii=False, indent=2)
