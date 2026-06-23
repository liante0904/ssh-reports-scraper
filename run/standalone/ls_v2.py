#!/usr/bin/env python3
"""
LS증권 GA standalone v2 — key 필터링 + GA 직접 detail (DB/WARP 불필요)

=== GA v1과의 차이 ===
v1: list만 → URL 빈값 → 서버 LS_detail() 후처리
v2: 기존 key 제외 → 신규만 detail → CDN URL 직접 구성 → 완전한 JSON

=== Detail URL 해결 전략 (GA 전용, DB 없음) ===
1순위: 상세페이지 HTML에서 upload filename 파싱 → CDN URL 변환 → HEAD 확인 (직접, no proxy)
2순위: export된 writer_emp_map으로 CDN URL 직접 구성 → HEAD probing
3순위: 포기 (서버 enrichment에 맡김)
"""
import asyncio
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from modules.LS_0 import LS_checkNewArticle, upload_filename_to_cdn_url

NM = "LS증권"
LS_MSG_PREFIX = "https://msg.ls-sec.co.kr/eum/K_"
LS_LIST_MAX_PAGES = int(os.getenv("LS_LIST_MAX_PAGES", "2"))
LS_DETAIL_TIMEOUT = int(os.getenv("LS_DETAIL_TIMEOUT", "120"))
LS_DETAIL_DELAY = float(os.getenv("LS_DETAIL_DELAY", "0.5"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
}


def fetch_existing_keys() -> tuple[set, dict, dict]:
    """GitHub Release → 복호화 → keys set + writer→emp_id map"""
    enc_key = os.getenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET", "")
    if not enc_key:
        print(f"[{NM}] WARN: TELEGRAM_BOT_TOKEN not set", file=sys.stderr)
        return set(), {}, {}

    gh_token = os.getenv("GH_TOKEN", "")
    enc_file = "data/ls_keys.enc"
    json_file = "data/ls_keys.json"
    os.makedirs("data", exist_ok=True)

    # curl로 GitHub Release API 직접 호출 (gh CLI 불필요)
    try:
        auth_header = f"Authorization: Bearer {gh_token}" if gh_token else ""
        subprocess.run(
            f'curl -fsSL {"-H \"" + auth_header + "\"" if auth_header else ""} '
            f'-o {enc_file} '
            f'https://github.com/liante0904/ssh-reports-scraper/releases/download/ls-keys-data/ls_keys.enc',
            shell=True, check=True, capture_output=True,
        )
    except subprocess.CalledProcessError:
        print(f"[{NM}] WARN: Release download failed (release not created yet)", file=sys.stderr)
        return set(), {}, {}

    # 복호화
    try:
        subprocess.run([
            "openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-d",
            "-pass", f"pass:{enc_key[:64]}",
            "-in", enc_file, "-out", json_file,
        ], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print(f"[{NM}] WARN: Decryption failed", file=sys.stderr)
        return set(), {}, {}

    try:
        with open(json_file, encoding="utf-8") as f:
            data = json.load(f)
        keys = set(data.get("keys", []))
        writer_emp = data.get("writer_emp_map", {})
        print(f"[{NM}] {len(keys)} keys, {len(writer_emp)} writer→emp_id mappings", file=sys.stderr)
        return keys, data.get("key_writer_map", {}), writer_emp
    except Exception as e:
        print(f"[{NM}] WARN: {e}", file=sys.stderr)
        return set(), {}, {}


def filter_new(all_articles, existing_keys):
    new = [a for a in all_articles if (a.get("key") or a.get("report_unique_key")) not in existing_keys]
    print(f"[{NM}] Filter: {len(all_articles) - len(new)} existing, {len(new)} new", file=sys.stderr)
    return new


def resolve_cdn_url_from_detail(article_url: str, writer_emp_map: dict) -> str | None:
    """
    GA 전용 detail resolver.
    1순위: detail HTML에서 upload filename 직접 파싱 → CDN URL → HEAD 검증
    2순위: writer_emp_map으로 CDN URL 구성 → HEAD probing
    """
    # detail 페이지 가져오기 (직접 접속, GA 클린IP)
    try:
        resp = requests.get(article_url, headers=HEADERS, verify=False, timeout=30)
        resp.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    writer_name = ""

    # 작성자 추출
    for tr in soup.select("tr"):
        th = tr.select_one("th")
        td = tr.select_one("td")
        if th and td and th.get_text(strip=True) == "필명":
            writer_name = td.get_text(strip=True)
            break

    # 첨부파일 영역에서 upload filename 찾기
    upload_name = ""
    for a_tag in soup.select("td a"):
        txt = a_tag.get_text(strip=True)
        if re.search(r'\d+_\d+_\d{8}\.\w+$', txt):
            upload_name = txt
            break

    if not upload_name:
        for img in soup.select("img"):
            alt = img.get("alt", "")
            if re.search(r'\d+_\d+_\d{8}\.\w+$', alt):
                upload_name = alt
                break
        if not upload_name:
            src = soup.select_one("img")
            if src:
                basename = os.path.basename(src.get("src", ""))
                if re.search(r'\d+_\d+_\d{8}\.\w+$', basename):
                    upload_name = basename

    # 1순위: upload filename → CDN URL 변환 → HEAD 확인 (no WARP)
    if upload_name:
        cdn_url = upload_filename_to_cdn_url(upload_name)
        if cdn_url:
            try:
                r = requests.head(cdn_url, headers=HEADERS, verify=False, timeout=10)
                if r.status_code == 200:
                    return cdn_url
            except Exception:
                pass

    # 2순위: writer_emp_map으로 CDN URL 구성 + probing
    if writer_name and writer_name in writer_emp_map:
        emp_id = writer_emp_map[writer_name]
        # LS_detail과 동일한 날짜 추출 시도: article URL에서 seq 파라미터 찾기
        # 실패하면 최근 seq로 probing
        for seq_offset in range(500):
            seq = 1000 + seq_offset  # 충분히 큰 범위
            try_url = f"{LS_MSG_PREFIX}20260623_{emp_id}_{seq}.pdf"
            try:
                r = requests.head(try_url, headers=HEADERS, verify=False, timeout=5)
                if r.status_code == 200:
                    return try_url
            except Exception:
                continue

    return None


def resolve_batch(articles, writer_emp_map):
    """순차적으로 detail resolve (GA 2-core 제한 고려)"""
    resolved = []
    with_url = 0
    for i, a in enumerate(articles):
        article_url = a.get("key") or a.get("report_unique_key")
        if not article_url:
            resolved.append(a)
            continue

        url = resolve_cdn_url_from_detail(article_url, writer_emp_map)
        if url:
            a["download_url"] = url
            a["telegram_url"] = url
            a["pdf_url"] = url
            a["article_url"] = url
            with_url += 1

        resolved.append(a)
        time.sleep(LS_DETAIL_DELAY)

        if (i + 1) % 10 == 0:
            print(f"[{NM}] Detail progress: {i+1}/{len(articles)} ({with_url} PDFs)", file=sys.stderr)

    return resolved


if __name__ == "__main__":
    print(f"[{NM}] LS GA v2 started", file=sys.stderr)

    # 1. 기존 key + writer_emp_map
    existing_keys, key_writer_map, writer_emp_map = fetch_existing_keys()

    # 2. List scraping
    all_articles = LS_checkNewArticle(page=1, max_pages=LS_LIST_MAX_PAGES)
    print(f"[{NM}] List: {len(all_articles)} scraped", file=sys.stderr)

    # 3. 신규 필터
    new_articles = filter_new(all_articles, existing_keys)

    # 4. Detail resolve (GA 직접, DB 없음)
    if new_articles:
        KST = timezone(timedelta(hours=9))
        today = datetime.now(KST).strftime("%Y%m")
        today_articles = [a for a in new_articles if str(a.get("reg_dt", "")).startswith(today)]
        older = [a for a in new_articles if a not in today_articles]

        if today_articles:
            resolved = resolve_batch(today_articles, writer_emp_map)
        else:
            resolved = []

        final_articles = resolved + older
    else:
        final_articles = []

    for a in final_articles:
        a["save_time"] = datetime.now(timezone(timedelta(hours=9))).isoformat()

    with_url = sum(1 for a in final_articles if a.get("download_url"))
    print(f"[{NM}] Done: {len(final_articles)} ({with_url} with PDF)", file=sys.stderr)
    json.dump(final_articles, sys.stdout, ensure_ascii=False, indent=2)
