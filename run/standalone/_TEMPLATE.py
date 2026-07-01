#!/usr/bin/env python3
"""[증권사명] standalone scraper — GitHub Actions 전용.

=== 새 증권사 추가 방법 (2026-06-11 개선판) ===
1. scrapers/{firm_key}_core.py 작성 — 순수 스크래핑 로직 (환경 의존성 없음)
2. 이 파일을 복사 → run/standalone/{firm_key}.py
3. 아래 [SETTING] 3개 값 + import 경로만 수정
4. GitHub Secrets에 {FIRM_KEY}_URLS_JSON 등록
5. .github/workflows/scrape-{firm_key}.yml 생성

=== 아키텍처 ===
  scrapers/{firm}_core.py  ← 순수 스크래핑 로직 (requests만 의존)
       ↙              ↘
  modules/{Firm}.py    run/standalone/{firm}.py
  (서버, async)         (GA, sync)
  ConfigManager →      env var →
  core 함수 호출        core 함수 호출
  → FirmInfo 보강       → stdout JSON

=== 필수 필드 (모든 증권사 공통) ===
json_data_list.append({
    "firm_id": FIRM_ID,
    "board_id": 0,
    "firm_nm": FIRM_NM,
    "reg_dt": "20260609",           # YYYYMMDD
    "download_url": "https://...",
    "telegram_url": "https://...",
    "article_title": "리포트 제목",
    "writer": "작성자",
    "mkt_tp": "KR",                  # "KR" or "GLOBAL"
    "report_unique_key": "https://...",  # 중복제거용 유니크 키 (URL 권장, key 컬럼 deprecated)
    "save_time": "2026-06-09T10:00:00",
})
"""
import json
import os
import sys

# repo root → sys.path (scrapers 패키지 import 가능하게)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# ── [SETTING] 3개 값만 수정 ──
from scrapers.XXX_core import scrape_XXX   # ← 코어 모듈 경로
FIRM_NM = "○○증권"
URLS_ENV_KEY = "XXX_URLS_JSON"             # 예: "KB_URLS_JSON"


if __name__ == "__main__":
    raw = os.getenv(URLS_ENV_KEY, "")
    if not raw:
        print(f"[{FIRM_NM}] FATAL: {URLS_ENV_KEY} not set", file=sys.stderr)
        sys.exit(1)

    urls = json.loads(raw)
    if not urls:
        print(f"[{FIRM_NM}] FATAL: {URLS_ENV_KEY} is empty", file=sys.stderr)
        sys.exit(1)

    result = scrape_XXX(url=urls[0])
    print(f"[{FIRM_NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
