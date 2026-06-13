#!/usr/bin/env python3
"""KB Securities standalone scraper — GitHub Actions 전용.

URL은 GA Secrets KB_URLS_JSON 환경변수에서 주입.
공통 코어 scrapers/kb_core.py 사용 → 서버 모듈과 로직 통일.
"""
import json
import os
import sys

# repo root를 sys.path에 추가 → scrapers 패키지 import 가능하게
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scrapers.kb_core import scrape_kb

FIRM_NM = "KB증권"
URLS_ENV_KEY = "KB_URLS_JSON"


if __name__ == "__main__":
    raw = os.getenv(URLS_ENV_KEY, "")
    if not raw:
        print(f"[{FIRM_NM}] FATAL: {URLS_ENV_KEY} not set", file=sys.stderr)


    result = scrape_kb(cfg=json.loads(raw)
    print(f"[{FIRM_NM}] total {len(result)} articles collected", file=sys.stderr)

    # 게시판별 통계 (디버깅용)
    from collections import Counter
    board_stats = Counter(r["article_board_order"] for r in result)
    for order, cnt in sorted(board_stats.items()):
        print(f"[{FIRM_NM}]   board_order={order}: {cnt} articles", file=sys.stderr)

    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
