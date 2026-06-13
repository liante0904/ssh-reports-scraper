#!/usr/bin/env python3
"""NH Investment standalone scraper — GitHub Actions 전용.
공통 코어 scrapers/nhqv_core.py 사용."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scrapers.nhqv_core import scrape_nhqv

URLS_ENV_KEY = "NHQV_URLS_JSON"
FIRM_NM = "NH투자증권"

if __name__ == "__main__":
    raw = os.getenv(URLS_ENV_KEY, "")
    if not raw:
        print(f"[{FIRM_NM}] FATAL: {URLS_ENV_KEY} not set", file=sys.stderr)

    result = scrape_nhqv(cfg=json.loads(raw)
    print(f"[{FIRM_NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
