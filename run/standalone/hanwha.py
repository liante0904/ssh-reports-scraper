#!/usr/bin/env python3
"""Hanwha standalone — GA 전용. scrapers/hanwha_core.py 사용."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scrapers.hanwha_core import scrape_hanwha

FIRM_NM, KEY = "한화투자증권", "HANWHA_URLS_JSON"

if __name__ == "__main__":
    raw = os.getenv(KEY, "")
    if not raw: print(f"[{FIRM_NM}] FATAL: {KEY} not set", file=sys.stderr), sys.exit(1)
    result = scrape_hanwha(cfg=json.loads(raw)
    print(f"[{FIRM_NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
