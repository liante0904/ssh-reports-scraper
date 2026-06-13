#!/usr/bin/env python3
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scrapers.kiwoom_core import scrape_kiwoom

K = "KIWOOM_URLS_JSON"
NM = "키움증권"
if __name__ == "__main__":
    raw = os.getenv(K, "")
    if not raw: print(f"[{NM}] FATAL: {K} not set", file=sys.stderr), sys.exit(1)
    result = scrape_kiwoom(cfg=json.loads(raw))
    print(f"[{NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
