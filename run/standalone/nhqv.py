#!/usr/bin/env python3
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scrapers.nhqv_core import scrape_nhqv

K = "NHQV_URLS_JSON"
NM = "NH투자증권"
if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    raw = os.getenv(K, "")
    if not raw: print(f"[{NM}] FATAL: {K} not set", file=sys.stderr), sys.exit(1)
    result = scrape_nhqv(cfg=json.loads(raw), target_date=target_date)
    print(f"[{NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
