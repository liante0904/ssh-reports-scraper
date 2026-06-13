#!/usr/bin/env python3
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scrapers.shinyoung_core import scrape_shinyoung

K = SHINYOUNG_URLS_JSON
if __name__ == "__main__":
    raw = os.getenv(K, "")
    if not raw: print(f"[] FATAL: {K} not set", file=sys.stderr), sys.exit(1)
    result = scrape_shinyoung(cfg=json.loads(raw))
    print(f"[] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
