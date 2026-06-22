#!/usr/bin/env python3
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scrapers.news_core import scrape_all_news

if __name__ == "__main__":
    try:
        result = asyncio.run(scrape_all_news())
        print(f"[news] total {len(result)} articles collected", file=sys.stderr)
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[news] FATAL error: {e}", file=sys.stderr)
        sys.exit(1)
