#!/usr/bin/env python3
import asyncio, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from modules.Koreainvestment_13 import Koreainvestment_selenium_checkNewArticle

NM = "한국투자증권"
if __name__ == "__main__":
    result = asyncio.run(Koreainvestment_selenium_checkNewArticle())
    print(f"[{NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
