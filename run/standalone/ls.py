#!/usr/bin/env python3
import asyncio, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from modules.LS_0 import LS_checkNewArticle

NM = "LS증권"
if __name__ == "__main__":
    # LS_checkNewArticle is a synchronous function, so call it directly instead of using asyncio.run
    result = LS_checkNewArticle()
    print(f"[{NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
