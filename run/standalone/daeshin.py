#!/usr/bin/env python3
import asyncio, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from modules.Daeshin_17 import Daeshin_checkNewArticle

NM = "대신증권"
if __name__ == "__main__":
    result = asyncio.run(Daeshin_checkNewArticle())
    print(f"[{NM}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
