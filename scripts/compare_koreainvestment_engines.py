#!/usr/bin/env python3
"""Read-only, bounded side-by-side comparison of KoreaInvestment browser engines.

Run with: uv run --extra browser-comparison python scripts/compare_koreainvestment_engines.py
"""
import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DB_BACKEND", "static")

from dotenv import load_dotenv  # noqa: E402

load_dotenv(override=False)

from modules.Koreainvestment_13 import (  # noqa: E402
    Koreainvestment_playwright_checkNewArticle,
    Koreainvestment_selenium_checkNewArticle,
)


COMPARE_FIELDS = (
    "firm_id", "board_id", "firm_nm", "report_date", "telegram_url",
    "pdf_file_url", "article_title", "writer", "report_unique_key", "mkt_tp",
)


def fingerprint(article: dict) -> tuple:
    return tuple(article.get(field) for field in COMPARE_FIELDS)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-pages", type=int, default=1, help="pages per category; default: 1")
    parser.add_argument("--allow-empty", action="store_true", help="allow both engines to return no articles")
    args = parser.parse_args()
    if args.max_pages < 1:
        parser.error("--max-pages must be at least 1")

    os.environ["KOREAMAX_PAGES"] = str(args.max_pages)
    selenium_articles = await Koreainvestment_selenium_checkNewArticle()
    playwright_articles = await Koreainvestment_playwright_checkNewArticle()

    selenium_rows = Counter(map(fingerprint, selenium_articles))
    playwright_rows = Counter(map(fingerprint, playwright_articles))
    selenium_only = selenium_rows - playwright_rows
    playwright_only = playwright_rows - selenium_rows

    result = {
        "max_pages_per_category": args.max_pages,
        "selenium_count": len(selenium_articles),
        "playwright_count": len(playwright_articles),
        "selenium_only_count": sum(selenium_only.values()),
        "playwright_only_count": sum(playwright_only.values()),
        "save_at_excluded": True,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if not args.allow_empty and not selenium_articles and not playwright_articles:
        print("Both engines returned no articles; refusing to treat this as a pass.", file=sys.stderr)
        return 2
    if selenium_only or playwright_only:
        for label, rows in (("Selenium only", selenium_only), ("Playwright only", playwright_only)):
            for row, count in rows.items():
                print(f"{label} x{count}: title={row[6]!r}, key={row[8]!r}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
