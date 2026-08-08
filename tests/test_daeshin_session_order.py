import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.Daeshin_17 import _fetch_pages_sequentially


def test_daeshin_pages_are_posted_in_order():
    seen = []

    async def fetch_page(page):
        seen.append(page)

    asyncio.run(_fetch_pages_sequentially(fetch_page, range(1, 5)))

    assert seen == [1, 2, 3, 4]
