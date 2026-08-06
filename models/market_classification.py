"""Shared market classification at the scraper-to-database boundary.

Source adapters may provide a market value when their source board is
authoritative.  This module keeps those declarations, but corrects them when
the title itself contains stronger domestic or overseas ticker evidence.
"""

from __future__ import annotations

import re
from typing import Any


# Hana's configured URL list is positional.  These source-board positions are
# the broker's dedicated overseas boards: global strategy, global industry,
# and global company research.  Keep this small and explicit rather than
# treating a generic Korean keyword as overseas coverage.
SOURCE_GLOBAL_BOARDS: dict[int, frozenset[int]] = {
    3: frozenset({14, 15, 16}),
}

# A domestic ticker is decisive even when a source board is normally global.
_DOMESTIC_TICKER_RE = re.compile(r"\([^)]*\.\s*K[QS](?:[^A-Z]|$)", re.IGNORECASE)

# Two-letter exchange suffixes observed in report titles.  KOSPI/KOSDAQ
# (.KS/.KQ) are deliberately excluded and handled by _DOMESTIC_TICKER_RE.
_FOREIGN_TICKER_RE = re.compile(
    r"\([^)]*\.\s*(?:US|JP|HK|CH|CN|TW|FP|GR|LN|NA|SW|AU|IN|SP|SS|ID)(?:[^A-Z]|$)",
    re.IGNORECASE,
)


def classify_market_type(
    *,
    firm_id: Any,
    board_id: Any,
    article_title: Any,
    declared_market_type: Any,
) -> str:
    """Return the canonical market type for a scraper payload.

    Evidence priority is: domestic ticker > foreign ticker > dedicated source
    board > scraper declaration/default.  A declaration such as ``US`` or
    ``JP`` is retained; inferred overseas rows use ``GLOBAL``.
    """
    declared = str(declared_market_type or "KR").strip().upper() or "KR"
    title = str(article_title or "")

    if _DOMESTIC_TICKER_RE.search(title):
        return "KR"
    if _FOREIGN_TICKER_RE.search(title):
        return declared if declared != "KR" else "GLOBAL"

    try:
        is_dedicated_global_board = int(board_id) in SOURCE_GLOBAL_BOARDS.get(
            int(firm_id), frozenset()
        )
    except (TypeError, ValueError):
        is_dedicated_global_board = False
    if is_dedicated_global_board:
        return "GLOBAL"

    return declared
