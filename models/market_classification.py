"""Shared market classification at the scraper-to-database boundary.

Source adapters may provide a market value when their source board is
authoritative.  This module keeps those declarations, but corrects them when
the title itself contains stronger domestic or overseas ticker evidence.
"""

from __future__ import annotations

import re
from typing import Any


# These are dedicated overseas source boards confirmed from the production
# board catalog.  Positional Hana board IDs come from the broker URL list;
# other IDs are the canonical broker board IDs persisted with each row.  Keep
# this explicit rather than classifying broad strategy/industry boards from a
# Korean keyword in their titles.
SOURCE_GLOBAL_BOARDS: dict[int, frozenset[int]] = {
    0: frozenset({9}),             # LS: 해외리서치
    1: frozenset({3, 5}),          # 신한: 해외주식, 해외 채권
    3: frozenset({14, 15, 16}),    # 하나: 글로벌 투자/산업/기업분석
    4: frozenset({7, 11}),         # KB: Global Insights, Asia Headline
    5: frozenset({2}),             # 삼성: 해외 분석
    9: frozenset({2}),             # 현대차: 해외주식
    10: frozenset({3}),            # 키움: 미국/선진국
    18: frozenset({2}),            # IM: 기업분석(해외)
    25: frozenset({4, 5}),         # IBK: 해외기업분석, 글로벌ETF
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

    Evidence priority is: dedicated source board > domestic ticker > foreign
    ticker > scraper declaration/default.  A declaration such as ``US`` or
    ``JP`` is retained; inferred overseas rows use ``GLOBAL``.
    """
    declared = str(declared_market_type or "KR").strip().upper() or "KR"
    title = str(article_title or "")

    try:
        is_dedicated_global_board = int(board_id) in SOURCE_GLOBAL_BOARDS.get(
            int(firm_id), frozenset()
        )
    except (TypeError, ValueError):
        is_dedicated_global_board = False
    if is_dedicated_global_board:
        return "GLOBAL"

    if _DOMESTIC_TICKER_RE.search(title):
        return "KR"
    if _FOREIGN_TICKER_RE.search(title):
        return declared if declared != "KR" else "GLOBAL"

    return declared
