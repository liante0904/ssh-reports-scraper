"""Bounded extraction of broker-provided HTML summaries.

This module deliberately handles HTML summaries only. PDF full-text extraction
belongs to the archive pipeline and must not be silently mixed into
``tbl_sec_reports.article_text``.
"""

from __future__ import annotations

import re
from html import unescape

import requests


def extract_ds_summary(html: str, *, max_chars: int = 10_000) -> str:
    """Return DS's report-body summary or an empty string when absent."""
    match = re.search(
        r'<(?:div|section)[^>]+id=["\']bo_v_con["\'][^>]*>(.*?)</(?:div|section)>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    text = re.sub(r"<[^>]+>", " ", match.group(1))
    text = re.sub(r"\s+", " ", unescape(text)).strip()
    return text[:max_chars] if len(text) > 30 else ""


def fetch_ds_summary(url: str, *, timeout: int = 10) -> str:
    """Fetch one DS detail page; source failures are represented by empty text."""
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            verify=False,
            timeout=timeout,
        )
        response.raise_for_status()
        return extract_ds_summary(response.text)
    except requests.RequestException:
        return ""
