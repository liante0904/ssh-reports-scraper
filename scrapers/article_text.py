"""Bounded extraction of broker-provided HTML summaries.

This module deliberately handles HTML summaries only. PDF full-text extraction
belongs to the archive pipeline and must not be silently mixed into
``tbl_sec_reports.article_text``.
"""

from __future__ import annotations

import re
from html import unescape

import requests
from urllib.parse import urljoin


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


def fetch_new_site_summaries(reports: list[dict], new_keys: set[str]) -> dict[str, str]:
    """Fetch at most one detail page for each newly inserted supported report.

    List scraping must never call this for historical/existing rows. Failed
    detail pages are intentionally returned as absent rather than retried here;
    retries belong to an explicit operator job, not the every-run scraper.
    """
    extracted: dict[str, str] = {}
    for report in reports:
        key = str(report.get("report_unique_key") or "")
        if key not in new_keys or report.get("article_text"):
            continue
        if report.get("firm_id") == 11:  # DS: list source_url is a relative detail link.
            detail_url = urljoin("https://www.ds-sec.co.kr/", str(report.get("source_url") or ""))
            text = fetch_ds_summary(detail_url)
            if text:
                extracted[key] = text
    return extracted
