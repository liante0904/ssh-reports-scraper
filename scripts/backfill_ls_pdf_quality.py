#!/usr/bin/env python3
"""Verify and repair LS PDF URLs for a bounded report-date window.

Dry-run is the default. With ``--execute``, only URLs whose response is a PDF
and whose first page matches the stored article title are written to the DB.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from datetime import date, timedelta
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.db_factory import get_db
from modules import LS_0
from utils.ls_pdf_verifier import verify_ls_pdf_candidate


def _report_date(value) -> str:
    return re.sub(r"\D", "", str(value or ""))[:8]


def _filename_from_url(url: str) -> str:
    path = unquote(urlparse(str(url or "")).path)
    basename = os.path.basename(path)
    match = re.fullmatch(r"K_(\d{8})_(.+)_(\d+)\.pdf", basename, re.IGNORECASE)
    if match:
        report_day, employee, sequence = match.groups()
        return f"{employee}_{sequence}_{report_day}.pdf"
    return basename


def _load_targets(db, date_from: str, limit: int) -> list[dict]:
    params: list[object] = [date_from]
    limit_sql = ""
    if limit > 0:
        limit_sql = " LIMIT %s"
        params.append(limit)
    return db._fetchall(
        f"""
        SELECT report_id, report_date, article_title, writer,
               report_unique_key, telegram_url, pdf_url
        FROM tbl_sec_reports
        WHERE firm_id = 0 AND report_date >= %s
        ORDER BY report_date ASC NULLS LAST, report_id ASC
        {limit_sql}
        """,
        tuple(params),
    )


def _detail_candidates(article: dict, html: str, search_days: int) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []
    filenames: set[str] = set()
    detail_url = str(article.get("report_unique_key") or "")

    for row in soup.select("tr"):
        th = row.select_one("th")
        if not th or th.get_text(strip=True) != "첨부파일":
            continue
        for anchor in row.select("td a"):
            direct_url = LS_0._ls_attachment_url(anchor.get("href", ""), detail_url)
            if direct_url:
                candidates.append(direct_url)
            text = anchor.get_text(strip=True)
            if re.search(r"[^/\\\s]+_\d+_\d{8}\.\w+$", text):
                filenames.add(text)
            elif anchor.get("href"):
                filename = _filename_from_url(anchor["href"])
                if re.search(r"_\d+_\d{8}\.\w+$", filename):
                    filenames.add(filename)

    for img in soup.select(
        "#contents > div.tbViewCon > div > html > body > p > img, "
        "#contents > div.tbViewCon > div > p > img"
    ):
        filename = img.get("alt") or os.path.basename(urlparse(img.get("src", "")).path)
        filename = unquote(filename)
        if re.search(r"_\d+_\d{8}\.\w+$", filename):
            filenames.add(filename)

    report_month = _report_date(article.get("report_date"))[:6]
    for filename in filenames:
        if filename.lower().endswith(".pdf") and report_month:
            candidates.append(
                f"{LS_0.LS_UPLOAD_PREFIX}/B{report_month}/"
                f"{LS_0.urllib.parse.quote(filename)}"
            )
        candidates.extend(
            LS_0._ls_pdf_candidate_urls(
                filename,
                search_days=search_days,
                preferred_date=_report_date(article.get("report_date")),
            )
        )
    return list(dict.fromkeys(candidates))


async def _verify(url: str, article: dict, headers: dict) -> tuple[bool, str]:
    result = await asyncio.to_thread(
        verify_ls_pdf_candidate,
        url,
        str(article.get("article_title") or ""),
        headers,
        LS_0.PROXIES,
    )
    return result.matched, result.reason


async def run(date_from: str, limit: int, search_days: int, execute: bool) -> None:
    db = get_db()
    rows = _load_targets(db, date_from, limit)
    print(f"targets={len(rows)} date_from={date_from} execute={execute}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept": "application/pdf,*/*",
        "Referer": LS_0.LS_HOME_URL,
    }
    verified_cache: dict[tuple[str, str], tuple[bool, str]] = {}
    counters = {"already_valid": 0, "resolved": 0, "unresolved": 0}

    async def check(url: str, article: dict) -> tuple[bool, str]:
        cache_key = (url, str(article.get("article_title") or ""))
        if cache_key not in verified_cache:
            try:
                verified_cache[cache_key] = await _verify(url, article, headers)
            except Exception as exc:
                verified_cache[cache_key] = (False, f"verification error: {type(exc).__name__}")
        return verified_cache[cache_key]

    for row in rows:
        article = dict(row)
        article["report_date"] = _report_date(article.get("report_date"))
        stored_urls = list(dict.fromkeys(
            str(article.get(field) or "").strip()
            for field in ("pdf_url", "telegram_url")
            if str(article.get(field) or "").strip()
        ))
        current_url = stored_urls[0] if stored_urls else ""

        valid_existing = ""
        for stored_url in stored_urls:
            matched, reason = await check(stored_url, article)
            if matched:
                valid_existing = stored_url
                break
            print(f"INVALID_CURRENT report_id={article['report_id']} reason={reason}")

        if valid_existing:
            counters["already_valid"] += 1
            if any(url != valid_existing for url in stored_urls):
                counters["resolved"] += 1
                if execute:
                    await db.update_telegram_url(
                        record_id=article["report_id"],
                        telegram_url=valid_existing,
                        article_title=article.get("article_title"),
                        pdf_file_url=valid_existing,
                    )
                    print(f"ALIGNED_FIELDS report_id={article['report_id']} url={valid_existing}")
                else:
                    print(f"WOULD_ALIGN_FIELDS report_id={article['report_id']} url={valid_existing}")
                continue
            continue

        candidates: list[str] = []
        filename = _filename_from_url(current_url) if current_url else ""
        if re.search(r"_\d+_\d{8}\.\w+$", filename):
            candidates.extend(
                LS_0._ls_pdf_candidate_urls(
                    filename,
                    search_days=search_days,
                    preferred_date=article["report_date"],
                )
            )

        detail_key = str(article.get("report_unique_key") or "")
        if detail_key:
            try:
                html = await LS_0.fetch(None, detail_key, headers)
                if html:
                    candidates.extend(_detail_candidates(article, html, search_days))
            except Exception as exc:
                print(f"DETAIL_ERROR report_id={article['report_id']} error={type(exc).__name__}")

        candidates = list(dict.fromkeys(url for url in candidates if url != current_url))
        resolved_url = ""
        reasons: dict[str, int] = {}
        for candidate in candidates:
            matched, reason = await check(candidate, article)
            if matched:
                resolved_url = candidate
                break
            reasons[reason] = reasons.get(reason, 0) + 1

        if not resolved_url:
            # Try the writer sequence history only after source attachment
            # candidates have failed. This routine verifies the PDF title too.
            try:
                resolved_url = await LS_0.reconstruct_msg_url_from_db(
                    article, headers, date_window_days=0
                ) or ""
            except Exception as exc:
                reasons[f"history inference error: {type(exc).__name__}"] = 1

        if resolved_url:
            counters["resolved"] += 1
            print(f"RESOLVED report_id={article['report_id']} url={resolved_url}")
            if execute:
                await db.update_telegram_url(
                    record_id=article["report_id"],
                    telegram_url=resolved_url,
                    article_title=article.get("article_title"),
                    pdf_file_url=resolved_url,
                )
        else:
            counters["unresolved"] += 1
            reason_summary = ", ".join(
                f"{key}={value}" for key, value in sorted(reasons.items())
            ) or "no candidates"
            print(
                f"UNRESOLVED report_id={article['report_id']} "
                f"candidates={len(candidates)} reasons={reason_summary}"
            )

    print(
        "summary="
        f"already_valid:{counters['already_valid']} "
        f"resolved:{counters['resolved']} "
        f"unresolved:{counters['unresolved']} "
        f"updated:{counters['resolved'] if execute else 0}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=92,
        help="report-date lookback window; default 92 days",
    )
    parser.add_argument("--date-from", help="inclusive report_date, YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--limit", type=int, default=0, help="maximum rows; 0 means all")
    parser.add_argument("--search-days", type=int, default=10, help="candidate date window on each side")
    parser.add_argument("--execute", action="store_true", help="write only verified URLs to production DB")
    args = parser.parse_args()
    if args.days < 1 or args.limit < 0 or args.search_days < 0:
        parser.error("days must be positive; limit and search-days must be non-negative")
    date_from = re.sub(r"\D", "", args.date_from) if args.date_from else (
        date.today() - timedelta(days=args.days)
    ).strftime("%Y%m%d")
    if len(date_from) != 8:
        parser.error("date-from must be YYYY-MM-DD or YYYYMMDD")
    asyncio.run(run(date_from, args.limit, args.search_days, args.execute))


if __name__ == "__main__":
    main()
