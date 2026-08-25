#!/usr/bin/env python3
"""Re-resolve LS rows whose stored delivery/PDF URL is an upload image.

Dry-run is the default.  ``--execute`` updates only rows for which LS_detail
finds a verifier-approved msg.ls-sec.co.kr PDF.  PNG/JPG URLs are never copied
to pdf_url or telegram_url.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from urllib.parse import unquote, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.db_factory import get_db
from modules.LS_0 import (
    LS_MSG_PREFIX,
    PROXIES,
    LS_detail,
    _ls_pdf_candidate_urls,
    reconstruct_msg_url_from_db,
)
from utils.ls_pdf_verifier import verify_ls_pdf_candidate


def load_targets(db, limit: int, date_from: str | None):
    clauses = [
        "firm_id = 0",
        "(lower(COALESCE(telegram_url, '')) ~ '\\.(png|jpg|jpeg)([?#]|$)' OR lower(COALESCE(pdf_url, '')) ~ '\\.(png|jpg|jpeg)([?#]|$)')",
    ]
    params = []
    if date_from:
        clauses.append("report_date >= %s")
        params.append(date_from)
    limit_sql = "" if limit <= 0 else " LIMIT %s"
    if limit > 0:
        params.append(limit)
    return db._fetchall(
        f"""
        SELECT report_id, article_title, writer, report_date,
               report_unique_key, telegram_url, pdf_url
        FROM tbl_sec_reports
        WHERE {' AND '.join(clauses)}
        ORDER BY report_id ASC
        {limit_sql}
        """,
        tuple(params),
    )


async def run(limit: int, date_from: str | None, execute: bool, skip_detail_fallback: bool):
    db = get_db()
    rows = load_targets(db, limit, date_from)
    print(f"targets={len(rows)} execute={execute}")
    if not rows:
        return

    # Clear legacy image values in the in-memory payload so LS_detail is
    # forced to inspect the detail page and/or verified msg candidates.
    for row in rows:
        row["legacy_image_url"] = row.get("telegram_url") or row.get("pdf_url") or ""
        row["telegram_url"] = ""
        row["pdf_file_url"] = ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept": "application/pdf,*/*",
    }
    resolved_rows = []
    detail_rows = []
    candidate_semaphore = asyncio.Semaphore(5)

    async def verify_candidate(candidate: str, row: dict):
        async with candidate_semaphore:
            result = await asyncio.to_thread(
                verify_ls_pdf_candidate,
                candidate,
                row.get("article_title", ""),
                headers,
                PROXIES,
            )
            return candidate, result

    for row in rows:
        legacy_url = row.get("legacy_image_url") or ""
        filename = unquote(os.path.basename(urlparse(legacy_url).path))
        candidates = _ls_pdf_candidate_urls(filename)
        results = await asyncio.gather(*(verify_candidate(candidate, row) for candidate in candidates))
        match = next(((candidate, result) for candidate, result in results if result.matched), None)
        if match:
            candidate, _ = match
            row["telegram_url"] = candidate
            row["pdf_file_url"] = candidate
            resolved_rows.append(row)
            print(f"RESOLVED_DIRECT report_id={row['report_id']} url={candidate}")
        else:
            detail_rows.append(row)

    print(f"direct_resolved={len(resolved_rows)} detail_fallback={len(detail_rows)}")

    async def persist(rows_to_update):
        for row in rows_to_update:
            await db.update_telegram_url(
                record_id=row["report_id"],
                telegram_url=row["telegram_url"],
                article_title=row.get("article_title"),
                pdf_file_url=row["pdf_file_url"],
            )

    if execute and resolved_rows:
        await persist(resolved_rows)
        print(f"updated_direct={len(resolved_rows)}")

    detail_resolved_rows = []
    unresolved_rows = []
    for row in detail_rows:
        inferred = await reconstruct_msg_url_from_db(
            row,
            headers,
            date_window_days=None,
        )
        if inferred:
            row["telegram_url"] = inferred
            row["pdf_file_url"] = inferred
            detail_resolved_rows.append(row)
            print(f"RESOLVED_HISTORY report_id={row['report_id']} url={inferred}")
        else:
            unresolved_rows.append(row)

    print(f"history_resolved={len(detail_resolved_rows)} page_detail_fallback={len(unresolved_rows)}")
    if skip_detail_fallback:
        print(f"detail_fallback_skipped={len(unresolved_rows)}")
        if execute and detail_resolved_rows:
            await persist(detail_resolved_rows)
            print(f"updated_history={len(detail_resolved_rows)}")
        return

    for row in await LS_detail(unresolved_rows, db=None):
        url = str(row.get("pdf_file_url") or row.get("telegram_url") or "")
        if url.startswith(LS_MSG_PREFIX) and url.lower().endswith(".pdf"):
            detail_resolved_rows.append(row)
            print(f"RESOLVED report_id={row['report_id']} url={url}")
        else:
            print(f"UNRESOLVED report_id={row['report_id']} reason=no_verified_pdf")

    if not execute:
        print(f"would_update={len(resolved_rows) + len(detail_resolved_rows)}")
        return
    if detail_resolved_rows:
        await persist(detail_resolved_rows)
    print(f"updated_detail={len(detail_resolved_rows)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=100, help="rows to inspect; 0 means all")
    parser.add_argument("--date-from", help="inclusive report_date, YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--execute", action="store_true", help="write verified PDF URLs to production DB")
    parser.add_argument("--skip-detail-fallback", action="store_true", help="do not fetch LS detail pages after direct filename resolution")
    args = parser.parse_args()
    asyncio.run(run(args.limit, args.date_from, args.execute, args.skip_detail_fallback))


if __name__ == "__main__":
    main()
