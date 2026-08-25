#!/usr/bin/env python3
"""Re-resolve LS rows whose stored delivery/PDF URL is an upload image.

Dry-run is the default.  ``--execute`` updates only rows for which an inferred
msg.ls-sec.co.kr PDF passes content verification.  PNG/JPG URLs are never
copied to pdf_url or telegram_url.  This script never fetches LS detail pages.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from urllib.parse import unquote, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.db_factory import get_db
from modules.LS_0 import PROXIES, _ls_pdf_candidate_urls
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
        ORDER BY report_date DESC NULLS LAST, report_id DESC
        {limit_sql}
        """,
        tuple(params),
    )


async def run(limit: int, date_from: str | None, execute: bool):
    db = get_db()
    rows = load_targets(db, limit, date_from)
    print(f"targets={len(rows)} execute={execute}")
    if not rows:
        return

    for row in rows:
        row["legacy_image_url"] = row.get("telegram_url") or row.get("pdf_url") or ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept": "application/pdf,*/*",
    }
    resolved_rows = []
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
    print(f"direct_resolved={len(resolved_rows)} unresolved={len(rows) - len(resolved_rows)}")

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

    if not execute:
        print(f"would_update={len(resolved_rows)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=100, help="rows to inspect; 0 means all")
    parser.add_argument("--date-from", help="inclusive report_date, YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--execute", action="store_true", help="write verified PDF URLs to production DB")
    args = parser.parse_args()
    asyncio.run(run(args.limit, args.date_from, args.execute))


if __name__ == "__main__":
    main()
