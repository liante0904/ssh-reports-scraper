#!/usr/bin/env python3
"""
Export recent SQLite rows to JSON, upsert them into PostgreSQL, and compare.

Default range is the last 3 calendar days including today, based on save_time.
"""
import argparse
import json
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from loguru import logger

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


ROOT = Path(__file__).resolve().parents[1]
SQLITE_COLUMNS = [
    "report_id",
    "sec_firm_order",
    "article_board_order",
    "firm_nm",
    "article_title",
    "article_url",
    "main_ch_send_yn",
    "is_sent",
    "download_url",
    "save_time",
    "reg_dt",
    "writer",
    "key",
    "telegram_url",
    "mkt_tp",
    "gemini_summary",
    "summary_time",
    "summary_model",
    "ARCHIVE_STATUS",
    "pdf_sync_status",
    "pdf_url",
]

PG_COLUMNS = [
    "report_id",
    '"sec_firm_order"',
    '"article_board_order"',
    '"firm_nm"',
    '"article_title"',
    '"article_url"',
    '"main_ch_send_yn"',
    '"is_sent"',
    '"download_url"',
    '"save_time"',
    '"reg_dt"',
    '"writer"',
    '"key"',
    '"telegram_url"',
    '"mkt_tp"',
    '"gemini_summary"',
    '"summary_time"',
    '"summary_model"',
    '"ARCHIVE_STATUS"',
    '"pdf_sync_status"',
    '"pdf_url"',
]

COMPARE_COLUMNS = [c for c in SQLITE_COLUMNS if c != "report_id"]


def clean(value):
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def comparable(value):
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ")
    return str(value)


def get_pg_conn():
    from models.ConfigManager import config

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "ssh_reports_hub"),
        user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
        password=os.getenv("POSTGRES_PASSWORD", config.get_secret("POSTGRES_PASSWORD")),
    )


def export_sqlite(days, output_path):
    sqlite_db = os.path.expanduser(os.getenv("SQLITE_DB_PATH", "~/sqlite3/telegram.db"))
    start_date = (datetime.now() - timedelta(days=days - 1)).strftime("%Y-%m-%d")
    columns = ",".join(SQLITE_COLUMNS)
    query = f"""
        SELECT {columns}
        FROM data_main_daily_send
        WHERE DATE(save_time) >= DATE(?)
          AND "key" IS NOT NULL
          AND "key" != ''
        ORDER BY save_time DESC, report_id DESC
    """

    conn = sqlite3.connect(sqlite_db)
    conn.row_factory = sqlite3.Row
    try:
        rows = [dict(row) for row in conn.execute(query, (start_date,)).fetchall()]
    finally:
        conn.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=json_default)

    logger.info(f"SQLite export range: save_time >= {start_date}")
    logger.info(f"SQLite rows exported: {len(rows)}")
    logger.info(f"JSON written: {output_path}")
    return rows, start_date


def upsert_postgres(rows):
    if not rows:
        return 0, 0

    values = [
        tuple(clean(row.get(col)) for col in SQLITE_COLUMNS)
        for row in rows
    ]
    insert_cols = ",".join(PG_COLUMNS)
    update_assignments = ",".join(
        f"{col}=EXCLUDED.{col}"
        for col in PG_COLUMNS
        if col not in ("report_id", '"key"')
    )
    sql = f"""
        INSERT INTO "tbl_sec_reports" ({insert_cols})
        VALUES %s
        ON CONFLICT ("key") DO UPDATE SET {update_assignments}
        RETURNING (xmax = 0) AS inserted
    """

    inserted = updated = 0
    conn = get_pg_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
                for (is_inserted,) in cur.fetchall():
                    if is_inserted:
                        inserted += 1
                    else:
                        updated += 1

                cur.execute('SELECT MAX(report_id) FROM "tbl_sec_reports"')
                max_id = cur.fetchone()[0] or 0
                cur.execute("SELECT setval('tb_sec_reports_report_id_seq', %s)", (max_id,))
    finally:
        conn.close()

    logger.info(f"PostgreSQL upsert complete: inserted={inserted}, updated={updated}")
    return inserted, updated


def fetch_postgres_by_keys(keys):
    if not keys:
        return {}

    select_cols = ",".join(PG_COLUMNS)
    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f'SELECT {select_cols} FROM "tbl_sec_reports" WHERE "key" = ANY(%s)',
                (list(keys),),
            )
            return {row["key"]: dict(row) for row in cur.fetchall()}
    finally:
        conn.close()


def compare_rows(sqlite_rows):
    sqlite_by_key = {row["key"]: row for row in sqlite_rows}
    pg_by_key = fetch_postgres_by_keys(sqlite_by_key.keys())

    missing_in_pg = sorted(set(sqlite_by_key) - set(pg_by_key))
    mismatches = []
    for key, sqlite_row in sqlite_by_key.items():
        pg_row = pg_by_key.get(key)
        if not pg_row:
            continue
        for column in COMPARE_COLUMNS:
            if comparable(sqlite_row.get(column)) != comparable(pg_row.get(column)):
                mismatches.append(
                    {
                        "key": key,
                        "column": column,
                        "sqlite": comparable(sqlite_row.get(column)),
                        "postgres": comparable(pg_row.get(column)),
                    }
                )
                break

    logger.info(f"Compare keys: sqlite={len(sqlite_by_key)}, postgres={len(pg_by_key)}")
    if missing_in_pg:
        logger.error(f"Missing in PostgreSQL: {len(missing_in_pg)}")
        logger.error(f"Missing sample: {missing_in_pg[:5]}")
    if mismatches:
        logger.error(f"Column mismatches: {len(mismatches)}")
        logger.error(f"Mismatch sample: {mismatches[:5]}")

    if missing_in_pg or mismatches:
        raise SystemExit(1)

    logger.success("SQLite and PostgreSQL recent data match by key and compared columns.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    load_dotenv(ROOT / ".env", override=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(args.output) if args.output else ROOT / "json" / f"sqlite_recent_{args.days}d_{timestamp}.json"

    rows, _ = export_sqlite(args.days, output_path)
    upsert_postgres(rows)
    compare_rows(rows)


if __name__ == "__main__":
    main()
