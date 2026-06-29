#!/usr/bin/env python3
"""
SQLite → PostgreSQL 데이터 마이그레이션 스크립트
Usage: python3 scripts/migrate_sqlite_to_postgres.py [--tables all|firm|main|naver|hankyung] [--truncate]
"""
import sqlite3
import sys
import os
import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import psycopg2
import psycopg2.extras
from loguru import logger

SQLITE_DB = os.path.expanduser("~/sqlite3/telegram.db")

def _clean(v):
    """Strip NUL bytes that PostgreSQL rejects in text columns."""
    if isinstance(v, str):
        return v.replace("\x00", "")
    return v


def get_pg_conn():
    from models.ConfigManager import config
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "ssh_reports_hub"),
        user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
        password=os.getenv("POSTGRES_PASSWORD", config.get_secret("POSTGRES_PASSWORD")),
    )


def migrate_firm_info():
    logger.info("Migrating tbm_sec_firm_info and tbm_sec_firm_board_info...")
    sq = sqlite3.connect(SQLITE_DB)
    sq.row_factory = sqlite3.Row

    firms = [dict(r) for r in sq.execute("SELECT * FROM tbm_sec_firm_info").fetchall()]
    boards = [dict(r) for r in sq.execute("SELECT * FROM tbm_sec_firm_board_info").fetchall()]
    sq.close()

    pg = get_pg_conn()
    cur = pg.cursor()

    # FIRM_INFO
    psycopg2.extras.execute_values(
        cur,
        '''INSERT INTO tbm_sec_firm_info (firm_id, firm_nm, telegram_update_yn)
           VALUES %s
           ON CONFLICT (firm_id) DO UPDATE SET
               firm_nm=EXCLUDED.firm_nm,
               telegram_update_yn=EXCLUDED.telegram_update_yn''',
        [(r["firm_id"], r["firm_nm"], r.get("telegram_update_yn") or r.get("TELEGRAM_UPDATE_YN")) for r in firms],
    )

    # BOARD_INFO
    psycopg2.extras.execute_values(
        cur,
        '''INSERT INTO tbm_sec_firm_board_info
               (firm_id, board_id, board_nm, board_cd, label_nm)
           VALUES %s
           ON CONFLICT (firm_id, board_id) DO UPDATE SET
               board_nm=EXCLUDED.board_nm,
               board_cd=EXCLUDED.board_cd,
               label_nm=EXCLUDED.label_nm''',
        [(r["firm_id"], r["board_id"], 
          r.get("board_nm") or r.get("BOARD_NM"), 
          r.get("board_cd") or r.get("BOARD_CD"), 
          r.get("label_nm") or r.get("LABEL_NM"))
         for r in boards],
    )

    pg.commit()
    cur.close()
    pg.close()
    logger.info(f"  firms: {len(firms)}, boards: {len(boards)}")


def migrate_main(batch_size=5000, truncate=False):
    logger.info('Migrating tbl_sec_reports...')
    sq = sqlite3.connect(SQLITE_DB)
    sq.row_factory = sqlite3.Row

    total = sq.execute("SELECT COUNT(*) FROM data_main_daily_send").fetchone()[0]
    logger.info(f"  total SQLite rows: {total}")

    pg = get_pg_conn()
    cur = pg.cursor()

    if truncate:
        logger.warning('Truncating tbl_sec_reports before migration...')
        cur.execute('TRUNCATE TABLE tbl_sec_reports CASCADE')
        pg.commit()
        logger.info('  Table truncated.')

    # Check already-migrated max report_id
    cur.execute('SELECT COALESCE(MAX(report_id), 0) FROM tbl_sec_reports')
    max_pg_id = cur.fetchone()[0]
    logger.info(f"  PostgreSQL max report_id: {max_pg_id} — starting from there")

    offset = 0
    sq_cur = sq.cursor()
    sq_cur.execute(
        "SELECT report_id,firm_id,board_id,firm_nm,"
        "article_title,article_url,main_ch_send_yn,download_status_yn,"
        "download_url,save_time,reg_dt,writer,key,telegram_url,mkt_tp,"
        "gemini_summary,summary_time,summary_model,archive_status,"
        "pdf_sync_status,pdf_url "
        "FROM data_main_daily_send ORDER BY report_id"
    )

    while True:
        rows = sq_cur.fetchmany(batch_size)
        if not rows:
            break

        records = [
            (
                r["report_id"], r["firm_id"], r["board_id"],
                _clean(r["firm_nm"]), _clean(r["article_title"]),
                _clean(r["article_url"]),
                _clean(r["download_url"]), _clean(r["save_time"]),
                _clean(r["reg_dt"] or ''), _clean(r["writer"] or ''),
                _clean(r["key"]), _clean(r["telegram_url"] or ''),
                _clean(r["mkt_tp"] or 'KR'), _clean(r["gemini_summary"]),
                _clean(r["summary_time"]), _clean(r["summary_model"]),
                _clean(r.get("archive_status") or r.get("ARCHIVE_STATUS") or 'INIT'),
                r["pdf_sync_status"] or 0, _clean(r["pdf_url"] or ''),
                True if r["main_ch_send_yn"] == 'Y' else False,
            )
            for r in rows
        ]

        psycopg2.extras.execute_values(
            cur,
            '''INSERT INTO tbl_sec_reports (
                report_id, firm_id, board_id, firm_nm,
                article_title, article_url,
                download_url, save_time, reg_dt, writer, key, telegram_url, mkt_tp,
                gemini_summary, summary_time, summary_model, archive_status,
                pdf_sync_status, pdf_url, telegram_sent
            ) VALUES %s
            ON CONFLICT (key) DO UPDATE SET
                reg_dt             = EXCLUDED.reg_dt,
                writer             = EXCLUDED.writer,
                mkt_tp             = EXCLUDED.mkt_tp,
                telegram_sent      = EXCLUDED.telegram_sent,
                gemini_summary     = COALESCE(NULLIF(EXCLUDED.gemini_summary,''), tbl_sec_reports.gemini_summary),
                download_url       = COALESCE(NULLIF(EXCLUDED.download_url,''),  tbl_sec_reports.download_url),
                telegram_url       = COALESCE(NULLIF(EXCLUDED.telegram_url,''),  tbl_sec_reports.telegram_url),
                pdf_url            = COALESCE(NULLIF(EXCLUDED.pdf_url,''),        tbl_sec_reports.pdf_url)''',
            records,
            page_size=batch_size,
        )
        pg.commit()
        offset += len(rows)
        logger.info(f"  progress: {offset}/{total}")

    # Advance sequence past max id
    cur.execute('SELECT MAX(report_id) FROM tbl_sec_reports')
    max_id = cur.fetchone()[0] or 0
    cur.execute(f"SELECT setval('tb_sec_reports_report_id_seq', {max_id})")
    pg.commit()

    cur.close()
    pg.close()
    sq.close()
    logger.info(f"  Done. {offset} rows processed.")


def migrate_aux_table(table_name):
    logger.info(f"Migrating {table_name}...")
    sq = sqlite3.connect(SQLITE_DB)
    sq.row_factory = sqlite3.Row

    rows = [dict(r) for r in sq.execute(
        f"SELECT firm_id,board_id,firm_nm,pdf_url,"
        f"article_title,send_user,main_ch_send_yn,save_time,article_url,download_url,writer"
        f" FROM {table_name}"
    ).fetchall()]
    sq.close()

    if not rows:
        logger.info(f"  {table_name}: empty, skipping")
        return

    pg = get_pg_conn()
    cur = pg.cursor()
    psycopg2.extras.execute_values(
        cur,
        f'''INSERT INTO {table_name}
               (firm_id, board_id, firm_nm, pdf_url,
                article_title, send_user, telegram_sent, save_time,
                article_url, download_url, writer)
           VALUES %s
           ON CONFLICT (pdf_url) DO NOTHING''',
        [(r["firm_id"], r["board_id"], r["firm_nm"], r["pdf_url"],
          r["article_title"], r.get("send_user") or r.get("SEND_USER"), 
          True if r["main_ch_send_yn"] == 'Y' else False, r["save_time"],
          r["article_url"], r["download_url"], r["writer"] or '') for r in rows],
    )
    pg.commit()
    cur.close()
    pg.close()
    logger.info(f"  {table_name}: {len(rows)} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", default="all",
                        choices=["all", "firm", "main", "naver", "hankyung"])
    parser.add_argument("--truncate", action="store_true", help="Truncate tables before migration")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv(os.path.expanduser("~/prod/ssh-reports-scraper/.env"))

    if args.tables in ("all", "firm"):
        migrate_firm_info()
    if args.tables in ("all", "main"):
        migrate_main(truncate=args.truncate)
    if args.tables in ("all", "naver"):
        migrate_aux_table("naver_research")
    if args.tables in ("all", "hankyung"):
        migrate_aux_table("hankyungconsen_research")

    logger.info("Migration complete.")
