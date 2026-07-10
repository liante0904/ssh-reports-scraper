"""Scraper-specific PostgreSQL manager compatibility fixes."""

import os
from datetime import datetime, timedelta

from loguru import logger
import psycopg2.extras
from ssh_library import SecReportsManager as LibrarySecReportsManager
from models.report_payload import ReportPayload

_DBFI_GATE_ENV = "DBFI_GATE_URL_PREFIX"
_DBFI_STREAMDOCS_ENV = "DBFI_STREAMDOCS_URL_PREFIX"
_DBFI_VIEWER_BASE_ENV = "DBFI_VIEWER_BASE_URL"


def _join_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}" if base else ""


def _dbfi_viewer_base_from_config() -> str:
    try:
        from models.ConfigManager import config

        dbfi_config = config.get_urls("DBfi_19")
        if isinstance(dbfi_config, dict):
            return (dbfi_config.get("viewer_base_url") or "").rstrip("/")
    except Exception:
        return ""
    return ""


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


class SecReportsManager(LibrarySecReportsManager):
    """Canonical ``report_unique_key`` replaces legacy ``key`` (dropped 2026-07)."""

    REPORTS_READ_VIEW = "public.v_sec_reports_canonical"

    @classmethod
    def dbfi_url_prefixes(cls) -> tuple[str, str]:
        viewer_base = (
            os.getenv(_DBFI_VIEWER_BASE_ENV, "").rstrip("/")
            or _dbfi_viewer_base_from_config()
        )
        gate_prefix = os.getenv(_DBFI_GATE_ENV, "").rstrip("/") or _join_url(viewer_base, "pv/gate")
        streamdocs_prefix = (
            os.getenv(_DBFI_STREAMDOCS_ENV, "").rstrip("/")
            or _join_url(viewer_base, "streamdocs/v4/documents")
        )
        return gate_prefix, streamdocs_prefix

    @classmethod
    def dbfi_ready_condition(cls, table_alias=None):
        gate_prefix, streamdocs_prefix = cls.dbfi_url_prefixes()
        if gate_prefix and streamdocs_prefix:
            condition = (
                "(\n"
                "    firm_id != 19\n"
                "    OR (\n"
                "        firm_id = 19\n"
                f"        AND telegram_url LIKE '{_sql_literal(gate_prefix)}%%'\n"
                f"        AND pdf_url LIKE '{_sql_literal(streamdocs_prefix)}%%'\n"
                "    )\n"
                ")"
            )
        else:
            condition = "(firm_id != 19)"
        if not table_alias:
            return condition
        for column in ("firm_id", "telegram_url", "pdf_url"):
            condition = condition.replace(column, f"{table_alias}.{column}")
        return condition

    @classmethod
    def dbfi_ready_condition_for_read_view(cls):
        return cls.dbfi_ready_condition()

    def _reset_duplicate_send_yn(self, json_data_list, table_name):
        """Do not mutate send status during scraper upsert.

        Historical code reset existing rows to unsent when the same
        title/date/firm appeared with a different key. That makes URL
        canonicalization issues turn into Telegram re-sends. Dedupe should be
        handled by canonical keys or read-side grouping, not by toggling
        delivery state.
        """
        return

    def insert_json_data_list(self, json_data_list, table_name=None):
        """scraper 전용 INSERT — ssh_library 부모 클래스 오버라이드.

        부모와의 차이:
        - telegram_sent = false 하드코딩 (신규 레포트는 미발송)
        - ON CONFLICT 시 telegram_sent 보존 (COALESCE로 기존값 유지)
        - pdf_url fallback: telegram_url 사용
        """
        if table_name is None:
            table_name = self.table_name
        table_name = self._TABLE_MAP.get(table_name, table_name)

        # Do not reset send status during upsert; see _reset_duplicate_send_yn.
        self._reset_duplicate_send_yn(json_data_list, table_name)

        records = [
            ReportPayload.from_scraper(entry).to_db_record()
            for entry in json_data_list
        ]

        if not records:
            logger.info("[SCRAPER-DB] Data inserted: 0 rows, updated: 0 rows.")
            return 0, 0

        sql = f"""
            INSERT INTO {table_name} (
                firm_id, board_id, firm_nm, report_date,
                article_title, telegram_url, pdf_url,
                writer, mkt_tp, report_unique_key, telegram_sent, save_at
            ) VALUES %s
            ON CONFLICT (report_unique_key) DO UPDATE SET
                firm_id             = EXCLUDED.firm_id,
                board_id            = EXCLUDED.board_id,
                firm_nm             = EXCLUDED.firm_nm,
                article_title       = EXCLUDED.article_title,
                report_date         = EXCLUDED.report_date,
                writer              = EXCLUDED.writer,
                mkt_tp              = EXCLUDED.mkt_tp,
                telegram_url        = COALESCE(NULLIF(EXCLUDED.telegram_url, ''), {table_name}.telegram_url),
                pdf_url             = COALESCE(NULLIF(EXCLUDED.pdf_url, ''), {table_name}.pdf_url),
                telegram_sent       = COALESCE({table_name}.telegram_sent, false),
                save_at             = {table_name}.save_at
            RETURNING report_unique_key, (xmax = 0) AS inserted
        """

        inserted = updated = 0
        new_keys = []
        conn = self.get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    for start in range(0, len(records), 1000):
                        chunk = records[start:start + 1000]
                        psycopg2.extras.execute_values(
                            cur,
                            sql,
                            chunk,
                            page_size=len(chunk),
                        )
                        for key_value, is_insert in cur.fetchall():
                            if is_insert:
                                inserted += 1
                                if key_value:
                                    new_keys.append(key_value)
                            else:
                                updated += 1
        finally:
            conn.close()

        self._last_inserted_keys = new_keys
        logger.info(
            f"[SCRAPER-DB] Data inserted: {inserted} rows, updated: {updated} rows."
        )
        return inserted, updated

    def mark_reports_sent(self, fetched_rows, match_by_url=False):
        """Mark Telegram delivery complete — telegram_sent only.

        기본적으로 report_id만 마킹한다. match_by_url=True일 때만
        같은 telegram_url을 가진 다른 row도 함께 마킹한다.
        DBfi처럼 URL 기반 중복이 불가피한 케이스에만 True를 사용한다.

        모든 발송 상태는 telegram_sent 단일 컬럼으로 관리한다.
        select_reports_ready_for_telegram, _broadcast_ga_reports 모두 telegram_sent만 체크.
        """
        for row in fetched_rows or []:
            if match_by_url and row.get("telegram_url"):
                self._execute(
                    f"""
                    UPDATE {self.table_name}
                    SET telegram_sent = true
                    WHERE report_id = %s OR telegram_url = %s
                    """,
                    (row["report_id"], row["telegram_url"]),
                )
            else:
                self._execute(
                    f"""
                    UPDATE {self.table_name}
                    SET telegram_sent = true
                    WHERE report_id = %s
                    """,
                    (row["report_id"],),
                )
        return {"status": "success"}

    async def update_telegram_url(
        self,
        record_id,
        telegram_url,
        article_title=None,
        pdf_url=None,
        pdf_file_url=None,
    ):
        """Update resolved report URLs with scraper naming compatibility."""
        if pdf_url is None:
            pdf_url = pdf_file_url
        return await super().update_telegram_url(
            record_id=record_id,
            telegram_url=telegram_url,
            article_title=article_title,
            pdf_url=pdf_url,
        )

    async def select_reports_ready_for_telegram(self, date_str=None, type=None):
        """텔레그램 발송 대상 레포트 조회. DBfi streamdocs PDF 확정 완료 + 미발송.

        type='send': 미발송 + DBfi-ready
        type='download': 발송 완료 + pdf_sync 미완료
        """
        if date_str is None:
            query_date = datetime.now().strftime("%Y-%m-%d")
            query_report_date_end = (datetime.now() + timedelta(days=2)).date()
        else:
            query_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            query_report_date_end = (datetime.strptime(date_str, "%Y%m%d") + timedelta(days=2)).date()

        three_days_ago = (datetime.now() - timedelta(days=3)).date()

        if type == "send":
            cond = f"""
                (telegram_sent IS NOT true)
                AND {self.dbfi_ready_condition_for_read_view()}
                AND (
                    firm_id = 19
                    OR COALESCE(telegram_url, '') <> ''
                    OR COALESCE(pdf_url, '') <> ''
                )
            """
        else:
            cond = "telegram_sent IS true AND pdf_sync_status != 2"

        sql = f"""
        SELECT DISTINCT ON (
            firm_id,
            article_title
        )
            report_id,
            firm_id AS firm_id,
            board_id AS board_id,
            firm_nm,report_date,
            article_title,pdf_url,writer,save_at,scraped_at,
            report_unique_key,
            CASE
                WHEN firm_id = 19 THEN pdf_url
                ELSE telegram_url
            END AS telegram_url
        FROM   {self.REPORTS_READ_VIEW}
        WHERE  save_at::date >= (%s::date - 2)
          AND  save_at::date <= %s
          AND  report_date >= %s
          AND  report_date <= %s
          AND  {cond}
        ORDER BY
            firm_id,
            article_title,
            save_at
        """
        return self._fetchall(sql, (query_date, query_date, three_days_ago, query_report_date_end))

    # Backward-compat alias. 새 코드는 select_reports_ready_for_telegram 사용.
    async def daily_update_data(self, date_str=None, fetched_rows=None, type=None):
        """Mark sent status and mirror it to the legacy main channel flag."""
        if type not in ("send", "download"):
            raise ValueError("Invalid type. Must be 'send' or 'download'.")

        if type != "send":
            return await super().daily_update_data(
                date_str=date_str,
                fetched_rows=fetched_rows,
                type=type,
            )

        return self.mark_reports_sent(fetched_rows)

    def fetch_keyword_reports(self, date: str, keyword: str, user_id: str):
        """Fetch unsent keyword reports only after DBfi PDF URL is finalized."""
        sql = f"""
            SELECT r.report_id, r.firm_nm, r.article_title,
                   CASE
                       WHEN r.firm_id = 19 THEN r.pdf_url
                       ELSE r.telegram_url
                   END AS telegram_url,
                   r.save_at
            FROM {self.table_name} r
            LEFT JOIN tbl_report_send_history h
                   ON r.report_id = h.report_id AND h.user_id = %s
            WHERE (r.article_title ILIKE %s OR r.writer ILIKE %s)
              AND r.save_at::date = %s
              AND h.id IS NULL
              AND {self.dbfi_ready_condition("r")}
            ORDER BY r.save_at ASC, r.firm_nm ASC
        """
        keyword_param = f"%{keyword}%"
        return self._fetchall(sql, (user_id, keyword_param, keyword_param, date))

    def update_keyword_send_user(self, date: str, keyword: str, user_id: str):
        """Record sent keyword reports using the same DBfi-ready predicate as fetch."""
        fetch_sql = f"""
            SELECT r.report_id
            FROM {self.table_name} r
            LEFT JOIN tbl_report_send_history h
                   ON r.report_id = h.report_id AND h.user_id = %s
            WHERE (r.article_title ILIKE %s OR r.writer ILIKE %s)
              AND r.save_at::date = %s
              AND h.id IS NULL
              AND {self.dbfi_ready_condition("r")}
        """
        keyword_param = f"%{keyword}%"
        reports = self._fetchall(fetch_sql, (user_id, keyword_param, keyword_param, date))

        inserted_count = 0
        for report in reports:
            insert_sql = """
                INSERT INTO tbl_report_send_history (report_id, user_id, keyword)
                VALUES (%s, %s, %s)
                ON CONFLICT (report_id, user_id) DO NOTHING
            """
            result = self._execute(insert_sql, (report["report_id"], user_id, keyword))
            if result["affected_rows"] > 0:
                inserted_count += result["affected_rows"]

        if inserted_count > 0:
            logger.info(f"[SSH-LIBRARY] tbl_report_send_history: {inserted_count} rows inserted for user {user_id}.")
        return {"affected_rows": inserted_count}
