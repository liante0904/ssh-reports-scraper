"""Small PostgreSQL data access layer used only by keyword-alert."""

import os

import psycopg2
import psycopg2.extras
from loguru import logger

from models.ConfigManager import config


class KeywordAlertDB:
    """Implements the keyword-alert queries without importing ssh_library."""

    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_REPORT_DB") or os.getenv(
            "POSTGRES_DB", "ssh_reports_hub"
        )
        self.keyword_database = os.getenv("POSTGRES_KEYWORD_DB") or self.database
        self.user = os.getenv("POSTGRES_USER", "ssh_reports_hub")
        self.password = os.getenv("POSTGRES_PASSWORD", "")

    def _connect(self, database=None):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=database or self.database,
            user=self.user,
            password=self.password,
            connect_timeout=10,
        )

    def _fetchall(self, sql, params=(), database=None):
        with self._connect(database) as conn, conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def _dbfi_ready_condition(self, alias="r"):
        viewer_base = os.getenv("DBFI_VIEWER_BASE_URL", "").rstrip("/")
        urls = config.get_urls("DBfi_19")
        if isinstance(urls, dict):
            viewer_base = (urls.get("viewer_base_url") or viewer_base).rstrip("/")
        gate = os.getenv("DBFI_GATE_URL_PREFIX", "").rstrip("/") or (
            f"{viewer_base}/pv/gate" if viewer_base else ""
        )
        streamdocs = os.getenv("DBFI_STREAMDOCS_URL_PREFIX", "").rstrip("/") or (
            f"{viewer_base}/streamdocs/v4/documents" if viewer_base else ""
        )
        if not gate or not streamdocs:
            return f"({alias}.firm_id != 19)"
        # URL prefixes are configuration values, escaped as SQL literals as in
        # the main manager's DBfi readiness predicate.
        gate = gate.replace("'", "''")
        streamdocs = streamdocs.replace("'", "''")
        return (
            f"({alias}.firm_id != 19 OR ({alias}.firm_id = 19 "
            f"AND {alias}.telegram_url LIKE '{gate}%%' "
            f"AND {alias}.pdf_url LIKE '{streamdocs}%%'))"
        )

    def load_keywords_from_db(self):
        sql = """
            SELECT user_id, keyword
            FROM tbl_sec_reports_alert_keywords
            WHERE is_active = true
            ORDER BY user_id, created_at ASC
        """
        try:
            rows = self._fetchall(sql, database=self.keyword_database)
        except Exception as exc:
            # Preserve the existing behavior when the optional keyword table
            # has not been provisioned yet.
            logger.warning(f"Unable to load keyword subscriptions: {exc}")
            return {}
        grouped = {}
        for row in rows:
            grouped.setdefault(str(row["user_id"]), []).append(
                {"keyword": row["keyword"], "code": "", "timestamp": ""}
            )
        return grouped

    def fetch_keyword_reports(self, date, keyword, user_id):
        sql = f"""
            SELECT r.report_id, r.firm_nm, r.article_title,
                   r.telegram_url, r.save_at
            FROM tbl_sec_reports r
            LEFT JOIN tbl_report_send_history h
                   ON r.report_id = h.report_id AND h.user_id = %s
            WHERE (r.article_title ILIKE %s OR r.writer ILIKE %s)
              AND r.save_at::date = %s
              AND h.id IS NULL
              AND NULLIF(BTRIM(r.telegram_url), '') IS NOT NULL
              AND {self._dbfi_ready_condition('r')}
            ORDER BY r.save_at ASC, r.firm_nm ASC
        """
        term = f"%{keyword}%"
        return self._fetchall(sql, (user_id, term, term, date))

    def update_keyword_send_user(self, date, keyword, user_id):
        sql = f"""
            SELECT r.report_id
            FROM tbl_sec_reports r
            LEFT JOIN tbl_report_send_history h
                   ON r.report_id = h.report_id AND h.user_id = %s
            WHERE (r.article_title ILIKE %s OR r.writer ILIKE %s)
              AND r.save_at::date = %s
              AND h.id IS NULL
              AND NULLIF(BTRIM(r.telegram_url), '') IS NOT NULL
              AND {self._dbfi_ready_condition('r')}
        """
        term = f"%{keyword}%"
        rows = self._fetchall(sql, (user_id, term, term, date))
        if not rows:
            return {"affected_rows": 0}
        inserted_count = 0
        with self._connect() as conn, conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO tbl_report_send_history (report_id, user_id, keyword)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (report_id, user_id) DO NOTHING
                    """,
                    (row["report_id"], user_id, keyword),
                )
                inserted_count += cur.rowcount
        return {"affected_rows": inserted_count}
