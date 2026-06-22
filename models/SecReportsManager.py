"""Scraper-specific PostgreSQL manager compatibility fixes."""

from loguru import logger
import psycopg2.extras
from ssh_library import SecReportsManager as LibrarySecReportsManager


class SecReportsManager(LibrarySecReportsManager):
    """Keep legacy ``key`` and canonical ``report_unique_key`` in sync."""

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
        if table_name is None:
            table_name = self.table_name
        table_name = self._TABLE_MAP.get(table_name, table_name)

        # ── 중앙 차단: 뉴스/미디어는 PostgreSQL에 insert 금지 ──
        EXCLUDED_FIRMS = {"네이버", "조선비즈"}
        original_count = len(json_data_list)
        json_data_list = [e for e in json_data_list if e.get("firm_nm") not in EXCLUDED_FIRMS]
        if len(json_data_list) < original_count:
            logger.info(
                f"[SCRAPER-DB] Stripped {original_count - len(json_data_list)} rows (네이버/조선비즈 excluded)"
            )

        # Do not reset send status during upsert; see _reset_duplicate_send_yn.
        self._reset_duplicate_send_yn(json_data_list, table_name)

        records = []
        for entry in json_data_list:
            unique_key = (
                entry.get("report_unique_key")
                or entry.get("key")
                or entry.get("article_url")
                or ""
            )
            save_time = entry.get("save_time", "")
            save_at = entry.get("save_at")
            if not save_at and save_time:
                try:
                    from datetime import datetime
                    save_at = datetime.fromisoformat(str(save_time).replace("Z", "+00:00"))
                except Exception:
                    pass

            records.append((
                entry.get("sec_firm_order"),
                entry.get("article_board_order"),
                entry.get("firm_nm"),
                entry.get("reg_dt", ""),
                entry.get("article_title"),
                entry.get("article_url"),
                entry.get("download_url"),
                entry.get("telegram_url"),
                entry.get("pdf_url") or entry.get("telegram_url"),
                entry.get("writer", ""),
                entry.get("mkt_tp", "KR"),
                unique_key,
                unique_key,
                save_time,
                False,
                save_at,
            ))

        if not records:
            logger.info("[SCRAPER-DB] Data inserted: 0 rows, updated: 0 rows.")
            return 0, 0

        sql = f"""
            INSERT INTO {table_name} (
                sec_firm_order, article_board_order, firm_nm, reg_dt,
                article_title, article_url, download_url,
                telegram_url, pdf_url, writer, mkt_tp,
                key, report_unique_key, save_time, telegram_sent, save_at
            ) VALUES %s
            ON CONFLICT (report_unique_key) DO UPDATE SET
                sec_firm_order      = EXCLUDED.sec_firm_order,
                article_board_order = EXCLUDED.article_board_order,
                firm_nm             = EXCLUDED.firm_nm,
                article_title       = EXCLUDED.article_title,
                reg_dt              = EXCLUDED.reg_dt,
                writer              = EXCLUDED.writer,
                mkt_tp              = EXCLUDED.mkt_tp,
                download_url        = COALESCE(NULLIF(EXCLUDED.download_url, ''), {table_name}.download_url),
                telegram_url        = COALESCE(NULLIF(EXCLUDED.telegram_url, ''), {table_name}.telegram_url),
                pdf_url             = COALESCE(NULLIF(EXCLUDED.pdf_url, ''), {table_name}.pdf_url),
                telegram_sent       = COALESCE({table_name}.telegram_sent, false),
                save_at             = COALESCE(EXCLUDED.save_at, {table_name}.save_at)
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

    def mark_reports_sent(self, fetched_rows):
        """Mark Telegram delivery complete (both is_sent + telegram_sent).

        daily_select_data checks is_sent, while _broadcast_ga_reports checks
        telegram_sent. Both must be set to prevent duplicate sending.
        """
        for row in fetched_rows or []:
            telegram_url = row.get("telegram_url")
            if telegram_url:
                self._execute(
                    f"""
                    UPDATE {self.table_name}
                    SET telegram_sent = true, is_sent = true
                    WHERE telegram_url = %s
                    """,
                    (telegram_url,),
                )
            else:
                self._execute(
                    f"""
                    UPDATE {self.table_name}
                    SET telegram_sent = true, is_sent = true
                    WHERE report_id = %s
                    """,
                    (row["report_id"],),
                )
        return {"status": "success"}

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
