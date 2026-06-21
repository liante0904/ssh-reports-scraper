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
            legacy_key = entry.get("key") or unique_key
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
                "N",
                entry.get("download_url"),
                entry.get("telegram_url"),
                entry.get("pdf_url") or entry.get("telegram_url"),
                entry.get("writer", ""),
                entry.get("mkt_tp", "KR"),
                legacy_key,
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
                article_title, article_url, main_ch_send_yn, download_url,
                telegram_url, pdf_url, writer, mkt_tp, key,
                report_unique_key, save_time, is_sent, save_at
            ) VALUES %s
            ON CONFLICT (report_unique_key) DO UPDATE SET
                sec_firm_order      = EXCLUDED.sec_firm_order,
                article_board_order = EXCLUDED.article_board_order,
                firm_nm             = EXCLUDED.firm_nm,
                article_title       = EXCLUDED.article_title,
                reg_dt              = EXCLUDED.reg_dt,
                writer              = EXCLUDED.writer,
                mkt_tp              = EXCLUDED.mkt_tp,
                key                 = EXCLUDED.key,
                download_url        = COALESCE(NULLIF(EXCLUDED.download_url, ''), {table_name}.download_url),
                telegram_url        = COALESCE(NULLIF(EXCLUDED.telegram_url, ''), {table_name}.telegram_url),
                pdf_url             = COALESCE(NULLIF(EXCLUDED.pdf_url, ''), {table_name}.pdf_url),
                main_ch_send_yn     = CASE
                                        WHEN COALESCE({table_name}.is_sent, false)
                                          OR {table_name}.main_ch_send_yn = 'Y'
                                        THEN 'Y'
                                        ELSE COALESCE({table_name}.main_ch_send_yn, 'N')
                                      END,
                is_sent             = COALESCE({table_name}.is_sent, false)
                                      OR {table_name}.main_ch_send_yn = 'Y',
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
        """Mark Telegram delivery complete.

        This is the only method in this manager that should turn send status
        on. Scraper upserts must not use incoming scrape payloads to change
        delivery state.
        """
        for row in fetched_rows or []:
            telegram_url = row.get("telegram_url")
            if telegram_url:
                self._execute(
                    f"""
                    UPDATE {self.table_name}
                    SET is_sent = true, main_ch_send_yn = 'Y'
                    WHERE telegram_url = %s
                    """,
                    (telegram_url,),
                )
            else:
                self._execute(
                    f"""
                    UPDATE {self.table_name}
                    SET is_sent = true, main_ch_send_yn = 'Y'
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
