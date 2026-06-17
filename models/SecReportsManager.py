"""Scraper-specific PostgreSQL manager compatibility fixes."""

from loguru import logger
import psycopg2.extras
from ssh_library import SecReportsManager as LibrarySecReportsManager


class SecReportsManager(LibrarySecReportsManager):
    """Keep legacy ``key`` and canonical ``report_unique_key`` in sync."""

    def _reset_duplicate_send_yn(self, json_data_list, table_name):
        """title+reg_dt+firm_nm 같고 key 다르면 기존 레코드의 send_yn을 N으로 초기화"""
        from collections import defaultdict
        groups = defaultdict(list)
        for entry in json_data_list:
            t = (entry.get("article_title","").strip(),
                 entry.get("reg_dt","").strip(),
                 entry.get("firm_nm","").strip())
            if all(t):
                groups[t].append(entry.get("key") or entry.get("report_unique_key",""))

        conn = self.get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    for (title, reg_dt, firm), keys in groups.items():
                        cur.execute(f"""
                            UPDATE {table_name}
                            SET main_ch_send_yn = 'N', is_sent = false
                            WHERE article_title = %s AND reg_dt = %s AND firm_nm = %s
                              AND is_sent = true
                              AND key != ALL(%s)
                        """, (title, reg_dt, firm, keys))
                        if cur.rowcount > 0:
                            logger.info(f"[DEDUP] reset is_sent for '{title[:40]}' ({cur.rowcount} rows)")
        except Exception as e:
            logger.warning(f"[DEDUP] reset send_yn failed: {e}")
        finally:
            conn.close()

    def insert_json_data_list(self, json_data_list, table_name=None):
        if table_name is None:
            table_name = self.table_name
        table_name = self._TABLE_MAP.get(table_name, table_name)

        # title+date+firm 중복 & key 다른 경우 → 기존 레코드 send_yn=N 초기화 (재발송)
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
            records.append((
                entry.get("sec_firm_order"),
                entry.get("article_board_order"),
                entry.get("firm_nm"),
                entry.get("reg_dt", ""),
                entry.get("article_title"),
                entry.get("article_url"),
                entry.get("main_ch_send_yn", "N"),
                entry.get("download_url"),
                entry.get("telegram_url"),
                entry.get("pdf_url") or entry.get("telegram_url"),
                entry.get("writer", ""),
                entry.get("mkt_tp", "KR"),
                legacy_key,
                unique_key,
                entry.get("save_time"),
                entry.get("is_sent", False),
            ))

        if not records:
            logger.info("[SCRAPER-DB] Data inserted: 0 rows, updated: 0 rows.")
            return 0, 0

        sql = f"""
            INSERT INTO {table_name} (
                sec_firm_order, article_board_order, firm_nm, reg_dt,
                article_title, article_url, main_ch_send_yn, download_url,
                telegram_url, pdf_url, writer, mkt_tp, key,
                report_unique_key, save_time, is_sent
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
                is_sent             = EXCLUDED.is_sent
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
