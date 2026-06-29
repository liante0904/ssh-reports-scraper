"""Telegram send audit tests — mark_reports_sent safety, URL presence, chunk isolation."""
import os
import sys
import pytest
import asyncio

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)
os.environ["DB_BACKEND"] = "sqlite"


class TestMarkReportsSentSafety:
    """mark_reports_sent가 기본적으로 report_id만 마킹하는지 검증."""

    def test_default_matches_report_id_only(self):
        """match_by_url=False(기본): report_id만 마킹. 같은 URL의 다른 row는 영향 없음."""
        from models.SecReportsManager import SecReportsManager

        manager = object.__new__(SecReportsManager)
        manager.table_name = "tbl_sec_reports"
        calls = []

        def fake_execute(sql, params):
            calls.append((sql, params))

        manager._execute = fake_execute

        rows = [
            {"report_id": 100, "telegram_url": "http://shared/pdf"},
            {"report_id": 200, "telegram_url": "http://shared/pdf"},
        ]
        manager.mark_reports_sent(rows)

        assert len(calls) == 2
        # report_id=100: must use report_id only
        assert "report_id = %s" in calls[0][0]
        assert "telegram_url" not in calls[0][0]
        # report_id=200: same
        assert "report_id = %s" in calls[1][0]
        assert "telegram_url" not in calls[1][0]

    def test_match_by_url_includes_or_condition(self):
        """match_by_url=True: report_id OR telegram_url 매칭."""
        from models.SecReportsManager import SecReportsManager

        manager = object.__new__(SecReportsManager)
        manager.table_name = "tbl_sec_reports"
        calls = []

        def fake_execute(sql, params):
            calls.append((sql, params))

        manager._execute = fake_execute

        rows = [{"report_id": 100, "telegram_url": "http://pdf"}]
        manager.mark_reports_sent(rows, match_by_url=True)

        assert len(calls) == 1
        assert "OR telegram_url" in calls[0][0]


class TestSelectReportsReadyForTelegram:
    """URL 없는 row가 발송 대상에서 제외되는지 검증."""

    def test_url_condition_in_send_query(self):
        """type='send' SQL에 non-DBfi URL 존재 조건이 포함되는지."""
        from models.SecReportsManager import SecReportsManager

        manager = object.__new__(SecReportsManager)
        manager.table_name = "tbl_sec_reports"
        calls = []

        def fake_fetchall(sql, params):
            calls.append((sql, params))
            return []

        manager._fetchall = fake_fetchall

        import asyncio
        asyncio.run(manager.select_reports_ready_for_telegram(date_str="20260629", type="send"))

        sql, params = calls[0]
        assert "COALESCE(telegram_url, '') <> ''" in sql, (
            "send query must exclude rows with no URL"
        )
        assert "firm_nm NOT IN" in sql

    def test_hana_ready_row_shape_is_allowed_by_query(self):
        """Hana-style non-DBfi rows with telegram_url remain eligible when unsent."""
        from models.SecReportsManager import SecReportsManager

        manager = object.__new__(SecReportsManager)
        manager.table_name = "tbl_sec_reports"
        calls = []

        def fake_fetchall(sql, params):
            calls.append((sql, params))
            return []

        manager._fetchall = fake_fetchall

        asyncio.run(manager.select_reports_ready_for_telegram(date_str="20260629", type="send"))

        sql, _ = calls[0]
        assert "(telegram_sent IS NOT true)" in sql
        assert "FROM   public.v_sec_reports_canonical" in sql
        assert "firm_id AS sec_firm_order" in sql
        assert "board_id AS article_board_order" in sql
        assert "report_key" not in sql
        assert "notification_sent" not in sql
        assert "firm_id = 19" in sql
        assert "OR COALESCE(telegram_url, '') <> ''" in sql


class TestTelegramMessageChunks:
    def test_chunks_keep_exact_rows_for_marking(self):
        from utils.sqlite_util import convert_sql_to_telegram_message_chunks

        rows = [
            {
                "report_id": 1,
                "sec_firm_order": 3,
                "firm_nm": "하나증권",
                "article_title": "A",
                "telegram_url": "https://example.test/a.pdf",
            },
            {
                "report_id": 2,
                "sec_firm_order": 3,
                "firm_nm": "하나증권",
                "article_title": "B",
                "telegram_url": "https://example.test/b.pdf",
            },
        ]

        chunks = convert_sql_to_telegram_message_chunks(rows)

        assert len(chunks) == 1
        assert chunks[0]["rows"] == rows
        assert "하나증권" in chunks[0]["message"]
        assert "https://example.test/a.pdf" in chunks[0]["message"]

    def test_chunks_split_rows_with_message_limit(self):
        from utils.sqlite_util import convert_sql_to_telegram_message_chunks

        rows = [
            {
                "report_id": 1,
                "sec_firm_order": 3,
                "firm_nm": "하나증권",
                "article_title": "A" * 60,
                "telegram_url": "https://example.test/a.pdf",
            },
            {
                "report_id": 2,
                "sec_firm_order": 3,
                "firm_nm": "하나증권",
                "article_title": "B" * 60,
                "telegram_url": "https://example.test/b.pdf",
            },
        ]

        chunks = convert_sql_to_telegram_message_chunks(rows, message_limit=90)

        assert len(chunks) == 2
        assert [r["report_id"] for r in chunks[0]["rows"]] == [1]
        assert [r["report_id"] for r in chunks[1]["rows"]] == [2]


def test_daily_send_report_marks_only_successful_chunks(monkeypatch):
    import scraper

    rows = [
        {
            "report_id": 1,
            "sec_firm_order": 3,
            "firm_nm": "하나증권",
            "article_title": "A" * 60,
            "telegram_url": "https://example.test/a.pdf",
        },
        {
            "report_id": 2,
            "sec_firm_order": 3,
            "firm_nm": "하나증권",
            "article_title": "B" * 60,
            "telegram_url": "https://example.test/b.pdf",
        },
    ]

    class FakeDB:
        def __init__(self):
            self.marked = []

        async def select_reports_ready_for_telegram(self, date_str=None, type=None):
            return rows

        async def daily_update_data(self, fetched_rows=None, type=None):
            self.marked.extend(r["report_id"] for r in fetched_rows)
            return {"status": "success"}

    db = FakeDB()
    monkeypatch.setattr(scraper, "get_db", lambda: db)
    monkeypatch.setattr(
        scraper,
        "convert_sql_to_telegram_message_chunks",
        lambda fetched_rows: [
            {"message": "chunk1", "rows": [fetched_rows[0]]},
            {"message": "chunk2", "rows": [fetched_rows[1]]},
        ],
    )

    calls = []

    async def fake_send_markdown(token, chat_id, sendMessageText):
        calls.append(sendMessageText)
        if sendMessageText == "chunk2":
            raise RuntimeError("telegram outage")

    monkeypatch.setattr(scraper, "sendMarkDownText", fake_send_markdown)

    asyncio.run(scraper.daily_send_report(date_str="20260629"))

    assert calls == ["chunk1", "chunk2"]
    assert db.marked == [1]
