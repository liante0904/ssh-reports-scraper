import sys
from pathlib import Path
import asyncio


ROOT = Path(__file__).resolve().parents[1]
LIB_DIR = ROOT.parents[3] / "lib"
sys.path.append(str(ROOT))
if (LIB_DIR / "ssh_library").exists():
    sys.path.append(str(LIB_DIR / "ssh_library"))


class FakeCursor:
    def __init__(self):
        self.sql = None
        self.records = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def fetchall(self):
        return [(self.records[0][11], True)]


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def test_insert_includes_legacy_and_canonical_keys(monkeypatch):
    from models.SecReportsManager import SecReportsManager

    connection = FakeConnection()
    manager = object.__new__(SecReportsManager)
    manager.table_name = "tbl_sec_reports"
    manager.get_connection = lambda: connection

    def fake_execute_values(cursor, sql, records, page_size):
        cursor.sql = sql
        cursor.records = records

    monkeypatch.setattr(
        "models.SecReportsManager.psycopg2.extras.execute_values",
        fake_execute_values,
    )

    inserted, updated = manager.insert_json_data_list([{
        "firm_id": 7,
        "board_id": 0,
        "firm_nm": "신영증권",
        "article_title": "테스트",
        "report_unique_key": "https://example.test/report.pdf",
        "save_at": "2026-06-15T08:00:00+09:00",
    }])

    assert (inserted, updated) == (1, 0)
    assert " key," not in connection.cursor_instance.sql
    assert "report_unique_key" in connection.cursor_instance.sql
    assert connection.cursor_instance.records[0][11] == "https://example.test/report.pdf"
    assert connection.cursor_instance.records[0][13] is not None  # save_at
    assert connection.cursor_instance.records[0][12] is False
    assert "main_ch_send_yn     = CASE" not in connection.cursor_instance.sql
    assert "telegram_sent       = COALESCE" in connection.cursor_instance.sql
    assert "EXCLUDED.telegram_sent" not in connection.cursor_instance.sql
    assert "EXCLUDED.main_ch_send_yn" not in connection.cursor_instance.sql
    assert connection.closed is True


def test_mark_reports_sent_marks_is_sent_and_legacy_main_channel_flag():
    from models.SecReportsManager import SecReportsManager

    calls = []
    manager = object.__new__(SecReportsManager)
    manager.table_name = "tbl_sec_reports"
    manager._execute = lambda sql, params: calls.append((sql, params))

    result = manager.mark_reports_sent([
        {"report_id": 1, "telegram_url": "https://example.test/report.pdf"},
    ])

    assert result == {"status": "success"}
    assert len(calls) == 1
    assert "SET telegram_sent = true" in calls[0][0]
    assert "main_ch_send_yn" not in calls[0][0]
    assert calls[0][1] == (1,)  # default: report_id only, not match_by_url


def test_daily_update_data_delegates_send_status_to_mark_reports_sent(monkeypatch):
    from models.SecReportsManager import SecReportsManager

    manager = object.__new__(SecReportsManager)
    rows = [{"report_id": 1}]
    seen = []
    monkeypatch.setattr(manager, "mark_reports_sent", lambda fetched_rows: seen.append(fetched_rows) or {"status": "success"})

    result = asyncio.run(manager.daily_update_data(fetched_rows=rows, type="send"))

    assert result == {"status": "success"}
    assert seen == [rows]


def test_select_reports_ready_for_telegram_requires_dbfi_streamdocs_pdf(monkeypatch):
    monkeypatch.setenv("DBFI_GATE_URL_PREFIX", "https://dbfi.example.test/pv/gate")
    monkeypatch.setenv("DBFI_STREAMDOCS_URL_PREFIX", "https://dbfi.example.test/streamdocs/v4/documents")
    from models.SecReportsManager import SecReportsManager

    manager = object.__new__(SecReportsManager)
    manager.table_name = "tbl_sec_reports"
    calls = []

    def fake_fetchall(sql, params):
        calls.append((sql, params))
        return []

    manager._fetchall = fake_fetchall

    assert asyncio.run(manager.select_reports_ready_for_telegram(date_str="20260626", type="send")) == []

    sql, params = calls[0]
    assert params[0] == "2026-06-26"
    assert "FROM   public.v_sec_reports_canonical" in sql
    assert "firm_id AS firm_id" in sql
    assert "board_id AS board_id" in sql
    assert "report_unique_key" in sql
    assert "report_key" not in sql
    assert "notification_sent" not in sql
    assert "firm_id = 19" in sql
    assert "telegram_url LIKE 'https://dbfi.example.test/pv/gate%%'" in sql
    assert "pdf_url LIKE 'https://dbfi.example.test/streamdocs/v4/documents%%'" in sql
    assert "firm_nm NOT IN" not in sql
    assert "WHEN firm_id = 19 THEN pdf_url" in sql


def test_keyword_fetch_requires_dbfi_streamdocs_pdf(monkeypatch):
    monkeypatch.setenv("DBFI_GATE_URL_PREFIX", "https://dbfi.example.test/pv/gate")
    monkeypatch.setenv("DBFI_STREAMDOCS_URL_PREFIX", "https://dbfi.example.test/streamdocs/v4/documents")
    from models.SecReportsManager import SecReportsManager

    manager = object.__new__(SecReportsManager)
    manager.table_name = "tbl_sec_reports"
    calls = []

    def fake_fetchall(sql, params):
        calls.append((sql, params))
        return []

    manager._fetchall = fake_fetchall

    assert manager.fetch_keyword_reports("2026-06-26", "방산", "123") == []

    sql, params = calls[0]
    assert params == ("123", "%방산%", "%방산%", "2026-06-26")
    assert "r.firm_id = 19" in sql
    assert "r.telegram_url LIKE 'https://dbfi.example.test/pv/gate%%'" in sql
    assert "r.pdf_url LIKE 'https://dbfi.example.test/streamdocs/v4/documents%%'" in sql
    assert "WHEN r.firm_id = 19 THEN r.pdf_url" in sql


def test_dbfi_ready_condition_blocks_dbfi_when_prefix_missing(monkeypatch):
    monkeypatch.delenv("DBFI_GATE_URL_PREFIX", raising=False)
    monkeypatch.delenv("DBFI_STREAMDOCS_URL_PREFIX", raising=False)
    monkeypatch.delenv("DBFI_VIEWER_BASE_URL", raising=False)
    from models import SecReportsManager as manager_module

    monkeypatch.setattr(manager_module, "_dbfi_viewer_base_from_config", lambda: "")

    assert manager_module.SecReportsManager.dbfi_ready_condition() == "(firm_id != 19)"


def test_duplicate_reset_does_not_mutate_send_status():
    from models.SecReportsManager import SecReportsManager

    manager = object.__new__(SecReportsManager)
    manager.get_connection = lambda: (_ for _ in ()).throw(AssertionError("DB should not be touched"))

    assert manager._reset_duplicate_send_yn([{
        "firm_nm": "신한증권",
        "reg_dt": "20260619",
        "article_title": "중복 제목",
        "key": "https://example.test/new.pdf",
    }], "tbl_sec_reports") is None
