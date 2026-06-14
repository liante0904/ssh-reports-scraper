import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LIB_DIR = ROOT.parents[3] / "lib"
sys.path.append(str(ROOT))
sys.path.append(str(LIB_DIR))


class FakeCursor:
    def __init__(self):
        self.sql = None
        self.records = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def fetchall(self):
        return [(self.records[0][13], True)]


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
        "sec_firm_order": 7,
        "article_board_order": 0,
        "firm_nm": "신영증권",
        "article_title": "테스트",
        "key": "https://example.test/report.pdf",
        "save_time": "2026-06-15T08:00:00+09:00",
    }])

    assert (inserted, updated) == (1, 0)
    assert "key,\n                report_unique_key" in connection.cursor_instance.sql
    assert connection.cursor_instance.records[0][12] == "https://example.test/report.pdf"
    assert connection.cursor_instance.records[0][13] == "https://example.test/report.pdf"
    assert connection.closed is True
