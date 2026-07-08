import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrapers.dbfi_core import scrape_dbfi


class _Response:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "rows": [
                {
                    "rid": "abc123",
                    "rdt": "20260708",
                    "title": "DB test report",
                    "writer": "Analyst",
                }
            ]
        }


def test_scrape_dbfi_accepts_legacy_rdt_item_key(monkeypatch):
    monkeypatch.setattr("scrapers.dbfi_core.requests.post", lambda *args, **kwargs: _Response())

    rows = scrape_dbfi({
        "base_url": "https://dbfi.example.test",
        "headers": {},
        "url_paths": [["/list", 0]],
        "list_key": "rows",
        "item_keys": {
            "rid": "rid",
            "rdt": "rdt",
            "title": "title",
            "writer": "writer",
        },
        "key_tpl": "{base}/appData/descRsh/{rid}.json",
    })

    assert len(rows) == 1
    assert rows[0]["firm_id"] == 19
    assert rows[0]["firm_nm"] == "DB증권"
    assert rows[0]["report_date"] == "20260708"
    assert rows[0]["report_unique_key"] == "https://dbfi.example.test/appData/descRsh/abc123.json"
