"""Verify KB parse failure logs index+type, not item data, and continues."""
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_kb_parse_failure_logs_index_and_continues(capsys):
    from scrapers.kb_core import scrape_kb

    good = {"pCategoryid": 1, "documentid": "ok", "publicDate": "20260712",
            "docTitle": "safe", "analystNm": "A"}
    item2 = {"pCategoryid": 2, "documentid": "ok2", "publicDate": "20260712",
             "docTitle": "also", "analystNm": "B"}
    fake_resp = mock.Mock()
    fake_resp.raise_for_status = mock.Mock()
    fake_resp.json.return_value = {"response": {"reportList": [good, "bad_item", item2]}}

    with mock.patch("scrapers.kb_core.requests.post", return_value=fake_resp):
        result = scrape_kb({})

    assert len(result) == 2
    captured = capsys.readouterr()
    assert "[kb] parse failed item[1]" in captured.err
    assert "AttributeError" in captured.err
    assert "bad_item" not in captured.err
