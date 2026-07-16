"""Verify Yuanta request failure logs diagnostic and preserves return."""
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_yuanta_request_failure_logs_and_returns_partial(capsys):
    from scrapers.yuanta_core import scrape_yuanta

    with mock.patch("scrapers.yuanta_core.requests.get") as fake_get:
        fake_get.side_effect = ConnectionError("no route")
        result = scrape_yuanta({
            "urls": ["https://test.test/yuanta"], "board_codes": ["001"],
        })

    assert isinstance(result, list)
    captured = capsys.readouterr()
    assert "[yuanta] request failed board=001 page=1" in captured.err
    assert "ConnectionError" in captured.err
    assert "no route" not in captured.err  # private message not exposed
