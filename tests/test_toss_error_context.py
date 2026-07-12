"""Verify toss_core logs diagnostic on request failure."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_scrape_toss_logs_error_preserves_return(capsys, monkeypatch):
    from scrapers import toss_core

    fail = lambda *a, **kw: (_ for _ in ()).throw(ConnectionError("no route"))
    monkeypatch.setattr(toss_core.requests, "get", fail)

    result = toss_core.scrape_toss({
        "urls": ["https://example.test/toss"], "headers": {}, "item_keys": {},
    })

    assert result == []
    assert "ConnectionError: no route" in capsys.readouterr().err
