"""call_async_scraper must log full traceback on scraper failure."""
import sys
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.mark.asyncio
async def test_call_async_scraper_logs_exception_preserves_return(monkeypatch):
    from scraper import call_async_scraper

    async def broken():
        raise KeyError("payload")

    fake_log = mock.Mock()
    monkeypatch.setattr("scraper.logger", fake_log)

    name, result, error = await call_async_scraper(broken)

    assert result is None
    assert "payload" in error
    # logger.exception() must be called to preserve full traceback in logs
    assert fake_log.exception.called, "logger.exception() not called — traceback lost"
