"""Verify DBfi POST failure logs type, never exception text."""
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_dbfi_post_failure_logs_type_not_private_text(monkeypatch):
    import modules.DBfi_19 as dbfi

    fail = mock.Mock(side_effect=ConnectionError("secret-token-abc123"))
    monkeypatch.setattr(dbfi.requests, "post", fail)
    fake_warn = mock.Mock()
    monkeypatch.setattr(dbfi.logger, "warning", fake_warn)

    import asyncio
    result = asyncio.run(dbfi.DBfi_enrich_and_persist_details(
        [{"report_unique_key": "https://dbfi.test/key", "report_id": 1}],
        db=mock.Mock(),
    ))

    assert isinstance(result, list)
    assert fake_warn.called
    call_msg = fake_warn.call_args[0][0]
    assert "DBfi POST failed" in call_msg
    assert "ConnectionError" in call_msg
    assert "secret-token-abc123" not in call_msg
