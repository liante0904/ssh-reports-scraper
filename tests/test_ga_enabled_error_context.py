import builtins, sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_filter_ga_enabled_logs_exception_preserves_fallback(monkeypatch):
    from scraper import _filter_ga_enabled

    orig_import = builtins.__import__

    def block_firm_utils(name, *a, **kw):
        if "firm_utils" in name:
            raise ImportError("DB unreachable")
        return orig_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", block_firm_utils)
    fake_log = mock.Mock()
    monkeypatch.setattr("scraper.logger", fake_log)

    mapping = {5: lambda: None, 9: lambda: None}
    result = _filter_ga_enabled(mapping)

    assert result == mapping
    fake_log.exception.assert_called_once_with(
        "ga_enabled lookup failed, falling back to all GA candidates"
    )
