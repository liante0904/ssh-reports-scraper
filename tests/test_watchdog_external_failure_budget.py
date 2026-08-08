import requests
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules import LS_0


def test_ls_marks_warp_unavailable_after_retry_budget(monkeypatch):
    monkeypatch.setattr(LS_0, "LS_WARP_RETRIES", 2)
    monkeypatch.setattr(LS_0.time, "sleep", lambda _: None)
    monkeypatch.setattr(LS_0, "WARP_UNAVAILABLE", False)

    def unavailable(*args, **kwargs):
        raise requests.ConnectionError("WARP tunnel unavailable")

    monkeypatch.setattr(LS_0.requests, "get", unavailable)

    assert LS_0.get_soup_with_warp("https://example.test/board", {}) is None
    assert LS_0.WARP_UNAVAILABLE is True
