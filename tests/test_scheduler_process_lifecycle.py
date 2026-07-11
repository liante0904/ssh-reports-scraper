import subprocess
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_scraper_process_uses_isolated_group_and_timeout(monkeypatch):
    import scheduler

    seen = {}

    class FakeProcess:
        pid = 1234
        args = ["scraper.py"]
        returncode = 0

        def communicate(self, timeout=None):
            seen["timeout"] = timeout
            return "out", "err"

    def fake_popen(args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        return FakeProcess()

    monkeypatch.setattr(scheduler.subprocess, "Popen", fake_popen)
    monkeypatch.setenv("SCRAPER_PROCESS_TIMEOUT_SECONDS", "120")

    result = scheduler._run_scraper_process()

    assert result.returncode == 0
    assert seen["kwargs"]["start_new_session"] is True
    assert seen["timeout"] == 120


def test_timeout_terminates_process_group_and_reaps(monkeypatch):
    import scheduler

    calls = []

    class FakeProcess:
        pid = 4321
        args = ["scraper.py"]
        returncode = -15
        attempts = 0

        def communicate(self, timeout=None):
            self.attempts += 1
            if self.attempts == 1:
                raise subprocess.TimeoutExpired(self.args, timeout)
            return "", "killed"

    monkeypatch.setattr(scheduler.subprocess, "Popen", lambda *a, **k: FakeProcess())
    monkeypatch.setattr(scheduler.os, "killpg", lambda pid, sig: calls.append((pid, sig)))
    monkeypatch.setenv("SCRAPER_PROCESS_TIMEOUT_SECONDS", "30")

    result = scheduler._run_scraper_process()

    assert result.returncode == -15
    assert calls == [(4321, scheduler.signal.SIGTERM)]


def test_invalid_timeout_uses_safe_default(monkeypatch):
    import scheduler

    monkeypatch.setenv("SCRAPER_PROCESS_TIMEOUT_SECONDS", "not-a-number")
    assert scheduler._scraper_process_timeout() == 900
