import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from run.standalone._runner import run_env_scraper
from scrapers.config_guard import ScraperConfigError, normalize_cfg, require_keys


def test_run_env_scraper_outputs_json(monkeypatch, capsys):
    monkeypatch.setenv("TEST_URLS_JSON", json.dumps(["https://example.test/report"]))

    def scrape_func(cfg):
        assert cfg == ["https://example.test/report"]
        return [{"report_unique_key": "a"}]

    run_env_scraper(
        env_key="TEST_URLS_JSON",
        firm_name="테스트증권",
        scrape_func=scrape_func,
    )

    captured = capsys.readouterr()
    assert json.loads(captured.out) == [{"report_unique_key": "a"}]
    assert "[테스트증권] total 1 articles collected" in captured.err


def test_run_env_scraper_accepts_plain_url_list(monkeypatch, capsys):
    monkeypatch.setenv(
        "TEST_URLS_JSON",
        "https://example.test/a, https://example.test/b\nhttps://example.test/c",
    )

    def scrape_func(cfg):
        assert cfg == [
            "https://example.test/a",
            "https://example.test/b",
            "https://example.test/c",
        ]
        return []

    run_env_scraper(
        env_key="TEST_URLS_JSON",
        firm_name="테스트증권",
        scrape_func=scrape_func,
    )

    captured = capsys.readouterr()
    assert json.loads(captured.out) == []
    assert "[테스트증권] total 0 articles collected" in captured.err


def test_run_env_scraper_rejects_list_when_full_config_required(monkeypatch, capsys):
    monkeypatch.setenv("TEST_URLS_JSON", json.dumps(["https://example.test/report"]))

    with pytest.raises(SystemExit) as exc:
        run_env_scraper(
            env_key="TEST_URLS_JSON",
            firm_name="테스트증권",
            scrape_func=lambda cfg: [],
            required_keys=("url", "payload"),
        )

    captured = capsys.readouterr()
    assert exc.value.code == 1
    assert "TEST_URLS_JSON must be full config object, got list" in captured.err


def test_run_env_scraper_reports_missing_config_key(monkeypatch, capsys):
    monkeypatch.setenv("TEST_URLS_JSON", json.dumps({"url": "https://example.test"}))

    with pytest.raises(SystemExit) as exc:
        run_env_scraper(
            env_key="TEST_URLS_JSON",
            firm_name="테스트증권",
            scrape_func=lambda cfg: [],
            required_keys=("url", "payload"),
        )

    captured = capsys.readouterr()
    assert exc.value.code == 1
    assert "TEST_URLS_JSON missing keys: payload" in captured.err


def test_run_env_scraper_reports_unhandled_exception(monkeypatch, capsys):
    monkeypatch.setenv("TEST_URLS_JSON", json.dumps(["https://example.test/report"]))

    def scrape_func(cfg):
        raise RuntimeError("upstream changed")

    with pytest.raises(SystemExit) as exc:
        run_env_scraper(
            env_key="TEST_URLS_JSON",
            firm_name="테스트증권",
            scrape_func=scrape_func,
        )

    captured = capsys.readouterr()
    assert exc.value.code == 1
    assert "[테스트증권] FATAL: scraper failed: upstream changed" in captured.err
    assert "RuntimeError: upstream changed" in captured.err


def test_config_guard_normalizes_urls_and_requires_keys():
    assert normalize_cfg(["https://example.test"], firm_key="TEST") == {
        "urls": ["https://example.test"]
    }
    assert normalize_cfg("https://example.test", firm_key="TEST") == {
        "url": "https://example.test"
    }

    with pytest.raises(ScraperConfigError, match="missing keys: payload"):
        require_keys({"url": "https://example.test"}, ("url", "payload"), firm_key="TEST")
