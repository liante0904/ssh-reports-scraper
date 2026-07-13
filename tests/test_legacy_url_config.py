import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrapers.config_guard import ScraperConfigError
from scrapers.legacy_url_config import FIRM_DEFAULTS, normalize_legacy_url_config


@pytest.mark.parametrize("firm_key", sorted(FIRM_DEFAULTS))
def test_url_list_receives_versioned_parser_defaults(firm_key):
    config = normalize_legacy_url_config(["https://example.test/source"], firm_key=firm_key)

    assert config["urls"] == ["https://example.test/source"]
    for key in FIRM_DEFAULTS[firm_key]:
        assert key in config
    assert config != FIRM_DEFAULTS[firm_key]


def test_explicit_config_overrides_defaults_without_losing_nested_keys():
    config = normalize_legacy_url_config(
        {
            "urls": ["https://example.test/source"],
            "headers": {"X-Test": "true"},
            "item_keys": {"title": "custom_title"},
        },
        firm_key="Toss",
    )

    assert config["headers"]["X-Test"] == "true"
    assert config["headers"]["User-Agent"]
    assert config["item_keys"]["title"] == "custom_title"
    assert config["item_keys"]["files"] == "files"


def test_missing_urls_and_unknown_firm_fail_fast():
    with pytest.raises(ScraperConfigError, match="no source URLs"):
        normalize_legacy_url_config([], firm_key="Samsung")
    with pytest.raises(ScraperConfigError, match="unknown legacy URL-list firm"):
        normalize_legacy_url_config([], firm_key="Unknown")
