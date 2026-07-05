import importlib
import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))


SCRAPERS = [
    ("modules.LS_0", "LS_checkNewArticle"),
    ("modules.ShinHanInvest_1", "ShinHanInvest_checkNewArticle"),
    ("modules.NHQV_2", "NHQV_checkNewArticle"),
    ("modules.HANA_3", "HANA_checkNewArticle"),
    ("modules.KBsec_4", "KB_checkNewArticle"),
    ("modules.Samsung_5", "Samsung_checkNewArticle"),
    ("modules.Sangsanginib_6", "Sangsanginib_checkNewArticle"),
    ("modules.Shinyoung_7", "Shinyoung_checkNewArticle"),
    ("modules.Miraeasset_8", "Miraeasset_checkNewArticle"),
    ("modules.Hmsec_9", "Hmsec_checkNewArticle"),
    ("modules.Kiwoom_10", "Kiwoom_checkNewArticle"),
    ("modules.DS_11", "DS_checkNewArticle"),
    ("modules.eugenefn_12", "eugene_checkNewArticle"),
    ("modules.Koreainvestment_13", "Koreainvestment_selenium_checkNewArticle"),
    ("modules.DAOL_14", "DAOL_checkNewArticle"),
    ("modules.TOSSinvest_15", "TOSSinvest_checkNewArticle"),
    ("modules.Leading_16", "Leading_checkNewArticle"),
    ("modules.Daeshin_17", "Daeshin_checkNewArticle"),
    ("modules.iMfnsec_18", "iMfnsec_checkNewArticle"),
    ("modules.DBfi_19", "DBfi_checkNewArticle"),
    ("modules.MERITZ_20", "MERITZ_checkNewArticle"),
    ("modules.Hanwhawm_21", "Hanwha_checkNewArticle"),
    ("modules.Hygood_22", "Hanyang_checkNewArticle"),
    ("modules.BNKfn_23", "BNK_checkNewArticle"),
    ("modules.Kyobo_24", "Kyobo_checkNewArticle"),
    ("modules.IBKs_25", "IBK_checkNewArticle"),
    ("modules.SKS_26", "Sks_checkNewArticle"),
    ("modules.Yuanta_27", "Yuanta_checkNewArticle"),
    ("modules.Heungkuk_28", "Heungkuk_checkNewArticle"),
]


def fake_urls_config():
    urls = {
        key: [f"https://example.test/{key}/{idx}" for idx in range(10)]
        for key in [
            "LS_0", "ShinHanInvest_1", "NHQV_2", "HANA_3", "KBsec_4",
            "Samsung_5", "Sangsanginib_6", "Shinyoung_7", "Miraeasset_8",
            "Hmsec_9", "Kiwoom_10", "DS_11", "eugenefn_12",
            "Koreainvestment_13", "DAOL_14", "TOSSinvest_15", "Leading_16",
            "Daeshin_17", "iMfnsec_18", "MERITZ_20", "Hanwhawm_21",
            "Hygood_22", "BNKfn_23", "Kyobo_24", "IBKs_25", "SKS_26",
            "Yuanta_27", "Heungkuk_28",
        ]
    }
    urls["DBfi_19"] = {
        "base_url": "https://example.test/dbfi",
        "viewer_base_url": "https://viewer.example.test",
        "url_paths": {
            "strategy": "/strategy",
            "company": "/company",
            "industry": "/industry",
        },
    }
    return urls


@pytest.fixture(autouse=True)
def configure_fake_urls(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("urls", json.dumps(fake_urls_config()))
    monkeypatch.setenv("DB_BACKEND", "static")

    from models.ConfigManager import ConfigManager

    ConfigManager._instance = None


@pytest.mark.parametrize("module_path, func_name", SCRAPERS)
def test_scraper_module_imports(module_path, func_name):
    module = importlib.import_module(module_path)

    assert callable(getattr(module, func_name))
