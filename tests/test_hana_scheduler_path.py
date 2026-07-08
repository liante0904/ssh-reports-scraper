"""Hana server-only scheduler path regression tests (no network, no DB)."""
import importlib.util
import os
import sys

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)
os.environ["DB_BACKEND"] = "static"


def _load_runtime_scraper_registry():
    """Ensure scraper.py imports the production registry, not tests/scraper_registry.py."""
    registry_path = os.path.join(SCRAPER_DIR, "scraper_registry.py")
    spec = importlib.util.spec_from_file_location("scraper_registry", registry_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    sys.modules["scraper_registry"] = module


_load_runtime_scraper_registry()


class TestHanaServerOnlyPath:
    """하나증권이 GA fallback이 아닌 regular server path에 있는지 검증."""

    def test_hana_in_regular_async_functions(self):
        """regular server path에 HANA_checkNewArticle이 포함되어야 한다."""
        from modules.HANA_3 import HANA_checkNewArticle
        from scraper import _regular_async_functions

        assert HANA_checkNewArticle in _regular_async_functions(), (
            "Hana must be scraped by the regular server scheduler path"
        )

    def test_hana_not_in_ga_firms_async(self):
        """하나증권(firm_id=3)이 _GA_FIRMS_ASYNC에 없어야 한다."""
        from scraper import _GA_FIRMS_ASYNC
        assert 3 not in _GA_FIRMS_ASYNC, (
            "Hana(3) must not be in _GA_FIRMS_ASYNC. "
            "It is server-only due to GA IP block. "
            "Putting it in _GA_FIRMS_ASYNC + ga_enabled_yn='N' "
            "causes it to be filtered out during full-scrape."
        )

    def test_hana_import_still_works(self):
        """HANA_checkNewArticle import가 유효한지 확인."""
        from modules.HANA_3 import HANA_checkNewArticle
        assert callable(HANA_checkNewArticle)
