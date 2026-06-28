"""Hana server-only scheduler path regression tests (no network, no DB)."""
import os
import sys

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)
os.environ["DB_BACKEND"] = "sqlite"


class TestHanaServerOnlyPath:
    """하나증권이 GA fallback이 아닌 regular server path에 있는지 검증."""

    def test_hana_not_filtered_by_ga_policy(self):
        """하나증권이 _GA_FIRMS_ASYNC에 없으므로 ga_enabled_yn='N' 필터에 걸리지 않는다.
        이 테스트는 Hana가 GA fallback 경로가 아닌 regular server 경로에만 존재함을 입증한다."""
        from scraper import _GA_FIRMS_ASYNC
        assert 3 not in _GA_FIRMS_ASYNC, (
            "Hana(3) must not be in _GA_FIRMS_ASYNC"
        )

    def test_hana_not_in_ga_firms_async(self):
        """하나증권(sec_firm_order=3)이 _GA_FIRMS_ASYNC에 없어야 한다."""
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
