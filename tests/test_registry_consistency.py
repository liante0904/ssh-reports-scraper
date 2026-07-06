# -*- coding:utf-8 -*-
"""Registry + scraper.py + scraper_health consistency tests.

Verifies:
1. scraper.py function lists match expected behavior (via registry)
2. utils/scraper_health.py works correctly
3. Registry YAML loads correctly (using filesystem import to avoid name shadowing)
"""
import importlib
import sys
from pathlib import Path
import pytest

_PROJECT_ROOT = str(Path(__file__).parent.parent)


def _root_module(name: str):
    """Import a root-level module, ensuring project root is first in sys.path.

    Inserts project root at position 0 to avoid name shadowing from tests/
    (e.g. tests/scraper_registry.py shadowing scraper_registry.py).
    """
    # Ensure project root is first in path
    if sys.path[0] != _PROJECT_ROOT:
        sys.path.insert(0, _PROJECT_ROOT)
    # Remove any cached module that might be the test file
    sys.modules.pop(name, None)
    return importlib.import_module(name)


class TestScraperFunctionLists:
    """Verify scraper.py's function lists match expected behavior."""

    @pytest.fixture(autouse=True)
    def _imports(self):
        self.scraper = _root_module("scraper")

    def test_regular_sync(self):
        funcs = self.scraper._regular_sync_functions()
        names = {f.__name__ for f in funcs}
        assert names == {"Shinyoung_checkNewArticle", "DS_checkNewArticle"}

    def test_regular_async(self):
        funcs = self.scraper._regular_async_functions()
        names = {f.__name__ for f in funcs}
        expected = {
            "ShinHanInvest_checkNewArticle",
            "Koreainvestment_selenium_checkNewArticle",
            "Daeshin_checkNewArticle",
            "HANA_checkNewArticle",
        }
        assert names == expected

    def test_ga_sync_firm_ids(self):
        assert set(self.scraper._GA_FIRMS_SYNC.keys()) == {5, 9, 15, 28}

    def test_ga_async_firm_ids(self):
        assert set(self.scraper._GA_FIRMS_ASYNC.keys()) == {2, 4, 6, 19, 20, 21, 24, 25, 27}

    def test_ga_no_overlap(self):
        assert not (set(self.scraper._GA_FIRMS_SYNC) & set(self.scraper._GA_FIRMS_ASYNC))

    def test_all_ga_funcs_callable(self):
        for d in [self.scraper._GA_FIRMS_SYNC, self.scraper._GA_FIRMS_ASYNC]:
            for fid, fn in d.items():
                assert callable(fn), f"firm_id={fid}: {fn} not callable"


class TestRegistryConsistency:
    """Verify registry module loads correctly (filesystem import to avoid name shadowing)."""

    @pytest.fixture(autouse=True)
    def _imports(self):
        self.reg = _root_module("scraper_registry")

    def test_29_firms_loaded(self):
        assert len(self.reg.all_firms()) == 29

    def test_firm_ids_0_to_28(self):
        ids = sorted(f["firm_id"] for f in self.reg.all_firms())
        assert ids == list(range(29))

    def test_enricher_dispatch(self):
        assert callable(self.reg.get_enricher(0))   # LS
        assert callable(self.reg.get_enricher(19))  # DBFI
        assert self.reg.get_enricher(4) is None     # KB — no enricher

    def test_skip_firm_ids(self):
        assert 11 in self.reg.get_enrichment_skip_firm_ids()

    def test_ls_special_functions(self):
        assert callable(self.reg.get_ls_module_func("LS_checkNewArticle"))
        assert callable(self.reg.get_ls_module_func("LS_detail"))
        assert callable(self.reg.get_ls_module_func("LS_enrich"))

    def test_regular_sync_from_registry(self):
        funcs = self.reg.get_regular_sync_funcs()
        names = {f.__name__ for f in funcs}
        assert names == {"Shinyoung_checkNewArticle", "DS_checkNewArticle"}

    def test_regular_async_from_registry(self):
        funcs = self.reg.get_regular_async_funcs()
        names = {f.__name__ for f in funcs}
        assert names == {
            "ShinHanInvest_checkNewArticle",
            "Koreainvestment_selenium_checkNewArticle",
            "Daeshin_checkNewArticle",
            "HANA_checkNewArticle",
        }

    def test_all_29_functions_importable(self):
        """Every non-blocked firm's function is importable through the registry."""
        failed = []
        for firm in self.reg.all_firms():
            if firm.get("mode") == "blocked":
                continue
            module = firm["server_module"]
            base = module.split(".")[-1]
            name_part = base.rsplit("_", 1)[0] if "_" in base else base
            fn_name = self.reg._KNOWN_FUNC_NAMES.get(name_part)
            if fn_name is None:
                failed.append(f"{firm['display_name']}: no known func name for {name_part}")
                continue
            fn = self.reg.get_func(module, fn_name)
            if fn is None:
                failed.append(f"{firm['display_name']}: {module}.{fn_name} not importable")
        assert failed == [], "\n".join(failed)


class TestScraperHealthUtility:
    """Verify utils/scraper_health.py works correctly."""

    @pytest.fixture(autouse=True)
    def _imports(self):
        self.health = _root_module("utils.scraper_health")

    def test_empty_rows_no_errors(self):
        errors = self.health.log_scraper_health("Test", [], stale_days=5)
        assert errors == []

    def test_non_list_returns_error(self):
        errors = self.health.log_scraper_health("Test", None, stale_days=5)
        assert len(errors) == 1

    def test_no_report_date_error(self):
        rows = [{"article_title": "test"}]
        errors = self.health.log_scraper_health("Test", rows, stale_days=5)
        assert len(errors) >= 1

    def test_valid_rows_no_errors(self):
        import datetime
        today = datetime.datetime.now().strftime("%Y%m%d")
        rows = [{"report_date": today, "article_title": "test"}]
        errors = self.health.log_scraper_health("Test", rows, stale_days=5)
        assert errors == []

    def test_stale_date_returns_errors(self):
        rows = [{"report_date": "20200101", "article_title": "test"}]
        errors = self.health.log_scraper_health("Test", rows, stale_days=5)
        assert len(errors) >= 1
