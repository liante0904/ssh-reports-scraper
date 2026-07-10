# -*- coding:utf-8 -*-
"""Registry + scraper.py + scraper_health consistency tests.

Verifies:
1. scraper.py function lists match expected behavior (via registry)
2. utils/scraper_health.py works correctly
3. Registry YAML loads correctly (using filesystem import to avoid name shadowing)
"""
import importlib
import inspect
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
            fn_name = self.reg._func_name_from_module(firm)
            fn = self.reg.get_func(module, fn_name)
            if fn is None:
                failed.append(f"{firm['display_name']}: {module}.{fn_name} not importable")
        assert failed == [], "\n".join(failed)

    def test_active_callable_modes_match_manifest(self):
        for firm in self.reg.all_firms():
            for field in ("server_list", "ga_full_scrape_list"):
                mode = firm[field]
                if mode == "none" or (
                    field == "ga_full_scrape_list" and firm["ga_fallback_excluded"]
                ):
                    continue
                fn = self.reg._get_active_func(firm, mode)
                assert inspect.iscoroutinefunction(fn) == (mode == "async")

    def test_active_callable_mode_mismatch_fails(self, monkeypatch):
        async def async_scraper():
            return []

        monkeypatch.setattr(self.reg, "_import_func", lambda *_: async_scraper)
        with pytest.raises(RuntimeError, match="callable mode mismatch"):
            self.reg._get_active_func({
                "firm_id": 99,
                "server_module": "modules.fake",
                "func_name": "fake",
            }, "sync")

    def test_active_missing_callable_fails(self, monkeypatch):
        def missing(*_):
            raise AttributeError("missing")

        monkeypatch.setattr(self.reg, "_import_func", missing)
        with pytest.raises(RuntimeError, match="callable is unavailable"):
            self.reg._get_active_func({
                "firm_id": 99,
                "server_module": "modules.fake",
                "func_name": "fake",
            }, "sync")


class TestYamlStructure:
    """Validate config/firms.yaml structure and new field consistency."""

    @pytest.fixture(autouse=True)
    def _imports(self):
        self.reg = _root_module("scraper_registry")

    def test_all_29_firms_have_func_name(self):
        """Every firm must have func_name in YAML."""
        missing = [f["display_name"] for f in self.reg.all_firms() if not f.get("func_name")]
        assert missing == [], f"Missing func_name for: {missing}"

    def test_func_names_unique(self):
        """func_name should be unique across all non-blocked firms."""
        names = [f["func_name"] for f in self.reg.all_firms() if f.get("mode") != "blocked"]
        assert len(names) == len(set(names)), f"Duplicate func_names: {names}"

    def test_enricher_format_valid(self):
        """Enricher field must be null or valid 'module.path:func_name'."""
        for firm in self.reg.all_firms():
            enricher = firm.get("enricher")
            if enricher is None:
                continue
            assert ":" in enricher, f"{firm['display_name']}: enricher '{enricher}' missing ':'"
            parts = enricher.split(":")
            assert len(parts) == 2, f"{firm['display_name']}: enricher '{enricher}' has {len(parts)} parts"
            mod_path, func = parts
            assert mod_path.startswith("modules."), f"{firm['display_name']}: enricher module '{mod_path}' must start with 'modules.'"
            assert func, f"{firm['display_name']}: enricher func name is empty"

    def test_special_funcs_format_valid(self):
        """special_funcs values must be valid 'module.path:func_name' strings."""
        for firm in self.reg.all_firms():
            special = firm.get("special_funcs")
            if not special:
                continue
            for name, path in special.items():
                assert ":" in path, f"{firm['display_name']}: special_funcs['{name}']='{path}' missing ':'"
                parts = path.split(":")
                assert len(parts) == 2, f"{firm['display_name']}: special_funcs['{name}'] has {len(parts)} parts"
                assert parts[0].startswith("modules."), f"{firm['display_name']}: special_funcs['{name}'] module '{parts[0]}' must start with 'modules.'"

    def test_server_list_values_valid(self):
        """server_list must be sync, async, or none."""
        valid = {"sync", "async", "none"}
        for firm in self.reg.all_firms():
            val = firm.get("server_list")
            assert val in valid, f"{firm['display_name']}: server_list='{val}' not in {valid}"

    def test_ga_full_scrape_list_values_valid(self):
        """ga_full_scrape_list must be sync, async, or none."""
        valid = {"sync", "async", "none"}
        for firm in self.reg.all_firms():
            val = firm.get("ga_full_scrape_list")
            assert val in valid, f"{firm['display_name']}: ga_full_scrape_list='{val}' not in {valid}"

    def test_enricher_consistency(self):
        """get_enricher matches YAML for known firms."""
        for firm in self.reg.all_firms():
            fid = firm["firm_id"]
            enricher_str = firm.get("enricher")
            result = self.reg.get_enricher(fid)
            if enricher_str:
                mod_path, fn_name = enricher_str.split(":", 1)
                assert callable(result), \
                    f"{firm['display_name']}: {mod_path}.{fn_name} is not importable"
                assert result.__name__ == fn_name, \
                    f"{firm['display_name']}: get_enricher returned {result.__name__}, expected {fn_name}"
            else:
                assert result is None, f"{firm['display_name']}: get_enricher should be None"

    def test_enrichment_skip_consistency(self):
        """get_enrichment_skip_firm_ids matches YAML enrichment_skip flags."""
        expected = frozenset(
            f["firm_id"] for f in self.reg.all_firms() if f.get("enrichment_skip")
        )
        actual = self.reg.get_enrichment_skip_firm_ids()
        assert actual == expected, f"Expected {expected}, got {actual}"

    def test_special_funcs_aggregated(self):
        """All firms' special_funcs are aggregated into _SPECIAL_FUNCTIONS."""
        expected = {}
        for firm in self.reg.all_firms():
            special = firm.get("special_funcs")
            if not special:
                continue
            for name, path in special.items():
                mod_path, fn_name = path.split(":", 1)
                expected[name] = (mod_path, fn_name)
        assert self.reg._SPECIAL_FUNCTIONS == expected

    def test_special_funcs_importable(self):
        for name, (module_path, func_name) in self.reg._SPECIAL_FUNCTIONS.items():
            result = self.reg.get_func(module_path, func_name)
            assert callable(result), f"{name}: {module_path}.{func_name} is not importable"

    def test_missing_manifest_fails(self, monkeypatch, tmp_path):
        monkeypatch.setattr(self.reg, "_MANIFEST_PATH", tmp_path / "missing.yaml")
        with pytest.raises(FileNotFoundError):
            self.reg._init_registry()

    def test_malformed_manifest_fails(self, monkeypatch, tmp_path):
        manifest = tmp_path / "firms.yaml"
        manifest.write_text("firms:\n  broken:\n    firm_id: 1\n", encoding="utf-8")
        monkeypatch.setattr(self.reg, "_MANIFEST_PATH", manifest)
        with pytest.raises(ValueError, match="display_name"):
            self.reg._init_registry()

    def test_duplicate_firm_ids_fail(self):
        firm = {
            "display_name": "A", "firm_id": 1, "mode": "server",
            "server_module": "modules.A_1", "server_list": "sync",
            "ga_full_scrape_list": "none", "ga_fallback_excluded": False,
            "func_name": "A_checkNewArticle", "enrichment_skip": False,
        }
        with pytest.raises(ValueError, match="duplicate firm_id"):
            self.reg._validate_manifest({"firms": {"a": firm, "b": dict(firm)}})


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
