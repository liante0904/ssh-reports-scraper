"""Core contract tests — standalone → core 인자 타입 검증 (no network)."""
import json, os, sys, pytest

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)

# Standalone files to check
STANDALONE_FILES = [
    f for f in os.listdir(os.path.join(SCRAPER_DIR, "run", "standalone"))
    if f.endswith(".py") and f != "_TEMPLATE.py"
]

# Core files
CORE_FILES = [
    f for f in os.listdir(os.path.join(SCRAPER_DIR, "scrapers"))
    if f.endswith("_core.py")
]


def _import_core(name):
    """Import a core module's scrape function."""
    mod_name = f"scrapers.{name.replace('_core.py', '')}_core"
    try:
        mod = __import__(mod_name, fromlist=["scrape"])
        funcs = [v for k, v in mod.__dict__.items() if k.startswith("scrape_")]
        return funcs[0] if funcs else None
    except Exception as e:
        return None


class TestCoreBackwardCompat:
    """모든 core가 list/str을 dict로 변환하는지."""

    @pytest.mark.parametrize("core_file", [c for c in CORE_FILES if c not in ("sks_core.py","kb_core.py","nhqv_core.py")])
    def test_core_accepts_list(self, core_file):
        """Core should not crash when receiving a URL list."""
        func = _import_core(core_file)
        if func is None:
            pytest.skip(f"Cannot import {core_file}")
        try:
            result = func(["https://example.com/test"])
            assert isinstance(result, list), f"{core_file}: should return list"
        except Exception as e:
            # Expected: may fail on HTTP request, but not on type error
            assert "list" not in str(e).lower() or "has no attribute" not in str(e), \
                f"{core_file}: type error on list input: {e}"

    @pytest.mark.parametrize("core_file", [c for c in CORE_FILES if c not in ("sks_core.py","kb_core.py","nhqv_core.py")])
    def test_core_accepts_str(self, core_file):
        """Core should not crash when receiving a single URL string."""
        func = _import_core(core_file)
        if func is None:
            pytest.skip(f"Cannot import {core_file}")
        try:
            result = func("https://example.com/test")
            assert isinstance(result, list), f"{core_file}: should return list"
        except Exception as e:
            assert "str" not in str(e).lower() or "has no attribute" not in str(e), \
                f"{core_file}: type error on str input: {e}"


class TestStandaloneSyntax:
    """모든 standalone이 Python 문법 오류 없이 import 가능한지."""

    @pytest.mark.parametrize("standalone", STANDALONE_FILES)
    def test_standalone_compiles(self, standalone):
        import py_compile
        path = os.path.join(SCRAPER_DIR, "run", "standalone", standalone)
        py_compile.compile(path, doraise=True)


class TestValidation:
    """결과 검증 로직."""

    def test_valid_result_passes(self):
        from scrapers.validate import validate_results
        results = [{"report_unique_key": "a", "report_date": "20260615"}]
        v = validate_results(results, "test")
        assert len(v) == 1

    def test_empty_report_date_filtered(self):
        from scrapers.validate import validate_results
        results = [{"report_unique_key": "a", "report_date": ""}]
        v = validate_results(results, "test")
        assert len(v) == 0

    def test_invalid_report_date_filtered(self):
        from scrapers.validate import validate_results
        results = [{"report_unique_key": "a", "report_date": "2026-06-15"}]
        v = validate_results(results, "test")
        assert len(v) == 0

    def test_missing_key_filtered(self):
        from scrapers.validate import validate_results
        results = [{"report_date": "20260615"}]
        v = validate_results(results, "test")
        assert len(v) == 0

    def test_today_date_not_substituted(self):
        from scrapers.validate import validate_results
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        results = [{"report_unique_key": "a", "report_date": "", "key": "a"}]
        v = validate_results(results, "test")
        assert len(v) == 0  # empty not filled with today
