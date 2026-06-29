"""GA enabled policy tests — _filter_ga_enabled, ga_enabled(), DB-failure fallback (no PostgreSQL)."""
import os
import sys
import pytest

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)
os.environ["DB_BACKEND"] = "sqlite"


class TestFilterGaEnabled:
    """scraper._filter_ga_enabled() 동작 검증."""

    def test_filter_includes_ga_enabled_firms(self, monkeypatch):
        """ga_enabled_orders()가 {2,4,5} 반환 → 해당 firm만 포함."""
        monkeypatch.setattr("models.firm_utils.ga_enabled_orders", lambda: {2, 4, 5})

        from scraper import _filter_ga_enabled
        mapping = {
            2: lambda: "nhqv",
            4: lambda: "kb",
            5: lambda: "samsung",
            8: lambda: "mirae",  # not in enabled set
        }
        result = _filter_ga_enabled(mapping)
        assert 2 in result
        assert 4 in result
        assert 5 in result
        assert 8 not in result

    def test_filter_excludes_all_when_none_enabled(self, monkeypatch):
        """ga_enabled_orders()가 빈 set 반환 → 빈 dict."""
        monkeypatch.setattr("models.firm_utils.ga_enabled_orders", lambda: set())

        from scraper import _filter_ga_enabled
        mapping = {2: lambda: "nhqv", 4: lambda: "kb"}
        result = _filter_ga_enabled(mapping)
        assert result == {}

    def test_filter_fallback_on_exception(self, monkeypatch):
        """ga_enabled_orders() 예외 발생 시 전체 mapping 반환 (운영 장애 방지)."""
        def raise_err():
            raise RuntimeError("DB down")
        monkeypatch.setattr("models.firm_utils.ga_enabled_orders", raise_err)

        from scraper import _filter_ga_enabled
        mapping = {2: lambda: "nhqv", 4: lambda: "kb"}
        result = _filter_ga_enabled(mapping)
        assert result == mapping  # 전체 fallback

    def test_filter_handles_empty_mapping(self, monkeypatch):
        """빈 mapping → 빈 dict."""
        monkeypatch.setattr("models.firm_utils.ga_enabled_orders", lambda: {2, 4})
        from scraper import _filter_ga_enabled
        assert _filter_ga_enabled({}) == {}


class TestGaFirmsMapping:
    """_GA_FIRMS_SYNC, _GA_FIRMS_ASYNC 구조 검증."""

    def test_ga_firms_are_dicts(self):
        """GA_FIRMS가 set이 아닌 {order: func} dict인지 확인."""
        from scraper import _GA_FIRMS_SYNC, _GA_FIRMS_ASYNC
        assert isinstance(_GA_FIRMS_SYNC, dict), "_GA_FIRMS_SYNC must be dict"
        assert isinstance(_GA_FIRMS_ASYNC, dict), "_GA_FIRMS_ASYNC must be dict"

    def test_ga_firms_keys_are_ints(self):
        """모든 key가 int(sec_firm_order)인지 확인."""
        from scraper import _GA_FIRMS_SYNC, _GA_FIRMS_ASYNC
        for order in _GA_FIRMS_SYNC:
            assert isinstance(order, int), f"key {order} is not int"
        for order in _GA_FIRMS_ASYNC:
            assert isinstance(order, int), f"key {order} is not int"

    def test_ga_firms_values_are_callable(self):
        """모든 value가 callable인지 확인."""
        from scraper import _GA_FIRMS_SYNC, _GA_FIRMS_ASYNC
        for func in _GA_FIRMS_SYNC.values():
            assert callable(func)
        for func in _GA_FIRMS_ASYNC.values():
            assert callable(func)

    def test_ga_firms_no_overlap(self):
        """SYNC와 ASYNC에 중복된 sec_firm_order 없음."""
        from scraper import _GA_FIRMS_SYNC, _GA_FIRMS_ASYNC
        overlap = set(_GA_FIRMS_SYNC) & set(_GA_FIRMS_ASYNC)
        assert not overlap, f"overlap: {overlap}"


class TestGaEnabledUtility:
    """firm_utils.ga_enabled() 기능 검증 (static fallback)."""

    def test_ga_enabled_false_in_sqlite_env(self):
        """SQLite/fallback 환경에서 ga_enabled()는 항상 False."""
        from models.FirmInfo import FirmInfo
        from models.firm_utils import ga_enabled
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}
        FirmInfo.load_data_from_db()

        for order in [0, 2, 4, 13, 28]:
            assert ga_enabled(order) is False, f"order={order} must be False in fallback"

    def test_ga_enabled_returns_bool(self):
        """ga_enabled()는 항상 bool 반환."""
        from models.firm_utils import ga_enabled
        assert isinstance(ga_enabled(0), bool)
        assert isinstance(ga_enabled(4), bool)


class TestFilterGaEnabledMetadataSource:
    """_filter_ga_enabled when ga_enabled_orders() signals metadata state."""

    def test_fallback_to_all_when_orders_is_none(self, monkeypatch):
        """ga_enabled_orders() returns None → return full mapping (not empty)."""
        monkeypatch.setattr("models.firm_utils.ga_enabled_orders", lambda: None)

        from scraper import _filter_ga_enabled
        mapping = {2: lambda: "nhqv", 4: lambda: "kb"}
        result = _filter_ga_enabled(mapping)
        assert result == mapping  # ALL candidates returned
        assert 2 in result
        assert 4 in result

    def test_filter_by_set_when_orders_is_set(self, monkeypatch):
        """ga_enabled_orders() returns {2} → only order=2 included."""
        monkeypatch.setattr("models.firm_utils.ga_enabled_orders", lambda: {2})

        from scraper import _filter_ga_enabled
        mapping = {2: lambda: "nhqv", 4: lambda: "kb"}
        result = _filter_ga_enabled(mapping)
        assert result == {2: mapping[2]}
        assert 4 not in result

    def test_empty_set_filters_all_out(self, monkeypatch):
        """ga_enabled_orders() returns empty set → empty dict."""
        monkeypatch.setattr("models.firm_utils.ga_enabled_orders", lambda: set())

        from scraper import _filter_ga_enabled
        mapping = {2: lambda: "nhqv", 4: lambda: "kb"}
        result = _filter_ga_enabled(mapping)
        assert result == {}
