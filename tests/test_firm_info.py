"""FirmInfo ga_enabled attribute & firm_utils.ga_enabled() tests (no PostgreSQL needed)."""
import os
import sys
import pytest

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)

# Force DB_BACKEND=sqlite before importing FirmInfo, so it hits static fallback
os.environ["DB_BACKEND"] = "sqlite"


class TestFirmInfoGaEnabled:
    """ga_enabled attribute via static fallback (DB 없는 환경)."""

    def test_ga_enabled_false_in_static_fallback(self):
        """Static fallback은 모든 firm의 ga_enabled를 False로 설정한다."""
        from models.FirmInfo import FirmInfo
        # 초기화 강제 리셋
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}
        FirmInfo.load_data_from_db()

        for order in [0, 4, 13]:
            fi = FirmInfo(order)
            assert fi.ga_enabled is False, f"order={order}: expected ga_enabled=False in fallback"
            assert hasattr(fi, "ga_enabled"), f"order={order}: missing ga_enabled attribute"

    def test_ga_enabled_in_get_state(self):
        """get_state()에 GA_ENABLED 키가 포함된다."""
        from models.FirmInfo import FirmInfo
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}
        FirmInfo.load_data_from_db()

        fi = FirmInfo(4)  # KB
        state = fi.get_state()
        assert "GA_ENABLED" in state
        assert isinstance(state["GA_ENABLED"], bool)

    def test_ga_enabled_persists_after_set_firm_id(self):
        """set_firm_id() 호출 후에도 ga_enabled가 갱신된다."""
        from models.FirmInfo import FirmInfo
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}
        FirmInfo.load_data_from_db()

        fi = FirmInfo(0)
        assert fi.ga_enabled is False
        fi.set_firm_id(4)
        assert fi.ga_enabled is False  # fallback always False


class TestFirmUtilsGaEnabled:
    """firm_utils.ga_enabled() 함수 테스트."""

    def test_ga_enabled_returns_bool(self):
        """ga_enabled()가 bool을 반환한다."""
        from models.firm_utils import ga_enabled
        result = ga_enabled(4)
        assert isinstance(result, bool)

    def test_ga_enabled_importable(self):
        """ga_enabled가 firm_utils에서 import 가능하다."""
        from models.firm_utils import ga_enabled
        assert callable(ga_enabled)


class TestFirmInfoSqliteFallbackGraceful:
    """SQLite 컬럼 없는 환경에서 graceful fallback 검증."""

    def test_static_fallback_sets_ga_enabled_false(self):
        """DB 접근 불가 → static fallback → ga_enabled=False.
        이미 TestFirmInfoGaEnabled에서 검증 완료. 이 클래스는
        향후 SQLite fixture 기반 테스트를 위한 자리 확보용."""
        pass  # covered by TestFirmInfoGaEnabled.test_ga_enabled_false_in_static_fallback


class TestFirmInfoPostgresGaEnabled:
    """PostgreSQL ga_enabled_yn loading — psycopg2 stub, no real DB."""

    def test_postgres_loads_ga_enabled_yn(self, monkeypatch):
        """_load_from_postgres(): ga_enabled_yn='Y'->True, 'N'/None/''->False."""
        import psycopg2

        class FakeCursor:
            def execute(self, query, *args, **kwargs): pass
            def fetchall(self):
                return [
                    {"firm_id": 2, "firm_nm": "NH", "telegram_update_yn": "Y", "ga_enabled_yn": "Y"},
                    {"firm_id": 4, "firm_nm": "KB", "telegram_update_yn": "N", "ga_enabled_yn": "N"},
                    {"firm_id": 5, "firm_nm": "Samsung", "telegram_update_yn": "Y", "ga_enabled_yn": None},
                    {"firm_id": 8, "firm_nm": "Mirae", "telegram_update_yn": "N", "ga_enabled_yn": ""},
                ]
            def __enter__(self): return self
            def __exit__(self, *args): pass

        class FakeConn:
            def cursor(self, **kwargs): return FakeCursor()
            def close(self): pass

        monkeypatch.setattr(psycopg2, "connect", lambda *a, **kw: FakeConn())
        monkeypatch.setenv("DB_BACKEND", "postgres")

        from models.FirmInfo import FirmInfo
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}
        FirmInfo.load_data_from_db()

        assert FirmInfo(2).ga_enabled is True
        assert FirmInfo(4).ga_enabled is False
        assert FirmInfo(5).ga_enabled is False  # None
        assert FirmInfo(8).ga_enabled is False  # ""

    def test_postgres_ga_enabled_via_firm_utils(self, monkeypatch):
        """firm_utils.ga_enabled() reflects PostgreSQL ga_enabled_yn."""
        import psycopg2

        class FakeCursor:
            def execute(self, query, *args, **kwargs): pass
            def fetchall(self):
                return [{"firm_id": 4, "firm_nm": "KB", "telegram_update_yn": "N", "ga_enabled_yn": "Y"}]
            def __enter__(self): return self
            def __exit__(self, *args): pass

        class FakeConn:
            def cursor(self, **kwargs): return FakeCursor()
            def close(self): pass

        monkeypatch.setattr(psycopg2, "connect", lambda *a, **kw: FakeConn())
        monkeypatch.setenv("DB_BACKEND", "postgres")

        from models.FirmInfo import FirmInfo
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}

        from models.firm_utils import ga_enabled
        assert ga_enabled(4) is True


class TestGaEnabledOrders:
    """ga_enabled_orders() metadata source signaling tests."""

    def test_returns_none_for_static_fallback(self):
        """Static fallback → ga_enabled_orders() returns None (metadata unavailable)."""
        from models.FirmInfo import FirmInfo
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}
        # force static fallback by setting metadata_source directly
        FirmInfo._metadata_source = "static"
        FirmInfo._is_loaded = True

        from models.firm_utils import ga_enabled_orders
        assert ga_enabled_orders() is None

    def test_returns_none_for_sqlite(self, monkeypatch):
        """SQLite → ga_enabled_orders() returns None."""
        monkeypatch.setenv("DB_BACKEND", "sqlite")

        from models.FirmInfo import FirmInfo
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}
        FirmInfo._metadata_source = "sqlite"

        from models.firm_utils import ga_enabled_orders
        assert ga_enabled_orders() is None

    def test_returns_enabled_set_for_postgres(self, monkeypatch):
        """PostgreSQL with mixed ga_enabled_yn → returns correct set."""
        import psycopg2

        _firm_rows = [
            {"firm_id": 2, "firm_nm": "NH", "telegram_update_yn": "Y", "ga_enabled_yn": "Y"},
            {"firm_id": 4, "firm_nm": "KB", "telegram_update_yn": "N", "ga_enabled_yn": "N"},
            {"firm_id": 5, "firm_nm": "Samsung", "telegram_update_yn": "Y", "ga_enabled_yn": "Y"},
        ]

        class FakeCursor:
            def __init__(self):
                self._call_count = 0
            def execute(self, query, *args, **kwargs):
                self._call_count += 1
            def fetchall(self):
                # first call = firm info, second = board info (empty)
                if self._call_count == 1:
                    return _firm_rows
                return []
            def __enter__(self): return self
            def __exit__(self, *args): pass

        class FakeConn:
            def cursor(self, **kwargs): return FakeCursor()
            def close(self): pass

        monkeypatch.setattr(psycopg2, "connect", lambda *a, **kw: FakeConn())
        monkeypatch.setenv("DB_BACKEND", "postgres")

        from models.FirmInfo import FirmInfo
        FirmInfo._is_loaded = False
        FirmInfo._firm_data = {}
        FirmInfo.load_data_from_db()

        from models.firm_utils import ga_enabled_orders
        result = ga_enabled_orders()
        assert result == {2, 5}  # NH+Y, Samsung+Y; KB=N excluded
