"""DBfi_detail backward-compat + naming tests (no network, no DB)."""
import os
import sys
import pytest

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)


class TestDbfiDetailNaming:
    """DBfi_enrich_and_persist_details 명칭 + 호환성 검증."""

    def test_new_name_exists_and_callable(self):
        """신규 함수 DBfi_enrich_and_persist_details가 존재하고 callable."""
        from modules.DBfi_19 import DBfi_enrich_and_persist_details
        import asyncio
        assert callable(DBfi_enrich_and_persist_details)

    def test_old_name_is_alias(self):
        """DBfi_detail이 DBfi_enrich_and_persist_details의 별칭인지 확인."""
        from modules import DBfi_19
        assert DBfi_19.DBfi_detail is DBfi_19.DBfi_enrich_and_persist_details

    def test_import_old_name_still_works(self):
        """기존 import 경로(from modules.DBfi_19 import DBfi_detail) 유효."""
        from modules.DBfi_19 import DBfi_detail
        assert callable(DBfi_detail)

    def test_import_new_name_works(self):
        """새 import 경로 유효."""
        from modules.DBfi_19 import DBfi_enrich_and_persist_details
        assert callable(DBfi_enrich_and_persist_details)

    def test_empty_articles_returns_empty(self):
        """빈 리스트 전달 → 빈 리스트 반환 (부수효과 없음)."""
        from modules.DBfi_19 import DBfi_enrich_and_persist_details
        import asyncio
        result = asyncio.run(DBfi_enrich_and_persist_details([], db=None))
        assert result == []

    def test_old_name_empty_articles_returns_empty(self):
        """DBfi_detail 별칭도 동일하게 동작."""
        from modules.DBfi_19 import DBfi_detail
        import asyncio
        result = asyncio.run(DBfi_detail([], db=None))
        assert result == []
