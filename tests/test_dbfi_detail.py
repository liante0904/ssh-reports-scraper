"""DBfi_detail backward-compat + naming tests (no network, no DB)."""
import os
import sys
import pytest

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)


class FakeEnrichmentDB:
    def __init__(self, fetch_rows=None):
        self.fetch_rows = fetch_rows or []
        self.fetchall_calls = []
        self.update_calls = []

    def _fetchall(self, sql, params=None):
        self.fetchall_calls.append((sql, params))
        return list(self.fetch_rows)

    async def update_telegram_url(
        self,
        report_id,
        telegram_url,
        article_title=None,
        pdf_url=None,
        pdf_file_url=None,
    ):
        self.update_calls.append((report_id, telegram_url, article_title, pdf_url or pdf_file_url))


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

    def test_dbfi_enrich_exists_and_callable(self):
        """DBfi_enrich 함수가 존재하고 callable인지 확인."""
        from modules.DBfi_19 import DBfi_enrich
        assert callable(DBfi_enrich)

    def test_ls_enrich_exists_and_callable(self):
        """LS_enrich 함수가 존재하고 callable인지 확인."""
        from modules.LS_0 import LS_enrich
        assert callable(LS_enrich)

    def test_dbfi_enrich_uses_parameterized_backlog_query(self, monkeypatch):
        """DBfi 유휴 backlog 조회는 URL prefix를 SQL 문자열에 직접 박지 않는다."""
        import asyncio
        from modules import DBfi_19

        async def fake_detail(articles, firm_info=None, db=None):
            return list(articles)

        monkeypatch.setattr(DBfi_19, "DBFI_GATE_PREFIX", "https://dbfi.example.test/pv/gate")
        monkeypatch.setattr(DBfi_19, "DBfi_enrich_and_persist_details", fake_detail)
        db = FakeEnrichmentDB(fetch_rows=[{"report_id": 2, "telegram_url": ""}])

        asyncio.run(DBfi_19.DBfi_enrich(db, [{"report_id": 1, "telegram_url": ""}], None, True))

        assert db.fetchall_calls
        sql, params = db.fetchall_calls[0]
        assert "NOT LIKE %s" in sql
        assert "https://dbfi.example.test" not in sql
        assert params == ("https://dbfi.example.test/pv/gate%",)

    def test_ls_enrich_updates_resolved_urls_and_parameterizes_queries(self, monkeypatch):
        """LS_enrich는 resolved URL을 저장하고 fallback/backlog prefix를 params로 넘긴다."""
        import asyncio
        from modules import LS_0

        async def fake_detail(articles, firm_info=None, db=None):
            return [
                {
                    "report_id": article["report_id"],
                    "article_title": article.get("article_title", "title"),
                    "telegram_url": "https://msg.example.test/doc.pdf",
                    "pdf_file_url": "https://msg.example.test/doc.pdf",
                }
                for article in articles
            ]

        monkeypatch.setattr(LS_0, "LS_PUBLIC_ORIGIN", "https://ls.example.test")
        monkeypatch.setattr(LS_0, "LS_MSG_PREFIX", "https://msg.example.test/")
        monkeypatch.setattr(LS_0, "LS_detail", fake_detail)
        db = FakeEnrichmentDB()

        asyncio.run(LS_0.LS_enrich(db, [{"report_id": 1, "article_title": "A"}], None, True))

        assert db.update_calls
        assert len(db.fetchall_calls) == 2
        assert all("LIKE %s" in sql for sql, _ in db.fetchall_calls)
        assert db.fetchall_calls[0][1] == ("https://ls.example.test/upload/%",)
        assert db.fetchall_calls[1][1] == ("https://msg.example.test/%",)
