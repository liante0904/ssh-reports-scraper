"""Heungkuk core tests — duplicate PDF guard + report_unique_key policy (no network)."""
import os
import sys
import pytest

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRAPER_DIR)


class TestDuplicatePdfGuard:
    """_filter_duplicate_pdf_rows() 동작 검증."""

    def test_safe_rows_preserved(self):
        """고유 PDF URL을 가진 행은 그대로 유지된다."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows

        rows = [
            {"article_title": "A", "article_url": "http://a.com/1", "telegram_url": "http://pdf/1"},
            {"article_title": "B", "article_url": "http://a.com/2", "telegram_url": "http://pdf/2"},
        ]
        result = _filter_duplicate_pdf_rows(rows)
        assert len(result) == 2

    def test_duplicate_pdf_across_different_articles_dropped(self):
        """서로 다른 article_url이 같은 PDF URL을 공유 → 모두 제거."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows

        rows = [
            {"article_title": "A", "article_url": "http://a.com/1", "telegram_url": "http://pdf/shared"},
            {"article_title": "B", "article_url": "http://a.com/2", "telegram_url": "http://pdf/shared"},
            {"article_title": "C", "article_url": "http://a.com/3", "telegram_url": "http://pdf/unique"},
        ]
        result = _filter_duplicate_pdf_rows(rows)
        assert len(result) == 1
        assert result[0]["article_title"] == "C"

    def test_empty_list(self):
        """빈 리스트 → 빈 리스트."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows
        assert _filter_duplicate_pdf_rows([]) == []

    def test_same_article_same_pdf_kept(self):
        """동일 article_url + 동일 PDF → 중복으로 간주하지 않음."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows

        rows = [
            {"article_title": "A", "article_url": "http://a.com/1", "telegram_url": "http://pdf/1"},
            {"article_title": "A", "article_url": "http://a.com/1", "telegram_url": "http://pdf/1"},
        ]
        result = _filter_duplicate_pdf_rows(rows)
        # Same article_url → NOT considered duplicate PDF abuse (same article identity)
        # Actually the guard checks article_url diversity within pdf group
        # Both have same article_url → article_urls set size = 1 → NOT flagged
        assert len(result) == 2

    def test_all_suspect_returns_empty(self):
        """모든 행이 의심스러운 PDF 공유 → 빈 리스트 반환."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows

        rows = [
            {"article_title": "A", "article_url": "http://a.com/1", "telegram_url": "http://pdf/shared"},
            {"article_title": "B", "article_url": "http://a.com/2", "telegram_url": "http://pdf/shared"},
        ]
        result = _filter_duplicate_pdf_rows(rows)
        assert result == []

    def test_none_pdf_handled(self):
        """telegram_url이 None인 행도 처리된다."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows

        rows = [
            {"article_title": "A", "article_url": "http://a.com/1", "telegram_url": None},
            {"article_title": "B", "article_url": "http://a.com/2", "telegram_url": None},
        ]
        result = _filter_duplicate_pdf_rows(rows)
        assert len(result) == 2  # None은 그룹핑되지 않음


class TestHeungkukUniqueKeyPolicy:
    """report_unique_key == article_url 정책 검증."""

    def test_report_unique_key_is_article_url(self):
        """생성된 행의 report_unique_key가 article_url과 일치해야 한다.
        실제 scraper를 호출하지 않고, _filter_duplicate_pdf_rows에 전달된
        행 구조만 검증한다."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows

        # simulate rows as scrape_heungkuk would produce them
        rows = [
            {
                "article_title": "Report A",
                "article_url": "http://a.com/view.do?key=21204",
                "telegram_url": "http://a.com/download.do?key=30366",
                "download_url": "http://a.com/download.do?key=30366",
                "key": "http://a.com/view.do?key=21204",       # article_url
                "report_unique_key": "http://a.com/view.do?key=21204",  # article_url
            },
        ]
        result = _filter_duplicate_pdf_rows(rows)
        assert len(result) == 1
        assert result[0]["report_unique_key"] == result[0]["article_url"]
        # key도 article_url이어야 함
        assert result[0]["key"] == result[0]["article_url"]
