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

    def test_duplicate_pdf_reassigns_losers_to_article_fallback(self):
        """서로 다른 article_url이 같은 PDF URL을 공유 → formula delta 승자만 PDF 유지, 나머지는 article_url 폴백."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows

        # download_url (not telegram_url) must contain the PDF key for delta calc
        rows = [
            {"article_title": "A", "article_url": "http://a.com/view.do?key=100", "download_url": "http://a.com/download.do?type=Board&key=200", "telegram_url": "http://a.com/download.do?type=Board&key=200", "pdf_url": "http://a.com/download.do?type=Board&key=200", "reg_dt": "20260630"},
            {"article_title": "B", "article_url": "http://a.com/view.do?key=101", "download_url": "http://a.com/download.do?type=Board&key=200", "telegram_url": "http://a.com/download.do?type=Board&key=200", "pdf_url": "http://a.com/download.do?type=Board&key=200", "reg_dt": "20260629"},
            {"article_title": "C", "article_url": "http://a.com/view.do?key=200", "download_url": "http://a.com/download.do?type=Board&key=999", "telegram_url": "http://a.com/download.do?type=Board&key=999", "pdf_url": "http://a.com/download.do?type=Board&key=999", "reg_dt": "20260628"},
        ]
        result = _filter_duplicate_pdf_rows(rows)
        assert len(result) == 3  # all 3 kept, but B reassigned
        # Article C (unique PDF) keeps it
        assert result[2]["download_url"] != ""
        # Articles A and B shared PDF → winner keeps, loser gets article fallback
        # A: formula=2*100-12059=-11859, delta=abs(200-(-11859))=12059
        # B: formula=2*101-12059=-11857, delta=abs(200-(-11857))=12057 → B wins!
        assert result[1]["download_url"] != ""  # B wins (smaller delta)
        assert result[0]["download_url"] == ""  # A loses → article fallback
        assert result[0]["telegram_url"] == result[0]["article_url"]

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

    def test_all_shared_pdf_one_winner_keeps_pdf(self):
        """모든 행이 같은 PDF 공유 → formula delta로 승자 하나만 PDF 유지, 나머지는 article 폴백."""
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows

        rows = [
            {"article_title": "A", "article_url": "http://a.com/view.do?key=100", "download_url": "http://a.com/download.do?type=Board&key=200", "telegram_url": "http://a.com/download.do?type=Board&key=200", "pdf_url": "http://a.com/download.do?type=Board&key=200", "reg_dt": "20260630"},
            {"article_title": "B", "article_url": "http://a.com/view.do?key=101", "download_url": "http://a.com/download.do?type=Board&key=200", "telegram_url": "http://a.com/download.do?type=Board&key=200", "pdf_url": "http://a.com/download.do?type=Board&key=200", "reg_dt": "20260629"},
        ]
        result = _filter_duplicate_pdf_rows(rows)
        assert len(result) == 2  # both kept
        # B has smaller delta (12057 vs 12059), so B wins
        assert result[1]["download_url"] != ""
        assert result[0]["download_url"] == ""  # A loses → fallback
        assert result[0]["telegram_url"] == result[0]["article_url"]

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


class TestHeungkukPdfResolution:
    """PDF HEAD 탐색이 bounded 동작을 하는지 검증."""

    def _cfg(self, **overrides):
        cfg = {
            "headers": {"User-Agent": "test"},
            "pdf_formula": "2 * {view_key} - 12039",
            "download_tpl": "{base}/download.do?type=Board&key={pdf_key}",
            "pdf_head_timeout": 0.8,
            "pdf_probe_timeout": 0.5,
            "max_pdf_probe_delta": 15,
            "enable_pdf_probe": True,
        }
        cfg.update(overrides)
        return cfg

    def test_formula_hit_returns_download_url(self, monkeypatch):
        from scrapers import heungkuk_core

        calls = []

        def fake_head_pdf_ok(url, headers, analyst_key, timeout):
            calls.append(url)
            return True

        monkeypatch.setattr(heungkuk_core, "_head_pdf_ok", fake_head_pdf_ok)
        url = heungkuk_core._resolve_pdf_download("https://host", 21204, "123", self._cfg())

        assert url == "https://host/download.do?type=Board&key=30369"
        assert calls == ["https://host/download.do?type=Board&key=30369"]

    def test_probe_disabled_formula_miss_falls_back(self, monkeypatch):
        from scrapers import heungkuk_core

        calls = []

        def fake_head_pdf_ok(url, headers, analyst_key, timeout):
            calls.append(url)
            return False

        monkeypatch.setattr(heungkuk_core, "_head_pdf_ok", fake_head_pdf_ok)
        url = heungkuk_core._resolve_pdf_download("https://host", 21204, "123",
                                                   self._cfg(enable_pdf_probe=False))

        assert url is None
        assert calls == ["https://host/download.do?type=Board&key=30369"]

    def test_probe_enabled_is_bounded(self, monkeypatch):
        from scrapers import heungkuk_core

        calls = []

        def fake_head_pdf_ok(url, headers, analyst_key, timeout):
            calls.append(url)
            return False

        monkeypatch.setattr(heungkuk_core, "_head_pdf_ok", fake_head_pdf_ok)
        url = heungkuk_core._resolve_pdf_download(
            "https://host",
            21204,
            "123",
            self._cfg(enable_pdf_probe=True, max_pdf_probe_delta=2),
        )

        assert url is None
        assert calls == [
            "https://host/download.do?type=Board&key=30369",
            "https://host/download.do?type=Board&key=30370",
            "https://host/download.do?type=Board&key=30368",
            "https://host/download.do?type=Board&key=30371",
            "https://host/download.do?type=Board&key=30367",
        ]


class TestHeungkukPdfFallback:
    """PDF 미확인 시 article_url fallback 동작 검증."""

    def test_unresolved_pdf_uses_article_url_for_telegram_only(self):
        """PDF 미확인 row는 발송 링크만 article_url로 대체하고 PDF 필드는 비운다."""
        row = {
            "firm_id": 28, "board_id": 0, "firm_nm": "흥국증권",
            "reg_dt": "20260630", "article_title": "Test",
            "article_url": "http://a.com/view.do?key=1",
            "download_url": "",
            "telegram_url": "http://a.com/view.do?key=1",
            "pdf_url": "",
            "writer": "", "key": "http://a.com/view.do?key=1",
            "report_unique_key": "http://a.com/view.do?key=1",
        }
        from scrapers.heungkuk_core import _filter_duplicate_pdf_rows
        result = _filter_duplicate_pdf_rows([row])
        assert len(result) == 1
        assert result[0]["telegram_url"] == result[0]["article_url"]
        assert result[0]["download_url"] == ""
        assert result[0]["pdf_url"] == ""
