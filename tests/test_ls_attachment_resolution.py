import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from modules import LS_0
from utils.ls_pdf_verifier import PdfVerificationResult


@pytest.mark.asyncio
async def test_ls_detail_falls_back_only_after_attachment_is_absent(monkeypatch):
    """Writer-history fallback runs only after this row's detail page is read."""

    async def fake_fetch(*_args, **_kwargs):
        return """
        <table><tr><th>제목</th><td>화장품 ODM 참 좋은데…</td></tr>
        <tr><th>필명</th><td>오린아</td></tr>
        <tr><th>첨부파일</th><td></td></tr></table>
        """

    calls = []

    async def fake_reconstruct(*_args, **kwargs):
        calls.append(kwargs)
        return None

    monkeypatch.setattr(LS_0, "fetch", fake_fetch)
    monkeypatch.setattr(LS_0, "reconstruct_msg_url_from_db", fake_reconstruct)

    article = {
        "report_unique_key": "https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no=33&board_seq=1",
        "report_date": "20260713",
        "writer": "오린아",
        "article_title": "화장품 ODM 참 좋은데…",
    }

    await LS_0.process_article(None, article, headers={})

    assert article["telegram_url"] == ""
    assert article["pdf_file_url"] == ""
    assert calls == [{"date_window_days": 0}]


@pytest.mark.asyncio
async def test_ls_png_attachment_is_never_saved_as_pdf(monkeypatch):
    async def fake_fetch(*_args, **_kwargs):
        return """
        <table><tr><th>제목</th><td>PNG 기사 제목</td></tr>
        <tr><th>작성자</th><td>홍길동</td></tr>
        <tr><th>첨부파일</th><td><a>12345_7_20260820.PNG</a></td></tr></table>
        """

    async def fake_reconstruct(*_args, **_kwargs):
        return None

    monkeypatch.setattr(LS_0, "fetch", fake_fetch)
    monkeypatch.setattr(LS_0, "reconstruct_msg_url_from_db", fake_reconstruct)
    monkeypatch.setattr(
        LS_0,
        "verify_ls_pdf_candidate",
        lambda *_args, **_kwargs: PdfVerificationResult(False, "response is not a PDF"),
    )

    article = {
        "report_unique_key": "https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no=36&board_seq=2",
        "report_date": "20260820",
        "article_title": "PNG 기사 제목",
    }

    await LS_0.process_article(None, article, headers={})

    assert article["telegram_url"] == ""
    assert article["pdf_file_url"] == ""
    assert article["article_asset_urls"] == ["12345_7_20260820.PNG"]


@pytest.mark.asyncio
async def test_ls_png_filename_can_resolve_only_to_verified_msg_pdf(monkeypatch):
    async def fake_fetch(*_args, **_kwargs):
        return """
        <table><tr><th>제목</th><td>검증된 기사</td></tr>
        <tr><th>첨부파일</th><td><a>12345_7_20260820.PNG</a></td></tr></table>
        """

    monkeypatch.setattr(LS_0, "fetch", fake_fetch)
    monkeypatch.setattr(
        LS_0,
        "verify_ls_pdf_candidate",
        lambda *_args, **_kwargs: PdfVerificationResult(True, "first-page text matched"),
    )

    article = {
        "report_unique_key": "https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no=36&board_seq=3",
        "report_date": "20260820",
        "article_title": "검증된 기사",
    }

    await LS_0.process_article(None, article, headers={})

    assert article["telegram_url"] == "https://msg.ls-sec.co.kr/eum/K_20260820_12345_7.pdf"
    assert article["pdf_file_url"] == article["telegram_url"]


def test_ls_upload_filename_supports_alphanumeric_writer_key():
    assert LS_0.upload_filename_to_cdn_url("jhsung_2113_20260818.png") == (
        "https://msg.ls-sec.co.kr/eum/K_20260818_jhsung_2113.pdf"
    )


def test_ls_pdf_candidates_expand_prefix_and_date_window():
    candidates = LS_0._ls_pdf_candidate_urls("com_3857_20260820.PNG")

    assert candidates[0].endswith("K_20260820_com_3857.pdf")
    assert "https://msg.ls-sec.co.kr/eum/K_20260821_com_3857.pdf" in candidates
    assert "https://msg.ls-sec.co.kr/eum/K_20260819_com_3857.pdf" in candidates


@pytest.mark.asyncio
async def test_ls_detail_processes_rows_sequentially(monkeypatch):
    detail_calls = []

    async def fake_process(_session, article, _headers, db=None):
        detail_calls.append(article["article_title"])

    monkeypatch.setattr(LS_0, "process_article", fake_process)
    monkeypatch.setattr(LS_0, "LS_DETAIL_DELAY_MIN", 0)
    monkeypatch.setattr(LS_0, "LS_DETAIL_DELAY_MAX", 0)

    first = {
        "article_title": "첫 번째 상세 페이지",
        "report_date": "20260713",
        "writer": "오린아",
    }
    second = {
        "article_title": "두 번째 상세 페이지",
        "report_date": "20260713",
        "writer": "오린아",
    }

    await LS_0.LS_detail([first, second])

    assert detail_calls == ["첫 번째 상세 페이지", "두 번째 상세 페이지"]


@pytest.mark.asyncio
async def test_ls_fallback_reloads_latest_db_sequence_for_same_writer(monkeypatch):
    class FakeDb:
        def __init__(self, latest_sequence):
            self.latest_sequence = latest_sequence

        def _fetchall(self, query, _params):
            if "WHERE firm_id = 0\n              AND writer" in query:
                return [{"telegram_url": f"https://msg.ls-sec.co.kr/eum/K_20260803_31447_{self.latest_sequence}.pdf"}]
            return [{
                "telegram_url": f"https://msg.ls-sec.co.kr/eum/K_20260803_31447_{self.latest_sequence}.pdf",
                "report_date": "2026-08-03",
            }]

    class FakeResponse:
        status_code = 200

    latest_sequences = iter((935, 936))
    monkeypatch.setattr("models.db_factory.get_db", lambda: FakeDb(next(latest_sequences)))
    monkeypatch.setattr(LS_0.requests, "get", lambda *_args, **_kwargs: FakeResponse())

    def fake_verify(url, expected_title, *_args):
        sequence = url.rsplit("_", 1)[-1].removesuffix(".pdf")
        return PdfVerificationResult(sequence == expected_title, "test")

    monkeypatch.setattr(LS_0, "verify_ls_pdf_candidate", fake_verify)
    first = {"writer": "전배승", "report_date": "20260803", "article_title": "936"}
    second = {"writer": "전배승", "report_date": "20260803", "article_title": "937"}

    first_url = await LS_0.reconstruct_msg_url_from_db(
        first, {}, date_window_days=0
    )
    second_url = await LS_0.reconstruct_msg_url_from_db(
        second, {}, date_window_days=0
    )

    assert first_url.endswith("_936.pdf")
    assert second_url.endswith("_937.pdf")
