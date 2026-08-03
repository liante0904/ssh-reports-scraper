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
