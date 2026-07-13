import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from modules import LS_0


@pytest.mark.asyncio
async def test_ls_detail_never_guesses_pdf_from_writer_history(monkeypatch):
    """A missing attachment must stay unresolved, not borrow another report's PDF."""

    async def fake_fetch(*_args, **_kwargs):
        return """
        <table><tr><th>제목</th><td>화장품 ODM 참 좋은데…</td></tr>
        <tr><th>필명</th><td>오린아</td></tr>
        <tr><th>첨부파일</th><td></td></tr></table>
        """

    async def must_not_run(*_args, **_kwargs):
        raise AssertionError("writer-history URL inference must not run")

    monkeypatch.setattr(LS_0, "fetch", fake_fetch)
    monkeypatch.setattr(LS_0, "reconstruct_msg_url_from_db", must_not_run)

    article = {
        "report_unique_key": "https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no=33&board_seq=1",
        "report_date": "20260713",
        "writer": "오린아",
        "article_title": "화장품 ODM 참 좋은데…",
    }

    await LS_0.process_article(None, article, headers={})

    assert article["telegram_url"] == ""
    assert article["pdf_file_url"] == ""


@pytest.mark.asyncio
async def test_ls_detail_tries_same_date_inference_before_detail_page(monkeypatch):
    inferred_calls = []
    detail_calls = []

    async def fake_reconstruct(article, _headers, exact_report_date=False):
        inferred_calls.append((article["article_title"], exact_report_date))
        if article["article_title"] == "목록에서 복구됨":
            return "https://msg.ls-sec.co.kr/eum/K_20260713_1_2.pdf"
        return None

    async def fake_process(_session, article, _headers, db=None):
        detail_calls.append(article["article_title"])

    monkeypatch.setattr(LS_0, "reconstruct_msg_url_from_db", fake_reconstruct)
    monkeypatch.setattr(LS_0, "process_article", fake_process)
    monkeypatch.setattr(LS_0, "LS_DETAIL_DELAY_MIN", 0)
    monkeypatch.setattr(LS_0, "LS_DETAIL_DELAY_MAX", 0)

    resolved = {
        "article_title": "목록에서 복구됨",
        "report_date": "20260713",
        "writer": "오린아",
    }
    unresolved = {
        "article_title": "상세 페이지 필요",
        "report_date": "20260713",
        "writer": "오린아",
    }

    await LS_0.LS_detail([resolved, unresolved])

    assert inferred_calls == [
        ("목록에서 복구됨", True),
        ("상세 페이지 필요", True),
    ]
    assert resolved["telegram_url"].endswith("K_20260713_1_2.pdf")
    assert detail_calls == ["상세 페이지 필요"]
