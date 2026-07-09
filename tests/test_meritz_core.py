import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrapers.meritz_core import scrape_meritz


class _ListResponse:
    text = """
    <table>
      <thead>
        <tr><th>작성일</th><th>제목</th><th>작성자</th></tr>
      </thead>
      <tbody>
        <tr>
          <td>2026.07.09</td>
          <td><a href="/research/detail.do?seq=1">Meritz test report</a></td>
          <td>Analyst</td>
        </tr>
      </tbody>
    </table>
    """

    def raise_for_status(self):
        return None


class _DetailResponse:
    text = """
    <html>
      <body>
        <a href="/include/resource/research/WorkFlow/20260709010101010K_02.pdf?download=1">PDF</a>
      </body>
    </html>
    """

    def raise_for_status(self):
        return None


def test_scrape_meritz_sets_pdf_url_from_detail_pdf_link_with_query(monkeypatch):
    calls = []

    def fake_get(url, *args, **kwargs):
        calls.append(url)
        if "detail.do" in url:
            return _DetailResponse()
        return _ListResponse()

    monkeypatch.setattr("scrapers.meritz_core.requests.get", fake_get)

    rows = scrape_meritz({"url": "https://home.imeritz.com/dalyrpt/InfoMain.do?pageNum=1"})

    assert len(rows) == 1
    assert rows[0]["report_date"] == "20260709"
    assert rows[0]["article_url"] == "https://home.imeritz.com/research/detail.do?seq=1"
    assert rows[0]["download_url"].endswith("20260709010101010K_02.pdf?download=1")
    assert rows[0]["telegram_url"] == rows[0]["download_url"]
    assert rows[0]["pdf_url"] == rows[0]["download_url"]
    assert rows[0]["report_unique_key"] == rows[0]["download_url"]
