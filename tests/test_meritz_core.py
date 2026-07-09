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
    assert rows[0]["source_url"] == "https://home.imeritz.com/research/detail.do?seq=1"
    assert rows[0]["pdf_file_url"].endswith("20260709010101010K_02.pdf?download=1")
    assert rows[0]["telegram_url"] == rows[0]["pdf_file_url"]
    assert rows[0]["report_unique_key"] == rows[0]["pdf_file_url"]


def test_resolve_meritz_pdf_url_from_iframe_srcdoc():
    from scrapers.meritz_core import _resolve_meritz_pdf_url

    html = """
    <iframe srcdoc="&lt;HTML&gt;&lt;BODY&gt;&lt;A href='http://home.imeritz.com/include/resource/research/WorkFlow/20260625074704120K_02.pdf' target=_blank&gt;report&lt;/A&gt;&lt;/BODY&gt;&lt;/HTML&gt;"></iframe>
    <a href="#" onclick="getDownLoadFile('/bbs/BbsDownLoad.go', 'bascGrp', 'pricenewsrs', '14386', '0');">20260625074704120K_02.pdf</a>
    """

    assert _resolve_meritz_pdf_url(
        html,
        "https://home.imeritz.com/bbs/BbsRead.go?bbsCnttTurnNo=14386",
        {},
    ) == "http://home.imeritz.com/include/resource/research/WorkFlow/20260625074704120K_02.pdf"


def test_resolve_meritz_pdf_url_from_download_title():
    from scrapers.meritz_core import _resolve_meritz_pdf_url

    html = """
    <a href="#" onclick="getDownLoadFile('/bbs/BbsDownLoad.go', 'bascGrp', 'pricenewsrs', '14233', '0');"
       title="20251001104659818K_02.pdf 파일 다운로드">20251001104659818K_02.pdf</a>
    """

    assert _resolve_meritz_pdf_url(
        html,
        "https://home.imeritz.com/bbs/BbsRead.go?bbsCnttTurnNo=14233",
        {},
    ) == "https://home.imeritz.com/include/resource/research/WorkFlow/20251001104659818K_02.pdf"
