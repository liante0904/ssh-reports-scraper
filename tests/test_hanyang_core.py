import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrapers.hanyang_core import scrape_hanyang


class _Response:
    status_code = 200
    text = """
    <table class="board_list">
      <tbody>
        <tr>
          <td>2055</td>
          <td><a href="/board/researchAnalyzeCompany/detail/2055">[07/02] 테스트</a></td>
          <td>2026.07.02</td>
          <td><a href="/file/test.pdf">PDF</a></td>
          <td>780</td>
        </tr>
      </tbody>
    </table>
    """

    def raise_for_status(self):
        return None


def test_scrape_hanyang_extracts_report_date_from_default_columns(monkeypatch):
    monkeypatch.setattr("scrapers.hanyang_core.requests.get", lambda *args, **kwargs: _Response())

    rows = scrape_hanyang({"url": "https://www.hygood.co.kr/board/researchAnalyzeCompany/list"})

    assert len(rows) == 1
    assert rows[0]["report_date"] == "20260702"
    assert rows[0]["report_unique_key"] == "https://www.hygood.co.kr/file/test.pdf"


def test_scrape_hanyang_falls_back_when_configured_date_cell_is_wrong(monkeypatch):
    monkeypatch.setattr("scrapers.hanyang_core.requests.get", lambda *args, **kwargs: _Response())

    rows = scrape_hanyang({
        "url": "https://www.hygood.co.kr/board/researchAnalyzeCompany/list",
        "cell_report_date": 3,
    })

    assert rows[0]["report_date"] == "20260702"
