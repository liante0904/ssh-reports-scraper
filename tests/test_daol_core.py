import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrapers.daol_core import scrape_daol


class _Response:
    text = """
    <table>
      <tr>
        <td>2026/07/08</td>
        <td><a title="Daol test report" href="javascript:download('/research','test.pdf','x')">PDF</a></td>
        <td></td><td></td><td>Analyst</td>
      </tr>
    </table>
    """

    def raise_for_status(self):
        return None


def test_scrape_daol_defaults_canonical_firm_fields(monkeypatch):
    monkeypatch.setattr("scrapers.daol_core.requests.post", lambda *args, **kwargs: _Response())

    rows = scrape_daol({
        "url": "https://daol.example.test/list?rGubun=1",
        "path_tpl": "ajax=list",
        "default_form": {},
        "form_keys": ["rGubun"],
        "headers": {},
        "origin": "https://daol.example.test",
        "row_sel": "tr",
        "cell_link": "a",
        "skip_title": "not present",
        "link_split_pattern": ",",
        "pdf_tpl": "https://daol.example.test{path}/{filename}",
    })

    assert len(rows) == 1
    assert rows[0]["firm_id"] == 14
    assert rows[0]["firm_nm"] == "다올투자증권"
    assert rows[0]["report_date"] == "20260708"
