import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))


def test_hmsec_keeps_contents_from_list_api(monkeypatch):
    from scrapers.hmsec_core import scrape_hmsec

    class Response:
        def json(self):
            return {
                "data_list": [{
                    "UPLOAD_FILE1": "report.pdf", "REG_DATE": "20260713",
                    "SUBJECT": "테스트", "NAME": "애널리스트",
                    "CONTENTS": "현대차증권이 제공한 리포트 요약",
                }],
                "paging": {"totalPages": 1},
            }

    monkeypatch.setattr("scrapers.hmsec_core.requests.get", lambda *args, **kwargs: Response())
    rows = scrape_hmsec(["https://example.test/list"])

    assert len(rows) == 1
    assert rows[0]["article_text"] == "현대차증권이 제공한 리포트 요약"


def test_shinyoung_keeps_summary_from_list_api(monkeypatch):
    from scrapers.shinyoung_core import scrape_shinyoung

    class Response:
        def __init__(self, payload=None, text=""):
            self._payload = payload
            self.text = text

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class Session:
        def __init__(self):
            self.calls = 0

        def post(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return Response({"rows": [{
                    "TITLE": "테스트", "APPDATE": "2026.07.13", "EMPNM": "애널리스트",
                    "SEQ": "1", "BBSNO": "2", "SUMMARY": "신영증권이 제공한 리포트 요약",
                }]})
            if self.calls == 4:
                return Response(text=json.dumps({"FILEINFO": {"FILEPATH": "report.pdf"}}))
            return Response()

    monkeypatch.setattr("scrapers.shinyoung_core.requests.Session", Session)
    rows = scrape_shinyoung({"list_url": "https://example.test/list", "urls": ["https://example.test/list", "https://example.test/files/"]})

    assert len(rows) == 1
    assert rows[0]["article_text"] == "신영증권이 제공한 리포트 요약"
