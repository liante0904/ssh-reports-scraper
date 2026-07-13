import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))


def test_canonical_shinhan_url_normalizes_legacy_domain_protocol_and_path():
    from scrapers.shinhan_core import canonical_shinhan_url

    variants = {
        "http://bbs2.shinhansec.com/board/message/file.pdf.do?attachmentId=351365",
        "https://bbs2.shinhansec.com/board/message/file.do?attachmentId=351365",
        "https://bbs2.shinhaninvest.com/board/message/file.do?attachmentId=351365",
        "https://bbs2.shinhaninvest.com/board/message/file.pdf.do?attachmentId=351365",
    }

    assert {
        canonical_shinhan_url(url)
        for url in variants
    } == {
        "https://bbs2.shinhansec.com/board/message/file.pdf.do?attachmentId=351365"
    }


def test_shinhan_mobile_item_keeps_detail_url_and_summary(monkeypatch):
    from scrapers.shinhan_core import scrape_shinhan

    class Response:
        status_code = 200

        def json(self):
            return {
                "header": {"resultCode": "00000", "repeatKeyN": ""},
                "body": {"list01": {"outputList": [{
                    "date": "2026.07.13",
                    "attachment_url": "https://bbs2.shinhansec.com/board/message/file.pdf.do?attachmentId=1",
                    "message_url": "https://m.shinhansec.com/mweb/invt/shrh/detail?id=1",
                    "summary": "실적 추정 상향과 밸류에이션 재평가가 기대됩니다.",
                    "title": "테스트", "nickname": "애널리스트",
                }]}}
            }

    monkeypatch.setattr("scrapers.shinhan_core.requests.post", lambda *args, **kwargs: Response())
    monkeypatch.setattr("scrapers.shinhan_core.requests.get", lambda *args, **kwargs: Response())
    rows = scrape_shinhan({"str_boards": "gicompanyanalyst", "bbs_boards": []})

    assert len(rows) == 1
    assert rows[0]["article_text"] == "실적 추정 상향과 밸류에이션 재평가가 기대됩니다."
    assert rows[0]["source_url"].endswith("id=1")
