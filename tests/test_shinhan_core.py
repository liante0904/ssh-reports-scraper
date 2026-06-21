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
