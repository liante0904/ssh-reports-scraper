from datetime import datetime
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models.report_payload import ReportPayload, ReportPayloadError


def test_scraper_aliases_are_mapped_to_physical_db_fields():
    payload = ReportPayload.from_scraper({
        "firm_id": 7,
        "firm_nm": "신영증권",
        "report_date": "20260710",
        "article_title": "Report",
        "source_url": "https://example.test/article",
        "pdf_file_url": "https://example.test/report.pdf",
    })

    record = payload.to_db_record()
    assert payload.report_unique_key == "https://example.test/article"
    assert record[5] == "https://example.test/article"  # telegram_url
    assert record[6] == "https://example.test/report.pdf"  # pdf_url
    assert record[9] == "https://example.test/article"  # report_unique_key


def test_ds_empty_telegram_url_is_preserved_for_internal_share_trigger():
    payload = ReportPayload.from_scraper({
        "firm_id": 11,
        "firm_nm": "DS투자증권",
        "report_date": "20260714",
        "article_title": "Report",
        "source_url": "https://www.ds-sec.co.kr/bbs/board.php?wr_id=1",
        "telegram_url": "",
        "pdf_file_url": "https://www.ds-sec.co.kr/bbs/download.php?wr_id=1",
    })

    assert payload.telegram_url == ""
    assert payload.pdf_file_url.endswith("wr_id=1")


def test_legacy_save_time_is_an_explicit_artifact_adapter():
    payload = ReportPayload.from_scraper({
        "report_unique_key": "legacy",
        "save_time": "2026-06-15T08:00:00+09:00",
    })
    assert payload.save_at == datetime.fromisoformat("2026-06-15T08:00:00+09:00")


@pytest.mark.parametrize(
    "firm_id,board_id,title,declared,expected",
    [
        (3, 16, "AMD(AMD.US): FY 2Q26 Review", "KR", "GLOBAL"),
        (3, 15, "글로벌 산업 전망", "KR", "GLOBAL"),
        (3, 16, "삼성전자(005930.KS): 실적 점검", "GLOBAL", "KR"),
        (10, 3, "NVIDIA(NVDA.US): earnings", "KR", "GLOBAL"),
        (10, 3, "삼성전자(005930.KS): earnings", "US", "KR"),
        (1, 0, "국내 산업 전망", "KR", "KR"),
    ],
)
def test_market_type_is_classified_at_the_shared_payload_boundary(
    firm_id, board_id, title, declared, expected
):
    payload = ReportPayload.from_scraper({
        "firm_id": firm_id,
        "board_id": board_id,
        "firm_nm": "test",
        "report_date": "20260806",
        "article_title": title,
        "report_unique_key": f"key-{firm_id}-{board_id}-{title}",
        "mkt_tp": declared,
    })

    assert payload.mkt_tp == expected


@pytest.mark.parametrize(
    "item,message",
    [
        ({"report_date": "20260710", "firm_nm": "Firm"}, "missing report_unique_key"),
        ({"report_unique_key": "key", "firm_nm": "Firm"}, "invalid report_date"),
        ({"report_unique_key": "key", "report_date": "20260710"}, "missing firm_nm"),
    ],
)
def test_strict_artifact_schema_rejects_missing_fields(item, message):
    with pytest.raises(ReportPayloadError, match=message):
        ReportPayload.from_scraper(
            item, require_schema=True, require_firm_name=True
        )
