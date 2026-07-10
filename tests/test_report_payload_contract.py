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


def test_legacy_save_time_is_an_explicit_artifact_adapter():
    payload = ReportPayload.from_scraper({
        "report_unique_key": "legacy",
        "save_time": "2026-06-15T08:00:00+09:00",
    })
    assert payload.save_at == datetime.fromisoformat("2026-06-15T08:00:00+09:00")


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
