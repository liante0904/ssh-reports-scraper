import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scrapers" / "sks_core.py"
SPEC = importlib.util.spec_from_file_location("sks_core_under_test", MODULE_PATH)
sks_core = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sks_core)

_extract_report_date = sks_core._extract_report_date
_normalize_date = sks_core._normalize_date


def test_normalize_date_accepts_supported_formats():
    assert _normalize_date("2026.06.12") == "20260612"
    assert _normalize_date("2026-06-12 08:30") == "20260612"


def test_normalize_date_rejects_blank_and_invalid_dates():
    assert _normalize_date("") == ""
    assert _normalize_date("2026-99-99") == ""


def test_extract_report_date_uses_configured_key_first():
    item = {"CUSTOM_DATE": "2026/06/13", "RDATE": "20260612"}

    assert _extract_report_date(item, {"date_key": "CUSTOM_DATE"}, "") == "20260613"


def test_extract_report_date_falls_back_to_pdf_filename():
    item = {"RDATE": ""}
    pdf_path = "/Upload/Research/20260612083650333_0_ko.pdf"

    assert _extract_report_date(item, {}, pdf_path) == "20260612"


def test_extract_report_date_does_not_use_unrelated_digits():
    assert _extract_report_date({}, {}, "/Upload/Research/report_123456.pdf") == ""
