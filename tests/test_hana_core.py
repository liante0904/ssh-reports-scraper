import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrapers.hana_core import _adjust_date


def test_hana_business_day_before_cutover_keeps_source_date():
    assert _adjust_date("20260803", "오후 17:59") == "20260803"


def test_hana_business_day_at_cutover_uses_next_business_day():
    assert _adjust_date("20260803", "오후 6:00") == "20260804"


def test_hana_friday_after_cutover_skips_weekend():
    assert _adjust_date("20260807", "오후 18:00") == "20260810"


def test_hana_weekend_ignores_time_and_uses_next_business_day():
    assert _adjust_date("20260802", "오전 09:00") == "20260803"


def test_hana_holiday_ignores_missing_time_and_uses_next_business_day():
    # 2026-08-17 is the observed Liberation Day holiday in Korea.
    assert _adjust_date("20260817", "") == "20260818"
