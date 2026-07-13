import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrapers.article_text import extract_ds_summary


def test_extract_ds_summary_strips_markup_and_entities():
    html = '''<div id="bo_v_con"><p>매출 &amp; 이익이 개선됩니다. 실적 추정치도 상향합니다.</p>
    <strong>목표가 상향</strong></div>'''

    assert extract_ds_summary(html) == "매출 & 이익이 개선됩니다. 실적 추정치도 상향합니다. 목표가 상향"


def test_extract_ds_summary_returns_empty_for_missing_or_short_body():
    assert extract_ds_summary("<html>no body</html>") == ""
    assert extract_ds_summary('<div id="bo_v_con">짧음</div>') == ""


def test_extract_ds_summary_is_bounded():
    body = "가" * 20_000
    assert len(extract_ds_summary(f'<div id="bo_v_con">{body}</div>')) == 10_000
