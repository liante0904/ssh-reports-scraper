import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils import report_json_store as store


def test_format_report_messages_groups_by_firm():
    reports = [
        {
            "firm_nm": "하나증권",
            "article_title": "A_B*",
            "telegram_url": "https://example.test/a.pdf",
        },
        {
            "firm_nm": "하나증권",
            "article_title": "B",
            "pdf_url": "https://example.test/b.pdf",
        },
        {
            "firm_nm": "KB증권",
            "article_title": "C",
            "download_url": "https://example.test/c.pdf",
        },
    ]

    message = store.format_report_messages(reports)

    assert message.count("●하나증권") == 1
    assert message.count("●KB증권") == 1
    assert "*A B*" in message
    assert "[링크](https://example.test/b.pdf)" in message


def test_append_report_if_new_writes_once(tmp_path):
    target = tmp_path / "reports.json"
    report = store.build_report_payload(
        firm_id=3,
        board_id=0,
        firm_nm="하나증권",
        pdf_url="https://example.test/report.pdf",
        article_title="Report",
        save_time="2026-07-05T09:00:00",
    )

    assert store.append_report_if_new(target, report) is True
    assert store.append_report_if_new(target, dict(report)) is False

    stored = json.loads(target.read_text(encoding="utf-8"))
    assert len(stored) == 1
    assert stored[0]["article_url"] == "https://example.test/report.pdf"
    assert stored[0]["download_url"] == "https://example.test/report.pdf"


def test_select_unsent_reports_filters_date_sent_and_firm():
    reports = [
        {
            "firm_nm": "하나증권",
            "article_title": "today",
            "save_time": "2026-07-05T09:00:00",
            "telegram_sent": False,
        },
        {
            "firm_nm": "제외증권",
            "article_title": "excluded",
            "save_time": "2026-07-05T09:00:00",
            "telegram_sent": False,
        },
        {
            "firm_nm": "하나증권",
            "article_title": "sent",
            "save_time": "2026-07-05T09:00:00",
            "telegram_sent": True,
        },
        {
            "firm_nm": "하나증권",
            "article_title": "yesterday",
            "save_time": "2026-07-04T09:00:00",
            "telegram_sent": False,
        },
    ]

    selected = store.select_unsent_reports(
        reports,
        target_date="2026-07-05",
        excluded_firms={"제외증권"},
    )

    assert [report["article_title"] for report in selected] == ["today"]


def test_mark_reports_sent_for_date_updates_only_target_date(tmp_path):
    target = tmp_path / "reports.json"
    store.save_report_json_list(
        target,
        [
            {"save_time": "2026-07-05T09:00:00", "telegram_sent": False},
            {"save_time": "2026-07-04T09:00:00", "telegram_sent": False},
        ],
    )

    changed = store.mark_reports_sent_for_date(target, "2026-07-05")

    stored = store.load_report_json_list(target)
    assert changed == 1
    assert stored[0]["telegram_sent"] is True
    assert stored[1]["telegram_sent"] is False
