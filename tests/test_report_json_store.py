import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils import report_json_store as store
from utils import json_util


class FixedDatetime:
    @classmethod
    def now(cls):
        return cls()

    def isoformat(self):
        return "2026-07-05T09:00:00"

    def strftime(self, fmt):
        assert fmt == "%Y-%m-%d"
        return "2026-07-05"


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
            "pdf_file_url": "https://example.test/b.pdf",
        },
        {
            "firm_nm": "KB증권",
            "article_title": "C",
            
        },
    ]

    message = store.format_report_messages(reports)

    assert message.count("●하나증권") == 1
    assert message.count("●KB증권") == 1
    assert "*A B*" in message
    assert "[링크](https://example.test/b.pdf)" in message


def test_format_legacy_message_matches_json_util_for_single_report():
    report = {
        "firm_nm": "하나증권",
        "article_title": "A_B*",
        "telegram_url": "",
        "pdf_file_url": "https://example.test/a.pdf",
    }

    assert store.format_legacy_message(report) == json_util.format_message(report)


def test_json_util_format_message_keeps_legacy_single_report_text():
    report = {
        "firm_nm": "하나증권",
        "article_title": "A_B*",
        "telegram_url": "",
        "pdf_file_url": "https://example.test/a.pdf",
    }

    assert json_util.format_message(report) == (
        "*A B*\n"
        "\U0001F449[링크](https://example.test/a.pdf)\n"
    )


def test_format_legacy_message_matches_json_util_for_report_list():
    reports = [
        {
            "firm_nm": "하나증권",
            "article_title": "A",
            "telegram_url": "https://example.test/a.pdf",
        },
        {
            "firm_nm": "KB증권",
            "article_title": "B",
            "telegram_url": "https://example.test/b.pdf",
        },
    ]

    assert store.format_legacy_message(reports) == json_util.format_message(reports)


def test_json_util_format_message_keeps_legacy_list_last_item_behavior():
    reports = [
        {
            "firm_nm": "하나증권",
            "article_title": "A",
            "telegram_url": "https://example.test/a.pdf",
        },
        {
            "firm_nm": "KB증권",
            "article_title": "B",
            "telegram_url": "https://example.test/b.pdf",
        },
    ]

    assert json_util.format_message(reports) == (
        "\n\n●KB증권\n"
        "*B*\n"
        "\U0001F449[링크](https://example.test/b.pdf)\n"
    )


def test_format_legacy_message_chunks_matches_json_util_chunk_shape():
    reports = [
        {
            "firm_nm": "하나증권",
            "article_title": "A" * 20,
            "telegram_url": "https://example.test/a.pdf",
        },
        {
            "firm_nm": "KB증권",
            "article_title": "B" * 20,
            "telegram_url": "https://example.test/b.pdf",
        },
    ]

    expected_messages = []
    current_message = ""
    previous_firm_name = None
    first_record = True
    for report in reports:
        firm_name = report.get("firm_nm", "알 수 없음")
        message_part = json_util.format_message(report)
        if first_record:
            current_message += f"●{firm_name}\n"
            first_record = False
            previous_firm_name = firm_name
        elif firm_name != previous_firm_name:
            if previous_firm_name is not None:
                current_message += "\n"
            current_message += f"\n●{firm_name}\n"
            previous_firm_name = firm_name
        if len(current_message) + len(message_part) > 55:
            expected_messages.append(current_message)
            current_message = message_part
        else:
            current_message += message_part
    if current_message:
        expected_messages.append(current_message)

    assert store.format_legacy_message_chunks(reports, message_limit=55) == expected_messages


def test_format_legacy_message_chunks_defaults_to_3500_char_limit():
    reports = [
        {
            "firm_nm": "테스트증권",
            "article_title": f"{i:02d}-" + ("A" * 70),
            "telegram_url": f"https://example.test/{i}.pdf",
        }
        for i in range(30)
    ]

    default_chunks = store.format_legacy_message_chunks(reports)

    assert default_chunks == store.format_legacy_message_chunks(reports, message_limit=3500)
    assert len(default_chunks) == 1
    assert len(store.format_legacy_message_chunks(reports, message_limit=3000)) > 1


def test_json_util_unsent_local_json_keeps_legacy_chunk_text(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(json_util, "datetime", FixedDatetime)
    target = tmp_path / "reports.json"
    target.write_text(
        json.dumps(
            [
                {
                    "firm_nm": "Alpha증권",
                    "article_title": "A",
                    "telegram_url": "https://example.test/a.pdf",
                    "save_at": "2026-07-05T08:00:00",
                    "telegram_sent": False,
                },
                {
                    "firm_nm": "Beta증권",
                    "article_title": "B",
                    "telegram_url": "https://example.test/b.pdf",
                    "save_at": "2026-07-05T08:10:00",
                    "telegram_sent": False,
                },
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    assert json_util.get_unsent_main_ch_data_to_local_json(str(target)) == [
        "●Alpha증권\n"
        "*A*\n"
        "\U0001F449[링크](https://example.test/a.pdf)\n"
        "\n\n●Beta증권\n"
        "*B*\n"
        "\U0001F449[링크](https://example.test/b.pdf)\n"
    ]


def test_append_report_if_new_writes_once(tmp_path):
    target = tmp_path / "reports.json"
    report = store.build_report_payload(
        firm_id=3,
        board_id=0,
        firm_nm="하나증권",
        pdf_url="https://example.test/report.pdf",
        article_title="Report",
        save_at="2026-07-05T09:00:00",
    )

    assert store.append_report_if_new(target, report) is True
    assert store.append_report_if_new(target, dict(report)) is False

    stored = json.loads(target.read_text(encoding="utf-8"))
    assert len(stored) == 1
    assert stored[0]["source_url"] == "https://example.test/report.pdf"
    assert stored[0]["pdf_file_url"] == "https://example.test/report.pdf"


def test_json_util_save_data_to_local_json_keeps_legacy_return_and_payload(tmp_path, monkeypatch):
    monkeypatch.setattr(json_util, "datetime", FixedDatetime)
    target = tmp_path / "reports.json"

    message = json_util.save_data_to_local_json(
        str(target),
        firm_id=3,
        board_id=0,
        firm_nm="하나증권",
        pdf_url="https://example.test/report.pdf",
        article_title="Report_Title*",
    )

    assert message == (
        "*Report Title*\n"
        "\U0001F449[링크](https://example.test/report.pdf)\n"
    )
    stored = json.loads(target.read_text(encoding="utf-8"))
    assert stored == [
        {
            "firm_id": 3,
            "board_id": 0,
            "firm_nm": "하나증권",
            "article_title": "Report_Title*",
            "source_url": "https://example.test/report.pdf",
            "telegram_sent": False,
            
            "pdf_file_url": "https://example.test/report.pdf",
            "save_at": "2026-07-05T09:00:00",
        }
    ]
    assert json_util.save_data_to_local_json(
        str(target),
        firm_id=3,
        board_id=0,
        firm_nm="하나증권",
        pdf_url="https://example.test/report.pdf",
        article_title="Report_Title*",
    ) == ""


def test_select_unsent_reports_filters_date_sent_and_firm():
    reports = [
        {
            "firm_nm": "하나증권",
            "article_title": "today",
            "save_at": "2026-07-05T09:00:00",
            "telegram_sent": False,
        },
        {
            "firm_nm": "제외증권",
            "article_title": "excluded",
            "save_at": "2026-07-05T09:00:00",
            "telegram_sent": False,
        },
        {
            "firm_nm": "하나증권",
            "article_title": "sent",
            "save_at": "2026-07-05T09:00:00",
            "telegram_sent": True,
        },
        {
            "firm_nm": "하나증권",
            "article_title": "yesterday",
            "save_at": "2026-07-04T09:00:00",
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
            {"save_at": "2026-07-05T09:00:00", "telegram_sent": False},
            {"save_at": "2026-07-04T09:00:00", "telegram_sent": False},
        ],
    )

    changed = store.mark_reports_sent_for_date(target, "2026-07-05")

    stored = store.load_report_json_list(target)
    assert changed == 1
    assert stored[0]["telegram_sent"] is True
    assert stored[1]["telegram_sent"] is False
