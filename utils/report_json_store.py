import json
import os
import tempfile
from datetime import datetime
from pathlib import Path


EMOJI_PICK = u'\U0001F449'


def load_report_json_list(path):
    report_path = Path(path)
    if not report_path.exists() or report_path.stat().st_size == 0:
        return []
    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def save_report_json_list(path, reports):
    report_path = Path(path)
    if report_path.parent:
        report_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        dir=str(report_path.parent or Path(".")),
        delete=False,
        encoding="utf-8",
    ) as temp_file:
        json.dump(list(reports), temp_file, ensure_ascii=False, indent=4)
        temp_name = temp_file.name
    os.replace(temp_name, report_path)


def best_report_url(report):
    return (
        report.get("telegram_url")
        or report.get("pdf_url")
        or report.get("download_url")
        or report.get("article_url")
        or ""
    )


def format_report_message(report, *, include_firm_header=False):
    message = ""
    firm_name = report.get("firm_nm")
    if include_firm_header and firm_name:
        message += f"\n\n●{firm_name}\n"

    title = report.get("article_title") or ""
    if title:
        message += f"*{title.replace('_', ' ').replace('*', '')}*\n"

    report_url = best_report_url(report)
    if report_url:
        message += f"{EMOJI_PICK}[링크]({report_url})\n"
    return message


def format_legacy_message(reports):
    """Return the same text as utils.json_util.format_message."""
    if isinstance(reports, dict):
        reports = [reports]

    message_text = ""
    last_firm_name = None
    for report in reports:
        title = report.get("article_title", "")
        report_url = best_report_url(report)

        message_text = ""
        if "firm_nm" in report:
            firm_name = report["firm_nm"]
            if len(reports) > 1 and firm_name != last_firm_name:
                message_text += "\n\n" + "●" + firm_name + "\n"
                last_firm_name = firm_name

    if title:
        message_text += "*" + title.replace("_", " ").replace("*", "") + "*" + "\n"
    if report_url:
        message_text += EMOJI_PICK + "[링크]" + "(" + report_url + ")" + "\n"
    return message_text


def format_report_messages(reports, *, include_firm_headers=True):
    messages = []
    last_firm_name = None
    for report in reports:
        firm_name = report.get("firm_nm")
        include_header = include_firm_headers and firm_name != last_firm_name
        messages.append(format_report_message(report, include_firm_header=include_header))
        if firm_name:
            last_firm_name = firm_name
    return "".join(messages)


def format_legacy_message_chunks(reports, *, message_limit=3500):
    """Mirror json_util.get_unsent_main_ch_data_to_local_json chunk formatting."""
    messages = []
    current_message = ""
    previous_firm_name = None
    first_record = True

    for report in reports:
        firm_name = report.get("firm_nm", "알 수 없음")
        message_part = format_legacy_message(report)

        if first_record:
            current_message += f"●{firm_name}\n"
            first_record = False
            previous_firm_name = firm_name
        elif firm_name != previous_firm_name:
            if previous_firm_name is not None:
                current_message += "\n"
            current_message += f"\n●{firm_name}\n"
            previous_firm_name = firm_name

        if len(current_message) + len(message_part) > message_limit:
            messages.append(current_message)
            current_message = message_part
        else:
            current_message += message_part

    if current_message:
        messages.append(current_message)
    return messages


def build_report_payload(
    *,
    firm_id,
    board_id,
    firm_nm,
    pdf_url,
    article_title,
    article_url=None,
    download_url=None,
    telegram_sent=False,
    save_time=None,
):
    return {
        "firm_id": firm_id,
        "board_id": board_id,
        "firm_nm": firm_nm,
        "article_title": article_title,
        "article_url": article_url or pdf_url,
        "telegram_sent": telegram_sent,
        "download_url": download_url or pdf_url,
        "pdf_url": pdf_url,
        "save_time": save_time or datetime.now().isoformat(),
    }


def append_report_if_new(path, report):
    reports = load_report_json_list(path)
    is_duplicate = any(
        existing.get("firm_nm") == report.get("firm_nm")
        and existing.get("article_title") == report.get("article_title")
        for existing in reports
    )
    if is_duplicate:
        return False
    reports.append(report)
    save_report_json_list(path, reports)
    return True


def select_unsent_reports(reports, *, target_date, excluded_firms=None):
    excluded_firms = set(excluded_firms or [])
    return [
        report for report in reports
        if str(report.get("save_time", "")).startswith(target_date)
        and not report.get("telegram_sent", False)
        and report.get("firm_nm") not in excluded_firms
    ]


def mark_reports_sent_for_date(path, target_date):
    reports = load_report_json_list(path)
    changed = 0
    for report in reports:
        if str(report.get("save_time", "")).startswith(target_date):
            if report.get("telegram_sent") is not True:
                changed += 1
            report["telegram_sent"] = True
    save_report_json_list(path, reports)
    return changed
