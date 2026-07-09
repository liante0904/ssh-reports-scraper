EMOJI_PICK = u'\U0001F449'


def telegram_link_for_report(row):
    _pdf = row.get('pdf_file_url') or row.get('pdf_url')
    _src = row.get('source_url') or row.get('article_url')
    if row.get('firm_id') == 19:
        return _pdf or row.get('telegram_url') or ""
    if row.get('firm_id') == 11:
        return row.get('telegram_url') if row.get('telegram_url') else "링크없음"
    return row.get('telegram_url') or _src or ""


def format_telegram_report_row(row):
    title = row['article_title'].replace("_", " ").replace("*", "")
    link_url = telegram_link_for_report(row)
    if not link_url or link_url == "링크없음":
        link_text = "링크없음\n"
    else:
        safe_url = link_url.replace("(", "%28").replace(")", "%29")
        link_text = EMOJI_PICK + "[링크]" + "(" + safe_url + ")" + "\n"
    return "*" + title + "*" + "\n" + link_text


def build_telegram_message_chunks(fetched_rows, message_limit=3500):
    """Return Telegram message chunks with the exact rows included for sent marking."""
    if not fetched_rows:
        raise ValueError("Invalid fetched_rows.")

    chunks = []
    message_chunk = ""
    chunk_rows = []
    last_firm_nm = None

    def flush_chunk():
        nonlocal message_chunk, chunk_rows
        if message_chunk.strip() and chunk_rows:
            chunks.append({
                "message": message_chunk.strip(),
                "rows": list(chunk_rows),
            })
        message_chunk = ""
        chunk_rows = []

    for row in fetched_rows:
        firm_nm = row.get('firm_nm')

        firm_header = ""
        if firm_nm and firm_nm != last_firm_nm:
            firm_header = "\n\n" + "●" + firm_nm + "\n"

        row_text = format_telegram_report_row(row)
        addition = firm_header + row_text

        if message_chunk and len(message_chunk) + len(addition) > message_limit:
            flush_chunk()
            firm_header = "\n\n" + "●" + firm_nm + "\n" if firm_nm else ""
            addition = firm_header + row_text

        message_chunk += addition
        chunk_rows.append(row)
        if firm_nm:
            last_firm_nm = firm_nm

    flush_chunk()
    return chunks


def build_telegram_messages(fetched_rows):
    return [chunk["message"] for chunk in build_telegram_message_chunks(fetched_rows)]
