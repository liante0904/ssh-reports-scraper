from loguru import logger


EMOJI_PICK = u'\U0001F449'


def _telegram_link_for_row(row):
    if row.get('firm_id') == 11:
        return row.get('telegram_url') if row.get('telegram_url') else "링크없음"
    return row.get('telegram_url') or row.get('download_url') or row.get('article_url') or ""


def _format_report_row(row):
    title = row['article_title'].replace("_", " ").replace("*", "")
    link_url = _telegram_link_for_row(row)
    if link_url == "링크없음":
        link_text = "링크없음\n"
    else:
        link_text = EMOJI_PICK + "[링크]" + "(" + link_url + ")" + "\n"
    return "*" + title + "*" + "\n" + link_text


def convert_sql_to_telegram_message_chunks(fetched_rows, message_limit=3500):
    """Return Telegram message chunks with the exact rows included in each chunk."""
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

        row_text = _format_report_row(row)
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


def convert_sql_to_telegram_messages(fetched_rows):
    """Convert SQL rows into Telegram message strings."""
    return [chunk["message"] for chunk in convert_sql_to_telegram_message_chunks(fetched_rows)]

def format_message_sql(data_list): 
    EMOJI_PICK = u'\U0001F449'  # 이모지 설정
    formatted_messages = []

    last_firm_nm = None  # 마지막으로 출력된 FIRM_NM을 저장하는 변수

    for data in data_list:
        # fetch_keyword_reports 등에서 넘어오는 데이터 순서: report_id, firm_nm, article_title, telegram_url, save_time
        if len(data) == 5:
            _, firm_nm, article_title, telegram_url, save_time = data
        else:
            # 기존 4개 컬럼(firm_nm, article_title, telegram_url, save_time) 대응
            firm_nm, article_title, telegram_url, save_time = data[:4]

        sendMessageText = ""
        
        # 'firm_nm'이 존재하는 경우에만 포함
        if firm_nm:
            if firm_nm != last_firm_nm:
                sendMessageText += "\n\n" + "●" + firm_nm + "\n"
                last_firm_nm = firm_nm
        
        # 게시글 제목(굵게)
        sendMessageText += "*" + article_title.replace("_", " ").replace("*", "") + "*" + "\n"
        # 원문 링크
        sendMessageText += EMOJI_PICK + "[링크]" + "(" + telegram_url + ")" + "\n"

        # SEND_USER 값을 표시하고 싶다면 여기에 추가
        # sendMessageText += "발송 사용자: " + SEND_USER + "\n"

        formatted_messages.append(sendMessageText)
    
    # 모든 메시지를 하나의 문자열로 결합
    return "\n".join(formatted_messages)
