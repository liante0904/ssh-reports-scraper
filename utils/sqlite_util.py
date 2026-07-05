EMOJI_PICK = u'\U0001F449'


from utils.telegram_message_builder import (
    build_telegram_message_chunks,
    build_telegram_messages,
    format_telegram_report_row,
    telegram_link_for_report,
)


def _telegram_link_for_row(row):
    return telegram_link_for_report(row)


def _format_report_row(row):
    return format_telegram_report_row(row)


def convert_sql_to_telegram_message_chunks(fetched_rows, message_limit=3500):
    return build_telegram_message_chunks(fetched_rows, message_limit=message_limit)


def convert_sql_to_telegram_messages(fetched_rows):
    """Convert SQL rows into Telegram message strings."""
    return build_telegram_messages(fetched_rows)

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
