import os
import json
from datetime import datetime, timedelta
import argparse
import tempfile

def safe_json_dump(data, filename):
    """임시 파일을 사용하여 JSON을 안전하게 저장합니다 (Atomic Write)."""
    directory = os.path.dirname(filename)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        
    # 임시 파일에 쓰기 (원래 파일과 같은 디렉토리에 생성하여 os.replace 보장)
    with tempfile.NamedTemporaryFile('w', dir=directory, delete=False, encoding='utf-8') as tf:
        json.dump(data, tf, ensure_ascii=False, indent=4)
        tempname = tf.name
    
    # 원래 파일로 원자적으로 교체
    os.replace(tempname, filename)

# 전역 변수로 필터링할 증권사 목록 정의
EXCLUDED_FORWARD_REPORT_FIRMS = {"교보증권","IBK투자증권","SK증권","하나증권", "신한투자증권", "이베스트증권","이베스트투자증권", "미래에셋증권", "iM증권", "대신증권", "상상인증권", "LS증권","키움증권", "유진투자증권", "메리츠증권", "한화투자증권", "유안타증권"}

def format_message(data_list):
    EMOJI_PICK = u'\U0001F449'  # 이모지 설정
    formatted_messages = []

    # data_list가 단일 데이터 항목일 경우, 리스트로 감싸줍니다.
    if isinstance(data_list, dict):
        data_list = [data_list]

    last_firm_nm = None  # 마지막으로 출력된 FIRM_NM을 저장하는 변수

    for data in data_list:
        article_title = data.get('article_title','')
        article_url = data.get('telegram_url') or data.get('pdf_url') or data.get('download_url') or data.get('article_url','')
        
        sendMessageText = ""
        
        # 'firm_nm'이 존재하는 경우에만 포함
        if 'firm_nm' in data:
            firm_nm = data['firm_nm']
            # data_list가 단건인 경우, 회사명 출력을 생략
            if len(data_list) > 1:
                # 제외할 FIRM_NM이 아닌 경우에만 처리
                if '네이버' not in firm_nm or '조선비즈' not in firm_nm:
                    # 새로운 FIRM_NM이거나 첫 번째 데이터일 때만 FIRM_NM을 포함
                    if firm_nm != last_firm_nm:
                        sendMessageText += "\n\n" + "●" + firm_nm + "\n"
                        last_firm_nm = firm_nm
        

    # 게시글 제목이 유효한 값인지 확인
    if article_title:
        sendMessageText += "*" + article_title.replace("_", " ").replace("*", "") + "*" + "\n"
    else:
        sendMessageText += ""  # 제목이 없을 경우의 처리

    # 원문 링크가 유효한 값인지 확인
    if article_url:
        sendMessageText += EMOJI_PICK + "[링크]" + "(" + article_url + ")" + "\n"
    else:
        sendMessageText += ""  # 링크가 없을 경우의 처리

    formatted_messages.append(sendMessageText)
    # 모든 메시지를 하나의 문자열로 결합합니다.
    return "\n".join(formatted_messages)


def save_data_to_local_json(filename, firm_id, board_id, firm_nm, pdf_url, article_title, article_url=None, download_url=None, telegram_sent=False):
    directory = os.path.dirname(filename)

    # 디렉터리가 존재하는지 확인하고, 없으면 생성합니다.
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"\n디렉터리 '{directory}'를 생성했습니다.")

    # 현재 시간을 저장합니다.
    current_time = datetime.now().isoformat()
    
    # `article_url`가 None이면 pdf_url로 대체합니다. (임시 추후변경)
    if article_url is None:
        article_url = pdf_url
    if download_url is None:
        download_url = pdf_url
        
    # 새 데이터를 딕셔너리로 저장합니다.
    new_data = {
        "firm_id": firm_id,
        "board_id": board_id,
        "firm_nm": firm_nm,
        "article_title": article_title,
        "article_url": article_url,
        "telegram_sent": telegram_sent,
        "download_url": download_url,
        "pdf_url": pdf_url,
        "save_time": current_time
    }


    # 기존 데이터를 읽어옵니다.
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        try:
            with open(filename, 'r', encoding='utf-8') as json_file:
                existing_data = json.load(json_file)
            if not isinstance(existing_data, list):
                print(f"Warning: {filename} format is invalid. Resetting to list.")
                existing_data = []
        except json.JSONDecodeError:
            print(f"Warning: {filename} is corrupted. Starting with empty list.")
            existing_data = []
    else:
        existing_data = []

    # 중복 체크 (firm_nm, article_title 중복 확인)
    is_duplicate = any(
        existing_item.get("firm_nm") == new_data["firm_nm"] and
        existing_item.get("article_title") == new_data["article_title"]
        for existing_item in existing_data
    )

    if not is_duplicate:
        existing_data.append(new_data)
        
        # 안전한 쓰기 방식 적용
        safe_json_dump(existing_data, filename)
        
        print(f"\n새 데이터가 {filename}에 성공적으로 저장되었습니다.")
        
        # 중복되지 않은 항목을 템플릿 형식으로 반환
        if '네이버'in firm_nm or '조선비즈' in firm_nm:
            return format_message(new_data) + '\n'
        else:
            return format_message(new_data)
    else:
        print("중복된 데이터가 발견되어 저장하지 않았습니다.")
        return ''

def get_unsent_main_ch_data_to_local_json(filename):

    directory = os.path.dirname(filename)
    
    # 디렉터리가 존재하는지 확인하고, 없으면 생성합니다.
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        print(f"\n디렉터리 '{directory}'를 생성했습니다.")
    
    # 현재 날짜를 가져옵니다.
    today_str = datetime.now().strftime("%Y-%m-%d")

    # json 파일을 읽어옵니다.
    data = []
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        try:
            with open(filename, 'r', encoding='utf-8') as json_file:
                data = json.load(json_file)
            if not isinstance(data, list):
                data = []
        except json.JSONDecodeError:
            print(f"Error: {filename} is corrupted. Returning empty data.")
            return []
    else:
        print(f"\n파일 경로 '{filename}'가 존재하지 않거나 비어 있습니다.")
        return []

    # 중복 확인을 위해 json/data_main_daily_send.json의 firm_nm 목록을 가져옵니다.
    main_daily_send_path = 'json/data_main_daily_send.json'
    sent_firms = set()
    if os.path.exists(main_daily_send_path) and os.path.getsize(main_daily_send_path) > 0:
        try:
            with open(main_daily_send_path, 'r', encoding='utf-8') as json_file:
                main_daily_data = json.load(json_file)
                if isinstance(main_daily_data, list):
                    sent_firms = {item.get("firm_nm") for item in main_daily_data if item.get("firm_nm")}
                    print(f"\n중복 확인을 위해 로드된 firm_nm 목록: {sent_firms}")
        except json.JSONDecodeError:
            print(f"Warning: {main_daily_send_path} is corrupted.")

    # EXCLUDED_FORWARD_REPORT_FIRMS를 sent_firms에 합치기
    sent_firms.update(EXCLUDED_FORWARD_REPORT_FIRMS)
    print(f"\n수기 EXCLUDED_FORWARD_REPORT_FIRMS 추가 목록(제외할 증권사 포함): {sent_firms}")

    additional_firms = set()

    # 추가된 목록을 sent_firms에 합치기
    sent_firms.update(additional_firms)
    print(f"\n최종 firm_nm 목록: {sent_firms}")

    # 조건에 맞는 데이터를 필터링합니다.
    unsent_data = [
        item for item in data
        if item.get("save_time", "").startswith(today_str) and 
           not item.get("telegram_sent", False) and 
           item.get("firm_nm") not in sent_firms
    ]

    # 디버깅 로그 추가
    print(f"\n필터링된 unsent_data: {unsent_data}")

    messages = []
    current_message = ""
    previous_firm_nm = None
    first_record = True  # 첫 번째 레코드인지 여부를 추적

    for item in unsent_data:
        firm_nm = item.get('firm_nm', '알 수 없음')
        message_part = format_message(item)

        # 첫 번째 레코드 처리
        if first_record:
            current_message += f"●{firm_nm}\n"
            first_record = False
            previous_firm_nm = firm_nm
        elif firm_nm != previous_firm_nm:
            if previous_firm_nm is not None:
                current_message += "\n"
            current_message += f"\n●{firm_nm}\n"
            previous_firm_nm = firm_nm

        # 메시지의 길이가 3000자를 넘으면 분리
        if len(current_message) + len(message_part) > 3000:
            messages.append(current_message)
            current_message = message_part
        else:
            current_message += message_part

    if current_message:
        messages.append(current_message)

    return messages

def update_telegram_sent(file_path, target_date=None):
    directory = os.path.dirname(file_path)

    # 디렉터리가 존재하는지 확인하고, 없으면 생성합니다.
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        print(f"\n디렉터리 '{directory}'를 생성했습니다.")
    
    if not os.path.exists(file_path):
        print(f"\n파일 경로 '{file_path}'가 존재하지 않습니다.")
        return

    # 대상 날짜를 설정합니다. 날짜를 받지 않은 경우 오늘 날짜로 설정합니다.
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        
        if not isinstance(data, list):
            return

        # 대상 날짜의 항목들에 대해 telegram_sent 값을 True로 설정합니다.
        for item in data:
            if item.get("save_time", "").startswith(target_date):
                item["telegram_sent"] = True

        # 안전한 쓰기 방식 적용
        safe_json_dump(data, file_path)
        
        print(f"\n{file_path} 파일의 {target_date} 날짜 항목에 대해 telegram_sent 키가 True로 업데이트되었습니다.")
    except json.JSONDecodeError:
        print(f"Error updating {file_path}: File is corrupted.")


def filter_news_by_save_time(filename):
    if not os.path.exists(filename):
        return
        
    # 파일에서 JSON 데이터 읽기
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, list):
            return
    except (json.JSONDecodeError, FileNotFoundError):
        return

    # 오늘 날짜
    today = datetime.now()

    # 1주일 이내 날짜 계산
    one_week_ago = today - timedelta(days=1)

    # 뉴스 리스트 필터링
    filtered_news_list = [
        news for news in data
        if datetime.fromisoformat(news.get('save_time', today.isoformat())) >= one_week_ago
    ]

    # 안전한 쓰기 방식 적용
    safe_json_dump(filtered_news_list, filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process JSON files with specified action.')
    parser.add_argument('action', choices=['update', 'send'], help='Action to perform: update or send')
    parser.add_argument('file_path', type=str, help='Path to the JSON file to process')

    args = parser.parse_args()

    if args.action == 'send':
        results = get_unsent_main_ch_data_to_local_json(args.file_path)
        for result in results:
            print(result)
            print("\n" + "="*50 + "\n")  # 구분선 추가
