import os
import json
from datetime import datetime, timedelta
import argparse
import tempfile
from utils import report_json_store

# Legacy public facade for JSON-backed Telegram report delivery.
#
# Keep these function names and return shapes stable: older scheduler/CLI callers
# depend on them. New behavior should be implemented in report_json_store.py, where
# the helpers are named by responsibility and covered by compatibility tests.

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
    return report_json_store.format_legacy_message(data_list)


def save_data_to_local_json(filename, firm_id, board_id, firm_nm, pdf_url, article_title, article_url=None, download_url=None, telegram_sent=False):
    directory = os.path.dirname(filename)

    # 디렉터리가 존재하는지 확인하고, 없으면 생성합니다.
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"\n디렉터리 '{directory}'를 생성했습니다.")

    new_data = report_json_store.build_report_payload(
        firm_id=firm_id,
        board_id=board_id,
        firm_nm=firm_nm,
        pdf_url=pdf_url,
        article_title=article_title,
        article_url=article_url,
        download_url=download_url,
        telegram_sent=telegram_sent,
        save_time=datetime.now().isoformat(),
    )


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
        report_json_store.save_report_json_list(filename, existing_data)
        
        print(f"\n새 데이터가 {filename}에 성공적으로 저장되었습니다.")
        
        # 중복되지 않은 항목을 템플릿 형식으로 반환
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

    unsent_data = report_json_store.select_unsent_reports(
        data,
        target_date=today_str,
        excluded_firms=sent_firms,
    )

    # 디버깅 로그 추가
    print(f"\n필터링된 unsent_data: {unsent_data}")

    return report_json_store.format_legacy_message_chunks(unsent_data, message_limit=3500)

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

        report_json_store.mark_reports_sent_for_date(file_path, target_date)
        
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
