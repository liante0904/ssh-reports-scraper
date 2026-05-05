# -*- coding:utf-8 -*- 
import os
import re
import subprocess
import datetime
from loguru import logger

# 로그 파일 경로는 환경 변수로부터 가져옵니다
LOG_FILE = os.getenv('LOG_FILE', '~/logs/logfile.log')
LOG_FILE = os.path.expanduser(LOG_FILE)  # ~ 를 홈 디렉토리로 확장

# 한글 인코딩 처리 및 wget으로 다운로드
async def download_file_wget(report_info_row, URL=None, FILE_NAME=None):
    """
    파일을 주어진 URL에서 다운로드하고 지정된 파일 이름으로 저장합니다.
    
    매개변수:
    report_info_row (dict): 보고서 정보가 담긴 딕셔너리.
    URL (str): 파일을 다운로드할 URL.
    FILE_NAME (str): 저장할 파일 이름.
    
    반환:
    bool: 파일이 성공적으로 다운로드되거나 이미 존재하는 경우 True, 그렇지 않으면 False.
    """
    logger.info("download_file_wget()")

    BOARD_NM = ''
    URL = (
        report_info_row.get('download_url') if report_info_row.get('download_url')
        else report_info_row.get('article_url') if report_info_row.get('article_url')
        else report_info_row.get('telegram_url')
    )

    if FILE_NAME is None:
        # FILE_NAME이 None인 경우 기존 로직으로 파일 이름 생성
        FILE_NAME = report_info_row.get('article_title', 'Unknown')
        FIRM_NAME = report_info_row.get('firm_nm', 'Unknown')
        
        KST = datetime.timezone(datetime.timedelta(hours=9))
        now_kst = datetime.datetime.now(KST)
        today_str = now_kst.strftime('%Y%m%d')
        
        # report_info_row['reg_dt'] 값이 있으면 이를 사용하고 없으면 현재 날짜 사용
        DATE_PART = report_info_row['reg_dt'][2:8] if report_info_row.get('reg_dt') else today_str[2:8]

        # DATE_PART가 비어 있는 경우 현재 날짜를 기본값으로 설정
        if not DATE_PART:
            DATE_PART = today_str[2:8]
            
        # 파일명 지정
        FILE_NAME = FILE_NAME.replace(DATE_PART, "").replace(today_str, "")
        FILE_NAME = DATE_PART + "_" + BOARD_NM + "_" + FILE_NAME + "_" + FIRM_NAME

        # 파일명 길이 제한 처리
        MAX_FILE_NAME_LENGTH = 240
        if len(FILE_NAME) > MAX_FILE_NAME_LENGTH:
            FILE_NAME = FILE_NAME[:MAX_FILE_NAME_LENGTH]

        # 확장자 .pdf 추가
        FILE_NAME += '.pdf'
        
    else:
        # FILE_NAME이 주어진 경우 해당 값을 사용하며, 확장자가 없는 경우 .pdf 추가
        FILE_NAME = FILE_NAME.replace(".pdf", "").replace(".PDF", "")
        if not FILE_NAME.lower().endswith('.pdf'):
            FILE_NAME += '.pdf'

    # 파일명에서 사용할 수 없는 문자 제거
    ATTACH_FILE_NAME = re.sub(r"[\/:*?\"<>|]", '', FILE_NAME)  # 첫 번째 조건
    ATTACH_FILE_NAME = re.sub(r"[^\w\s.]", "", ATTACH_FILE_NAME)  # 두 번째 조건 수정: .을 제외한 특수 문자 제거

    logger.info('convert URL:', URL)
    logger.info('convert ATTACH_FILE_NAME:', ATTACH_FILE_NAME)

    if URL is None:
        logger.info('URL값이 유효하지 않아 종료')
        return True
    # 파일이 이미 존재하는지 확인
    if os.path.exists(ATTACH_FILE_NAME):
        log_message = f"파일 '{ATTACH_FILE_NAME}'이(가) 이미 존재합니다. 다운로드를 건너뜁니다."
        logger.info(log_message)
        if not os.path.exists(os.path.dirname(LOG_FILE)):
            os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, 'a') as log_file:
            log_file.write(log_message + '\n')
        return True  # 파일이 이미 존재하므로 성공으로 처리

    # wget 명령어 생성 및 실행
    wget_command = [
        'wget', '--user-agent="Mozilla/5.0"', '-O', ATTACH_FILE_NAME, URL,
        '--max-redirect=10', '--retry-connrefused', '--waitretry=5',
        '--read-timeout=20', '--timeout=15', '--tries=5', '--no-check-certificate'
    ]

    try:
        # wget_command 리스트를 문자열로 변환
        wget_command_str = ' '.join(wget_command)
        
        result = subprocess.run(wget_command, check=True, text=True, capture_output=True)
        
        # 로그 메시지에 사용할 wget_command
        log_message = wget_command_str
        log_message += f"\n파일 다운로드 완료: {ATTACH_FILE_NAME}"
        
        logger.info(log_message)
        
        # 로그 파일에 기록
        if not os.path.exists(os.path.dirname(LOG_FILE)):
            os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, 'a') as log_file:
            log_file.write(log_message + '\n')
            log_file.write(result.stdout + '\n')  # 성공 시 출력도 로그에 기록
        
        return True  # 파일 다운로드 성공 시 True 반환

    except subprocess.CalledProcessError as e:
        error_message = f"wget 다운로드 실패: {e}"
        
        logger.error(error_message)
        
        # 에러 메시지를 로그 파일에 기록
        if not os.path.exists(os.path.dirname(LOG_FILE)):
            os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, 'a') as log_file:
            log_file.write(error_message + '\n')
            log_file.write(e.stderr + '\n')  # 에러 메시지도 로그에 기록
        
        return False  # 다운로드 실패 시 False 반환
