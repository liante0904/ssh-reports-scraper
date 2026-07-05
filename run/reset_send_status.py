# -*- coding:utf-8 -*- 
import asyncio
import os
import sys
import argparse
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

# 상위 디렉터리를 모듈 경로에 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.db_factory import get_db
from models.FirmInfo import FirmInfo
from utils.telegram_util import sendMarkDownText
from utils.telegram_message_builder import build_telegram_messages

# 환경 변수 로드
load_dotenv()
token = os.getenv('TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET')
chat_id = os.getenv('TELEGRAM_CHANNEL_ID_REPORT_ALARM')

async def reset_and_send(firm_order, date_str, board_order=None, do_send=False):
    db = get_db()
    
    # 1. 상태 초기화
    await db.reset_send_status(firm_order, date_str, board_order)
    
    firm_name = FirmInfo.firm_names[firm_order] if firm_order < len(FirmInfo.firm_names) else f"Unknown({firm_order})"
    logger.success(f"[{date_str}] {firm_name} 발송 상태 초기화 완료.")

    # 2. 즉시 발송 처리
    if do_send:
        logger.info(f"[{firm_name}] 즉시 발송을 시작합니다...")
        
        # 해당 업체/날짜의 데이터를 다시 읽어옴
        select_query = """
        SELECT report_id, firm_id, board_id, firm_nm, report_date,
               article_title, article_url, telegram_sent,
               download_url, writer, save_at,
               CASE WHEN firm_id = 19 THEN pdf_url ELSE telegram_url END AS telegram_url,
               report_unique_key
        FROM v_sec_reports_read
        WHERE firm_id = %s
          AND DATE(save_at) = %s
          AND telegram_sent IS NOT true
        """
        if board_order is not None:
            select_query += " AND board_id = %s"
            params = [firm_order, date_str, board_order]
        else:
            params = [firm_order, date_str]
        select_query += " ORDER BY save_at DESC, report_id DESC"
        rows = await db.execute_query(select_query, params)
        
        if rows:
            messages = build_telegram_messages(rows)
            logger.info(f"Sending {len(messages)} message chunks to Telegram...")
            
            success = True
            for msg in messages:
                try:
                    await sendMarkDownText(token=token, chat_id=chat_id, sendMessageText=msg)
                except Exception as e:
                    logger.error(f"Telegram send error: {e}")
                    success = False
            
            if success:
                # 발송 성공 시 다시 'Y'로 업데이트
                await db.daily_update_data(date_str=date_str.replace("-", ""), fetched_rows=rows, type='send')
                logger.success(f"[{firm_name}] 재발송 및 DB 상태 업데이트 완료.")
        else:
            logger.warning("발송할 대상이 없습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telegram 재발송 처리를 위한 발송 상태 초기화 및 즉시 발송 도구")
    parser.add_argument('--firm', type=int, default=11, help='증권사 번호 (기본값: 11 - DS투자증권)')
    parser.add_argument('--date', type=str, default=datetime.now().strftime('%Y-%m-%d'), help='조회 날짜 (YYYY-MM-DD, 기본값: 오늘)')
    parser.add_argument('--board', type=int, help='게시판 번호 (선택 사항)')
    parser.add_argument('--send', action='store_true', help='상태 초기화 후 즉시 발송 여부')
    
    args = parser.parse_args()

    asyncio.run(reset_and_send(args.firm, args.date, args.board, args.send))
