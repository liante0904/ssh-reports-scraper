# -*- coding:utf-8 -*- 
import os
import sys
import asyncio
import argparse
import datetime
from loguru import logger
from dotenv import load_dotenv

# 프로젝트 루트 경로 추가 (run 폴더의 상위 경로)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logger_util import setup_logger
setup_logger("scraper_af")

from models.SQLiteManager import SQLiteManager
from models.FirmInfo import FirmInfo
from modules.LS_0 import LS_detail
from modules.DBfi_19 import fetch_detailed_url

async def update_firm_telegram_url(date_str=None, target_firm_order=None):
    """
    telegram_update_required가 True인 증권사의 telegram_url 컬럼을 업데이트합니다.
    """
    logger.info(f"🚀 Starting Enrichment Process (Target: {target_firm_order if target_firm_order is not None else 'ALL'})")
    db = SQLiteManager()
    
    # firm_names 배열의 모든 인덱스를 순회
    for sec_firm_order in range(len(FirmInfo.firm_names)):
        if target_firm_order is not None and sec_firm_order != target_firm_order:
            continue

        firm_info = FirmInfo(sec_firm_order=sec_firm_order, article_board_order=0)
        firm_name = firm_info.get_firm_name()

        # 회사 이름이 있고, telegram_update_required가 True인 경우만 처리
        if firm_name and firm_info.telegram_update_required:
            if target_firm_order is not None:
                # 특정 회사 지정 시 전체 미비 건 조회
                records = await db.fetch_all_empty_telegram_url_articles(firm_info=firm_info)
            else:
                # 일간 배치 시 특정 날짜 기준 조회
                records = await db.fetch_daily_articles_by_date(firm_info=firm_info, date_str=date_str)
            
            if not records:
                logger.debug(f"[{firm_name}] No records found for enrichment.")
                continue

            logger.info(f"[{firm_name}] Found {len(records)} records for enrichment.")

            try:
                if sec_firm_order == 19:  # DB증권
                    # 데이터가 너무 많을 경우를 대비한 안전장치 (필요 시 조절)
                    if len(records) > 50:
                        logger.warning(f"[{firm_name}] Too many records ({len(records)}). Processing top 50.")
                        records = records[:50]
                        
                    update_records = await fetch_detailed_url(records)
                    for record in update_records:
                        if record.get('telegram_url'):
                            await db.update_telegram_url(
                                record['report_id'], 
                                record['telegram_url'], 
                                pdf_url=record.get('pdf_url') or record['telegram_url']
                            )
                            logger.info(f"[{firm_name}] Updated URL for ID {record['report_id']}")
                
                elif sec_firm_order == 0:  # LS증권
                    for record in records:
                        # LS_detail은 단건 또는 리스트 처리가 가능하므로 맞춰서 호출
                        res_list = await LS_detail(articles=[record], firm_info=firm_info)
                        if res_list:
                            updated_item = res_list[0]
                            await db.update_telegram_url(
                                updated_item['report_id'], 
                                updated_item['telegram_url'], 
                                updated_item.get('article_title'), 
                                pdf_url=updated_item.get('pdf_url') or updated_item['telegram_url']
                            )
                            logger.info(f"[{firm_name}] Updated URL for ID {updated_item['report_id']}")
                
                logger.success(f"[{firm_name}] Enrichment completed.")
            except Exception as e:
                logger.error(f"[{firm_name}] Enrichment failed: {e}")

async def main():
    parser = argparse.ArgumentParser(description="Scraper After Process (Enrichment)")
    parser.add_argument('--date', type=str, help='Target date (YYYYMMDD)', default=None)
    parser.add_argument('--firm', type=int, help='Target firm order (index)', default=None)
    args = parser.parse_args()

    load_dotenv()
    await update_firm_telegram_url(date_str=args.date, target_firm_order=args.firm)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
