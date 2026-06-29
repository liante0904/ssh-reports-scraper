# -*- coding:utf-8 -*- 
import gc
import requests
import re
from datetime import datetime
from loguru import logger

from bs4 import BeautifulSoup
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo
from models.ConfigManager import config


def DS_checkNewArticle(full_scan=False):
    sec_firm_order = 11  # DS투자증권 고유 식별자
    requests.packages.urllib3.disable_warnings()

    TARGET_URL_TUPLE = config.get_urls("DS_11")

    json_data_list = []
    current_time_suffix = datetime.now().strftime("T%H:%M:%S")

    for article_board_order, BASE_URL in enumerate(TARGET_URL_TUPLE):
        firm_info = FirmInfo(
            sec_firm_order=sec_firm_order,
            article_board_order=article_board_order
        )
        logger.debug(f"DS Scraper Start: {firm_info.get_firm_name()} Board {article_board_order} (Full Scan: {full_scan})")

        page = 1
        while True:
            target_url_with_page = f"{BASE_URL}&page={page}"
            logger.info(f"Scraping DS: {firm_info.get_board_name()} - Page {page}")

            try:
                response = requests.get(target_url_with_page, verify=False, timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # 게시글 목록 추출
                table = soup.select_one("#fboardlist > div > table")
                if not table:
                    logger.warning(f"Table not found in {target_url_with_page}")
                    break

                rows = table.select("tbody tr")
                
                # 빈 페이지 체크 (데이터가 없는 경우)
                if not rows or (len(rows) == 1 and "게시물이 없습니다" in rows[0].get_text()):
                    logger.info(f"No more data in page {page}. Stopping.")
                    break

                logger.info(f"DS Scraper: Found {len(rows)} rows in page {page}")

                for row in rows:
                    # 제목과 URL 추출
                    title_element = row.select_one(".bo_tit a")
                    if not title_element:
                        continue 

                    title = title_element.get_text(strip=True)
                    article_url = title_element["href"]

                    # wr_id 및 bo_table 추출을 위한 정규식
                    wr_id_match = re.search(r"wr_id=(\d+)", article_url)
                    bo_table_match = re.search(r"bo_table=([^&]+)", article_url)
                    
                    if wr_id_match and bo_table_match:
                        wr_id = wr_id_match.group(1)
                        bo_table = bo_table_match.group(1)
                        pdf_url = f"https://www.ds-sec.co.kr/bbs/download.php?bo_table={bo_table}&wr_id={wr_id}&no=0"
                        telegram_url = ""
                    else:
                        pdf_url = "없음"
                        telegram_url = ""

                    # 날짜 추출
                    date_element = row.select_one(".td_datetime")
                    date_str = date_element.get_text(strip=True) if date_element else datetime.now().strftime("%Y-%m-%d")
                    reg_dt = re.sub(r"[-./]", "", date_str)

                    # SAVE_TIME을 게시글 등록일 기준으로 설정 (시간은 현재 시간 유지)
                    # date_str 형식: 2024-04-13 또는 04-13
                    if len(date_str) <= 5: # "MM-DD" 형식인 경우 현재 연도 붙임
                        save_time = f"{datetime.now().year}-{date_str}{current_time_suffix}"
                    else:
                        save_time = f"{date_str}{current_time_suffix}"

                    # JSON 데이터 구성
                    json_data_list.append({
                        "sec_firm_order": sec_firm_order,
                        "article_board_order": article_board_order,
                        "firm_nm": firm_info.get_firm_name(),
                        "article_title": title,
                        "article_url": article_url,
                        "telegram_url": telegram_url,
                        "pdf_url": pdf_url,
                        "reg_dt": reg_dt,
                        "save_time": save_time,
                        "key": pdf_url if pdf_url != "없음" else article_url,
                    "report_unique_key": pdf_url if pdf_url != "없음" else article_url
                    })

                # 한 페이지만 가져올 경우 루프 종료
                if not full_scan:
                    break
                
                page += 1
                # 무한 루프 방지용 (최대 50페이지까지만)
                if page > 50:
                    break

            except Exception as e:
                logger.error(f"Error scraping {target_url_with_page}: {e}")
                break

        # 메모리 정리
        gc.collect()

    return json_data_list

if __name__ == "__main__":
    import argparse
    from models.db_factory import get_db
    
    parser = argparse.ArgumentParser(description="DS투자증권 스크래퍼 직접 실행")
    parser.add_argument('--full', action='store_true', help='전체 페이지를 스캔할지 여부 (기본값: False)')
    args = parser.parse_args()

    logger.info(f"DS투자증권 직접 실행 시작 (Full Scan: {args.full})")
    
    # 스크래핑 수행
    results = DS_checkNewArticle(full_scan=args.full)
    
    if results:
        logger.info(f"총 {len(results)}개의 데이터를 추출했습니다. DB 저장을 시도합니다.")
        db = get_db()
        try:
            ins, upd = db.insert_json_data_list(results)
            logger.success(f"DB 저장 완료: 신규 {ins}건, 업데이트 {upd}건.")
        except Exception as e:
            logger.error(f"DB 저장 중 오류 발생: {e}")
    else:
        logger.warning("추출된 데이터가 없습니다.")
