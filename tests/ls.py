import os
import gc
import logging
import json
import re
import sys
import urllib.parse as urlparse
import urllib.request
import asyncio
import aiohttp
from datetime import datetime, timedelta, date
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from bs4 import BeautifulSoup
from models.FirmInfo import FirmInfo
from models.WebScraper import AsyncWebScraper  # 비동기 웹 스크래퍼가 필요합니다.
from models.SQLiteManager import SQLiteManager
from package.json_to_sqlite import insert_json_data_list
from utils.date_util import GetCurrentDate
from models.ConfigManager import config

async def fetch_html(session, url):
    async with session.get(url, ssl=False) as response:
        return await response.text()

async def LS_checkNewArticle():
    db_manager = SQLiteManager()
    json_data_list = []
    sec_firm_order = 0
    firm_nm = "LS증권"

    TARGET_URLS = config.get_urls("LS_0")
    if not TARGET_URLS:
        raise RuntimeError("Missing LS_0 URLs. Populate ~/secrets/ssh-reports-scraper/secrets.json first.")

    async with aiohttp.ClientSession() as session:
        for article_board_order, base_url in enumerate(TARGET_URLS):
            firm_info = FirmInfo(
                sec_firm_order=sec_firm_order,
                article_board_order=article_board_order
            )

            page = 1
            while True:
                if article_board_order == 0:
                    print(f"{article_board_order}는 이제 그만함")
                    break
                if page == 20:
                    break

                TARGET_URL = f"{base_url}&currPage={page}"
                html = await fetch_html(session, TARGET_URL)
                soup = BeautifulSoup(html, 'html.parser')

                if soup.select_one(".no_data") and "등록된 데이터가 없습니다." in soup.select_one(".no_data").text:
                    print(f"{TARGET_URL}: 등록된 데이터가 없으므로 종료합니다.")
                    break

                soupList = soup.select('#contents > table > tbody > tr')
                today = date.today()
                seven_days_ago = today - timedelta(days=7)

                for item in soupList:
                    str_txt = item.get_text()
                    if "등록된 데이터가 없습니다." in str_txt:
                        print("등록된 데이터가 없습니다.")
                        break

                    str_date = item.select('td')[3].get_text().strip()
                    post_date_obj = datetime.strptime(str_date, '%Y.%m.%d').date()

                    link_element = item.select_one('a')
                    LIST_ARTICLE_URL = 'https://www.ls-sec.co.kr/EtwFrontBoard/' + link_element['href'].replace("amp;", "")
                    LIST_ARTICLE_TITLE = link_element.get_text().strip().replace("...", "")
                    writer = item.select('td')[2].get_text().strip()
                    reg_dt = post_date_obj.strftime('%Y%m%d')

                    select_query = """
                    SELECT * FROM data_main_daily_send 
                    WHERE key like '%upload% AND 'firm_nm = ? AND article_title LIKE ? LIMIT 1
                    """
                    params = (firm_nm, f"%{LIST_ARTICLE_TITLE}%")
                    print(params)
                    result = db_manager.execute_query(select_query, params)

                    if result and "status" not in result:
                        record_id = result[0]['id']
                        update_query = """
                        UPDATE data_main_daily_send 
                        SET reg_dt = ?, key = ? 
                        WHERE id = ?
                        """
                        db_manager.execute_query(update_query, (reg_dt, LIST_ARTICLE_URL, record_id))
                        print(f"Updated record with id {record_id}: reg_dt={reg_dt}, key={LIST_ARTICLE_URL}")

                page += 1
                print(f"{page} 진행중...")
                await asyncio.sleep(0.5)

    gc.collect()
    return json_data_list

async def main():
    await LS_checkNewArticle()

if __name__ == "__main__":
    asyncio.run(main())
