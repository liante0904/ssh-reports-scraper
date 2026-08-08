# -*- coding:utf-8 -*- 
import sys
import os
import re
import asyncio
import aiohttp
from datetime import datetime
from loguru import logger

from bs4 import BeautifulSoup

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo
from models.ConfigManager import config

async def Daeshin_checkNewArticle():
    firm_id      = 17
    board_id = 0
    json_data_list = []

    firm_info = FirmInfo(
        firm_id=firm_id,
        board_id=board_id
    )
    logger.debug(f"Daeshin Scraper Start: {firm_info.get_firm_name()}")

    from urllib.parse import urljoin
    urls = config.get_urls("Daeshin_17")
    if not urls: return json_data_list
    url = urls[0]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Referer": url
    }

    async def fetch_hidden_values(session, url):
        """초기 페이지에서 hidden 필드 값을 추출하는 함수"""
        async with session.get(url, headers=headers) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            # hidden 필드 값 추출
            viewstate = soup.find(id="__VIEWSTATE")['value']
            viewstate_gen = soup.find(id="__VIEWSTATEGENERATOR")['value']
            event_validation = soup.find(id="__EVENTVALIDATION")['value']
            
            return viewstate, viewstate_gen, event_validation

    async def fetch_page_data(session, page, viewstate, viewstate_gen, event_validation, sem):
        """각 페이지의 데이터와 hidden 필드를 갱신하여 크롤링하는 함수"""
        data = {
            "ctl00$sm1": "ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$bt_refresh",
            "ctl00$ContentPlaceHolder1$hf_page": str(page),
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstate_gen,
            "__EVENTVALIDATION": event_validation,
            "__ASYNCPOST": "true",
            "ctl00$ContentPlaceHolder1$bt_refresh": ""
        }

        async with session.post(url, headers=headers, data=data) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            # 게시글 목록 추출
            items = soup.find_all("li")
            if not items:
                logger.info(f"Daeshin Scraper: No more items on page {page}")
                return None
            
            logger.info(f"Daeshin Scraper: Found {len(items)} items on page {page}")
            
            # 각 아이템(리포트)별 상세 조회를 병렬 비동기로 처리하기 위한 내부 함수
            async def process_item(item):
                title = item.find("strong", class_="title1").text.strip()
                if title.startswith("[대신증권 "):
                    title = title.replace("[대신증권 ", "[")
                report_date = item.find("span", class_="date").text.strip()
                author = item.find("span", class_="time").text.strip()
                
                link_tag = item.find("a")
                if link_tag and 'href' in link_tag.attrs:
                    href = link_tag['href']
                    source_url = urljoin(url, href)
                else:
                    logger.warning("No href found for a Daeshin item")
                    return
                
                # 대상 서버 과부하 방지 및 IP 차단 예방을 위해 세마포어로 동시 요청수 제어
                async with sem:
                    attach_url = await fetch_attach_url(session, source_url)

                if attach_url:
                    json_data_list.append({
                        "firm_id": firm_id,
                        "board_id": board_id,
                        "firm_nm": firm_info.get_firm_name(),
                        "report_date": re.sub(r"[-./]", "", report_date),
                        "source_url": source_url,
                        
                        "telegram_url": attach_url,
                        "pdf_file_url": attach_url,
                        "report_unique_key": attach_url,
                        "article_title": title,
                        "writer": author,
                        "save_at": datetime.now().isoformat()
                    })

            # 전체 아이템에 대한 병렬 태스크 실행
            item_tasks = [process_item(item) for item in items]
            await asyncio.gather(*item_tasks)

    async def fetch_attach_url(session, source_url):
        """source_url 페이지에서 pdf_url 추출"""
        try:
            async with session.get(source_url, headers=headers) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                attach_element = soup.find(id="btnPdfLoad")
                
                if attach_element:
                    return attach_element['href']
        except Exception as e:
            logger.error(f"Error fetching attach URL from {source_url}: {e}")
        return None

    # 명시적 타임아웃 세팅 (Hanging으로 인한 무한 대기 현상 전면 방지)
    timeout = aiohttp.ClientTimeout(total=15)
    sem = asyncio.Semaphore(3)  # 최대 동시 3개 상세조회 허용

    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            viewstate, viewstate_gen, event_validation = await fetch_hidden_values(session, url)
            tasks = []
            for page in range(1, 5):
                tasks.append(fetch_page_data(session, page, viewstate, viewstate_gen, event_validation, sem))
            
            await asyncio.gather(*tasks)
        except asyncio.TimeoutError:
            # The source occasionally stalls from the production IP.  This is
            # an empty-result, retry-on-next-schedule condition, not a scraper
            # crash that should trigger a watchdog error alert.
            logger.warning("Daeshin scraping timed out; retrying on the next schedule.")
        except Exception as e:
            logger.error(
                f"Error during Daeshin scraping process: {type(e).__name__}: {e!r}"
            )
            
        return json_data_list


async def main():
    articles = await Daeshin_checkNewArticle()
    logger.info(f"Total Daeshin articles fetched: {len(articles)}")
    for item in articles[:5]:
        logger.debug(item)

if __name__ == "__main__":
    asyncio.run(main())
