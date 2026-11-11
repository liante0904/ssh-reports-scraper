# -*- coding:utf-8 -*- 
import os
import requests
import re
import urllib.parse as urlparse
import urllib.request
import asyncio
from datetime import datetime
from loguru import logger

from playwright.async_api import async_playwright

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo
from models.ConfigManager import config


async def Koreainvestment_playwright_checkNewArticle():
    """Collect Korea Investment reports using the shared Playwright browser when available."""
    firm_id = 13
    json_data_list = []

    requests.packages.urllib3.disable_warnings()
    target_url = config.get_urls("Koreainvestment_13")[0]
    max_pages = int(os.getenv("KOREAMAX_PAGES", "10"))

    categories = [
        {"name": "전체", "board_order": 0, "script": None, "mkt_tp": "KR"},
        {"name": "미국 현지 리서치", "board_order": 10,
         "script": "onTab1Selected('stifel', 1);", "mkt_tp": "GLOBAL"},
    ]

    logger.debug("KoreaInvestment Scraper: Launching Playwright browser...")
    async with async_playwright() as playwright:
        endpoint = os.getenv("PLAYWRIGHT_WS_ENDPOINT")
        launch_args = ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        if endpoint:
            try:
                browser = await playwright.chromium.connect(endpoint)
            except Exception as exc:
                logger.warning(
                    "KoreaInvestment: shared Playwright browser unavailable at {}; "
                    "falling back to local Chromium: {}",
                    endpoint,
                    exc,
                )
                browser = await playwright.chromium.launch(headless=True, args=launch_args)
        else:
            browser = await playwright.chromium.launch(headless=True, args=launch_args)
        try:
            page = await browser.new_page()
            for category in categories:
                board_id = category["board_order"]
                firm_info = FirmInfo(firm_id, board_id)
                logger.debug(f"KoreaInvestment Scraper: Processing [{category['name']}]")

                await page.goto(target_url, wait_until="load")
                if category["script"]:
                    await page.evaluate(category["script"])
                    await page.wait_for_timeout(2000)

                for page_number in range(1, max_pages + 1):
                    if page_number > 1:
                        await page.evaluate(f"goPage({page_number});")
                        await page.wait_for_timeout(2000)

                    title_elements = await page.locator('//*[@id="searchResult"]/div/ul/li/a[1]/div[2]/span[1]').all()
                    link_elements = await page.locator('//*[@id="searchResult"]/div/ul/li/a[2]').all()
                    info_elements = await page.locator('//*[@id="searchResult"]/div/ul/li/a[1]/span').all()

                    if not title_elements:
                        logger.info(f"KoreaInvestment Scraper: No more articles on page {page_number}")
                        break

                    logger.info(f"KoreaInvestment Scraper: Collected {len(title_elements)} items on page {page_number}")
                    for title, link, article_info in zip(title_elements, link_elements, info_elements):
                        article_info_str = (await article_info.inner_text()).split(" ")
                        if len(article_info_str) < 2:
                            continue

                        article_url_raw = await link.get_attribute("onclick")
                        article_url = Koreainvestment_GET_LIST_ARTICLE_URL(article_url_raw)
                        json_data_list.append({
                            "firm_id": firm_id,
                            "board_id": board_id,
                            "firm_nm": firm_info.get_firm_name(),
                            "report_date": re.sub(r"[-./]", "", article_info_str[1]),
                            "telegram_url": article_url,
                            "pdf_file_url": article_url,
                            "article_title": await title.inner_text(),
                            "writer": article_info_str[0],
                            "report_unique_key": article_url,
                            "save_at": datetime.now().isoformat(),
                            "mkt_tp": category["mkt_tp"],
                        })
        except Exception as e:
            logger.error(f"Error during KoreaInvestment Playwright scraping: {e}")
        finally:
            await browser.close()

    logger.info(f"Total articles collected with Playwright: {len(json_data_list)}")
    return json_data_list

def Koreainvestment_GET_LIST_ARTICLE_URL(string):
    if not string: return ""
    string = string.replace("javascript:prePdfFileView2(", "").replace("javascript:prePdfFileView3(", "").replace("&amp;", "&").replace(")", "").replace("(", "").replace("'", "")
    params = [p.strip() for p in string.split(",")]
    
    if len(params) < 5: return ""

    category = "category1="+params[0] +"&"+ "category2=" + params[1]
    filename = params[2]
    option = params[3]
    datasubmitdate = params[4]
    
    air_yn = params[5] if len(params) > 5 else "N"
    kor_yn = params[6] if len(params) > 6 else "Y"
    special_yn = params[7] if len(params) > 7 else "N"

    r = Koreainvestment_MAKE_LIST_ARTICLE_URL(category, filename, option, datasubmitdate, air_yn, kor_yn, special_yn)

    parsed_url = urlparse.urlparse(r)
    query_params = urlparse.parse_qs(parsed_url.query)
    
    filepath = query_params.get('filepath', [''])[0]
    filename_val = query_params.get('filename', [''])[0]
    
    if filepath and filename_val:
        return f"http://file.truefriend.com/Storage/{filepath}/{filename_val}"
    else:
        return r

def Koreainvestment_MAKE_LIST_ARTICLE_URL(filepath, filename, option, datasubmitdate, air_yn, kor_yn, special_yn):
    filename = urllib.parse.quote(filename)
    host_name = "http://research.truefriend.com/streamdocs/openResearch"
    host_name2, host_name3 = "https://kis-air.com/kor/", "https://kis-air.com/us/"

    if filepath.startswith("?") or filepath.startswith("&"):
        filepath = filepath[1:]

    params = filepath.split("&")
    if len(params) == 2:
        p1, p2 = params[0], params[1]
        if (p1 == 'category1=01' and p2 in ['category2=01', 'category2=02', 'category2=03', 'category2=04', 'category2=05']):
            filepath = "research/research01"
        elif (p1 == 'category1=02' and p2 in ['category2=01', 'category2=02', 'category2=03', 'category2=04', 'category2=06', 'category2=08', 'category2=09', 'category2=10', 'category2=11', 'category2=12', 'category2=13', 'category2=14']):
            filepath = "research/research02"
        elif (p1 == 'category1=03' and p2 in ['category2=01', 'category2=02', 'category2=03']):
            filepath = "research/research03"
        elif (p1 == 'category1=04' and p2 in ['category2=00', 'category2=01', 'category2=02', 'category2=03']):
            filepath = "research/research04"
        elif p1 == 'category1=05':
            filepath = "research/research05"
        elif p1 == 'category1=07' and p2 == 'category2=01':
            filepath = "research/research07"
        elif p1 == 'category1=08' and p2 in ['category2=03', 'category2=04', 'category2=05']:
            filepath = "research/research08"
        elif p1 == 'category1=06' and p2 in ['category2=01', 'category2=02']:
            filepath = "research/research06"
        elif p1 == 'category1=09' and p2 == 'category2=00':
            filepath = "research/research11"
        elif p1 == 'category1=10' and p2 in ['category2=01', 'category2=04', 'category2=06']:
            if p2 == 'category2=06': filepath = "research/research_emailcomment"
            elif p2 == 'category2=04': filepath = "research/china"
            else: filepath = "research/research10"
        elif p1 == 'category1=14' and p2 == 'category2=01':
            filepath = "research/research14"
        elif p1 == 'category1=13' and p2 == 'category2=01':
            filepath = "research/research11"
        elif p1 == 'category1=17':
            filepath = "research/research17"
        elif p1 == 'category1=15' and p2 == 'category2=01':
            filepath = "research/research01"
        elif p1 == 'category1=16' and p2 == 'category2=01':
            filepath = "research/research15"

    if not option: option = "01"

    if params == ['category1=15', 'category2=01']:
        datasubmitdate = datasubmitdate.replace(".", "-")
        return f"{host_name2 if kor_yn == 'Y' else host_name3}{datasubmitdate}/{'special' if special_yn == 'Y' else 'daily'}"
    else:
        return f"{host_name}?filepath={urllib.parse.quote(filepath)}&filename={filename}&option={option}"

if __name__ == "__main__":
    results = asyncio.run(Koreainvestment_playwright_checkNewArticle())
    logger.info(f"Total articles fetched: {len(results)}")
