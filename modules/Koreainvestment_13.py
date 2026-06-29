# -*- coding:utf-8 -*- 
import os
import requests
import re
import urllib.parse as urlparse
import urllib.request
import asyncio
from datetime import datetime
import time
from loguru import logger

# selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo
from models.ConfigManager import config


async def Koreainvestment_selenium_checkNewArticle():
    sec_firm_order      = 13
    json_data_list = []

    requests.packages.urllib3.disable_warnings()

    TARGET_URL_0 = config.get_urls("Koreainvestment_13")[0]
    
    # 백필 등 고페이지 수집용 — KOREAMAX_PAGES 환경변수 (기본 10)
    _max_pages = int(os.getenv("KOREAMAX_PAGES", "10"))

    CATEGORIES = [
        {"name": "전체", "board_order": 0, "script": None, "mkt_tp": "KR"},
        {"name": "미국 현지 리서치", "board_order": 10, "script": "onTab1Selected('stifel', 1);", "mkt_tp": "GLOBAL"}
    ]

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-allow-origins=*")
    chrome_options.add_argument("--disable-software-rasterizer")

    import platform as _plat
    _is_arm = _plat.machine() in ("aarch64", "arm64")

    # 시스템에 설치된 chromedriver 우선 확인 (ARM64 호환성 해결)
    system_chromedriver = "/usr/bin/chromedriver"
    system_chrome = "/usr/bin/chromium-browser"
    snap_chromedriver = "/snap/bin/chromium.chromedriver"

    if os.path.exists(system_chromedriver):
        logger.debug(f"KoreaInvestment Scraper: Using system chromedriver at {system_chromedriver}")
        service = Service(executable_path=system_chromedriver)
    elif os.path.exists(snap_chromedriver):
        logger.debug(f"KoreaInvestment Scraper: Using snap chromedriver at {snap_chromedriver}")
        service = Service(executable_path=snap_chromedriver)
    elif _is_arm:
        # ARM64에서는 webdriver_manager 대신 시스템 패키지 필수
        logger.warning("KoreaInvestment: ARM64 detected, trying default Service()")
        service = Service()
    else:
        # x86_64 환경 (GitHub Actions 등) — webdriver_manager 사용
        try:
            logger.debug("KoreaInvestment Scraper: System chromedriver not found, trying ChromeDriverManager...")
            service = Service(ChromeDriverManager().install())
        except Exception as e:
            logger.error(f"Failed to install ChromeDriver: {e}")
            service = Service()

    binary_paths = [
        "/usr/bin/chromium-browser", "/usr/bin/chromium", "/usr/bin/google-chrome",
        "/snap/bin/chromium",
    ]
    for bp in binary_paths:
        if os.path.exists(bp):
            chrome_options.binary_location = bp
            break

    logger.debug("KoreaInvestment Scraper: Launching headless browser...")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        for cat in CATEGORIES:
            article_board_order = cat["board_order"]
            firm_info = FirmInfo(sec_firm_order, article_board_order)
            logger.debug(f"KoreaInvestment Scraper: Processing [{cat['name']}]")

            driver.get(TARGET_URL_0)
            driver.implicitly_wait(5)

            if cat["script"]:
                driver.execute_script(cat["script"])
                time.sleep(2)

            for page in range(1, _max_pages + 1):
                if page > 1:
                    driver.execute_script(f"goPage({page});")
                    time.sleep(2)

                title_elements = driver.find_elements(By.XPATH, '//*[@id="searchResult"]/div/ul/li/a[1]/div[2]/span[1]')
                link_elements = driver.find_elements(By.XPATH, '//*[@id="searchResult"]/div/ul/li/a[2]')
                info_elements = driver.find_elements(By.XPATH, '//*[@id="searchResult"]/div/ul/li/a[1]/span')
                
                if not title_elements:
                    logger.info(f"KoreaInvestment Scraper: No more articles on page {page}")
                    break
                
                logger.info(f"KoreaInvestment Scraper: Collected {len(title_elements)} items on page {page}")

                for title, link, article_info in zip(title_elements, link_elements, info_elements):
                    LIST_ARTICLE_TITLE = title.text
                    LIST_ARTICLE_URL_RAW = link.get_attribute("onclick")
                    article_info_str = article_info.text.split(' ')
                    
                    if len(article_info_str) < 2: continue

                    LIST_ARTICLE_URL = Koreainvestment_GET_LIST_ARTICLE_URL(LIST_ARTICLE_URL_RAW)
                    
                    json_data_list.append({
                        "sec_firm_order":sec_firm_order,
                        "article_board_order":article_board_order,
                        "firm_nm":firm_info.get_firm_name(),
                        "reg_dt":re.sub(r"[-./]", "", article_info_str[1]),
                        "download_url": LIST_ARTICLE_URL,
                        "telegram_url": LIST_ARTICLE_URL,
                        "pdf_url": LIST_ARTICLE_URL,
                        "article_title":LIST_ARTICLE_TITLE,
                        "writer": article_info_str[0],
                        "key": LIST_ARTICLE_URL,
                    "report_unique_key": LIST_ARTICLE_URL,
                        "save_time": datetime.now().isoformat(),
                        "mkt_tp": cat["mkt_tp"]
                    })
    except Exception as e:
        logger.error(f"Error during KoreaInvestment scraping: {e}")
    finally:
        driver.quit()
        
    logger.info(f"Total articles collected: {len(json_data_list)}")
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
    results = asyncio.run(Koreainvestment_selenium_checkNewArticle())
    logger.info(f"Total articles fetched: {len(results)}")
