import asyncio
from datetime import datetime
import re  # 정규식 사용을 위한 import
import os
import sys
import json
from bs4 import BeautifulSoup
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo
from models.WebScraper import AsyncWebScraper
from models.ConfigManager import config

sec_firm_order = 12

BASE_URLS = config.get_urls("eugenefn_12")

def get_eugene_headers():
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 8.0.0; SM-G955U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Content-Type": "application/x-www-form-urlencoded",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"'
    }
    
    # 쿠키 로드 시도
    cookie_path = os.path.join(os.path.dirname(__file__), '..', 'json', 'eugene_cookies.json')
    if os.path.exists(cookie_path):
        try:
            with open(cookie_path, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
                if isinstance(cookies, dict):
                    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
                    headers["Cookie"] = cookie_str
                elif isinstance(cookies, str):
                    headers["Cookie"] = cookies
            logger.info("Eugene Scraper: Loaded cookies from eugene_cookies.json")
        except Exception as e:
            logger.error(f"Eugene Scraper: Failed to load cookies: {e}")
            
    return headers

async def parse_article_list(html_text, article_board_order):
    articles = []
    if html_text:
        soup = BeautifulSoup(html_text, 'html.parser')
        list_items = soup.find_all('li')
        for item in list_items:
            a_tag = item.find('a')
            if a_tag:
                url = a_tag['href']
                # 상세 페이지 URL이 상대 경로인 경우 처리
                if url.startswith('/'):
                    url = 'https://m.eugenefn.com' + url
                
                # 더블 슬래시(//) 정규화 (보안 솔루션이나 라이브러리 오작동 방지)
                if "//" in url:
                    # 프로토콜 부분(https://)을 제외한 나머지 부분의 더블 슬래시 제거
                    protocol, rest = url.split("://", 1)
                    url = f"{protocol}://{rest.replace('//', '/')}"
                
                title_tag = item.find('strong', class_='title line2')
                title = title_tag.text.strip() if title_tag else ''
                
                date_tag = item.find('span', class_='date')
                date = date_tag.text.strip() if date_tag else ''
                
                # reg_dt 포맷 변경: yyyyMMdd
                reg_dt = re.sub(r"[-./]", "", date)
                
                writer_tag = item.find('span', class_='writer')
                writer = writer_tag.text.strip() if writer_tag else ''
                
                firm_info = FirmInfo(
                    sec_firm_order=sec_firm_order,
                    article_board_order=article_board_order
                )

                articles.append({
                    "sec_firm_order": sec_firm_order,
                    "article_board_order": article_board_order,
                    "firm_nm": firm_info.get_firm_name(),
                    "reg_dt": reg_dt,
                    "writer": writer,
                    "article_url": '',
                    "download_url": url,  # 필요시 변경
                    "telegram_url": url,
                    "pdf_url": url,
                    "article_title": title,
                    "key": url,
                    "report_unique_key": url,
                    "save_time": datetime.now().isoformat()
                })
                
    return articles

async def eugene_checkNewArticle():
    all_articles = []
    headers = get_eugene_headers()
    
    for article_board_order, base_url in enumerate(BASE_URLS):
        referer_url = base_url.replace('Add.do', '.do')
        firm_info = FirmInfo(sec_firm_order, article_board_order)
        logger.debug(f"Eugene Scraper Start: {firm_info.get_firm_name()} Board {article_board_order}")
        
        # 각 요청마다 Referer 헤더 업데이트
        current_headers = headers.copy()
        current_headers["Referer"] = referer_url
        
        scraper = AsyncWebScraper(target_url=base_url, headers=current_headers)
        for page_no in range(1, 6):
            payload = f"pageNo={page_no}&add=Y"
            try:
                response_soup = await scraper.Post(data=payload)
                if response_soup:
                    # 응답이 제한된 경우(예: 로그인 페이지)를 감지하는 로직 추가 가능
                    if "로그인" in str(response_soup) or "login" in str(response_soup).lower():
                        logger.warning(f"Eugene Scraper: Session might be expired or restricted for {base_url}")
                        break
                        
                    html_text = str(response_soup)
                    articles = await parse_article_list(html_text, article_board_order)
                    all_articles.extend(articles)
                    logger.info(f"Eugene Scraper: Found {len(articles)} articles on page {page_no}")
                else:
                    logger.warning(f"Status: Failed for {base_url} (Page {page_no})")
            except Exception as e:
                logger.error(f"Error scraping {base_url} page {page_no}: {e}")

    logger.info(f"Total articles scraped: {len(all_articles)}")
    return all_articles

async def main():
    articles = await eugene_checkNewArticle()
    logger.debug(json.dumps(articles, indent=4, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
