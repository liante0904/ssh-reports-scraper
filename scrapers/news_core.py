# -*- coding:utf-8 -*-
import asyncio
import aiohttp
import html
import datetime
from loguru import logger

async def fetch(session, url, max_retries=3, delay=3):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                if attempt < max_retries - 1:
                    logger.warning(f"{url} 접속 실패 (Status: {response.status}). {delay}초 후 재시도 ({attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"{url} 최종 접속 실패 (Status: {response.status})")
                    return None
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Error fetching {url}: {e}. {delay}초 후 재시도 ({attempt + 1}/{max_retries})")
                await asyncio.sleep(delay)
            else:
                logger.exception(f"Error fetching {url} {max_retries}회 시도 후 최종 실패: {e}")
                return None

def escape_html(text):
    return html.escape(text) if text else ""

async def scrape_chosun_biz(session) -> list[dict]:
    url = 'https://mweb-api.stockplus.com/api/news_items/all_news.json?scope=latest&limit=100'
    data = await fetch(session, url)
    if not data:
        return []
    
    articles = []
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    for item in data.get('newsItems', []):
        title_raw = item.get('title', '').strip()
        if not title_raw:
            continue
        title = escape_html(title_raw)
        link = item['url']
        
        articles.append({
            "firm_id": 100,
            "board_id": 0,
            "board_name": "조선비즈",
            "firm_nm": "조선비즈",
            "reg_dt": today_str,
            "article_title": title,
            "article_url": link,
            "download_url": link,
            "telegram_url": link,
            "pdf_url": link,
            "writer": "",
            "save_time": datetime.datetime.now().isoformat(),
            "report_unique_key": link,
        })
    return articles

async def scrape_naver_flash(session) -> list[dict]:
    url = 'https://m.stock.naver.com/api/json/news/newsListJson.nhn?category=flashnews'
    res = await fetch(session, url)
    if not res or 'result' not in res:
        return []
    data = res['result']
    
    articles = []
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    for item in data.get('newsList', []):
        title_raw = item.get('tit', '').strip()
        if not title_raw:
            continue
        title = escape_html(title_raw)
        link = f"https://m.stock.naver.com/investment/news/flashnews/{item['oid']}/{item['aid']}"
        unique_key = f"naver_flash_{item['oid']}_{item['aid']}"
        
        articles.append({
            "firm_id": 101,
            "board_id": 0,
            "board_name": "네이버 실시간",
            "firm_nm": "네이버",
            "reg_dt": today_str,
            "article_title": title,
            "article_url": link,
            "download_url": link,
            "telegram_url": link,
            "pdf_url": link,
            "writer": "",
            "save_time": datetime.datetime.now().isoformat(),
            "report_unique_key": unique_key,
        })
    return articles

async def scrape_naver_rank(session) -> list[dict]:
    url = 'https://m.stock.naver.com/api/json/news/newsListJson.nhn?category=ranknews'
    res = await fetch(session, url)
    if not res or 'result' not in res:
        return []
    data = res['result']
    
    articles = []
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    for item in data.get('newsList', []):
        title_raw = item.get('tit', '').strip()
        if not title_raw:
            continue
        title = escape_html(title_raw)
        link = f"https://m.stock.naver.com/investment/news/ranknews/{item['oid']}/{item['aid']}"
        unique_key = f"naver_rank_{item['oid']}_{item['aid']}"
        
        articles.append({
            "firm_id": 101,
            "board_id": 1,
            "board_name": "네이버 랭킹",
            "firm_nm": "네이버",
            "reg_dt": today_str,
            "article_title": title,
            "article_url": link,
            "download_url": link,
            "telegram_url": link,
            "pdf_url": link,
            "writer": "",
            "save_time": datetime.datetime.now().isoformat(),
            "report_unique_key": unique_key,
        })
    return articles

async def scrape_all_news() -> list[dict]:
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            scrape_chosun_biz(session),
            scrape_naver_flash(session),
            scrape_naver_rank(session)
        )
    return results[0] + results[1] + results[2]
