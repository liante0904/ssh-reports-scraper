# -*- coding:utf-8 -*- 
import os
import gc
import requests
import urllib.request
import sys
from bs4 import BeautifulSoup
import time
import asyncio
import aiohttp
from aiohttp import ClientSession
import re
import urllib.parse
from loguru import logger

from datetime import datetime, timedelta, date
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.WebScraper import SyncWebScraper
from models.FirmInfo import FirmInfo
from models.db_factory import get_db
from models.ConfigManager import config

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

skip_boards = set()
USE_WARP_ONLY = False  # 직접 접속 실패 시 전역적으로 WARP만 사용하도록 설정

# 프록시 설정 (환경 변수가 없으면 로컬 기본값 사용)
SOCKS_PROXY = os.getenv("SOCKS_PROXY_URL", "socks5h://localhost:9091")
LS_DIRECT_RETRIES = int(os.getenv("LS_DIRECT_RETRIES", "2"))
LS_WARP_RETRIES = int(os.getenv("LS_WARP_RETRIES", "5"))
LS_SEARCH_DAYS = int(os.getenv("LS_SEARCH_DAYS", "10"))  # msg.ls-sec.co.kr URL 탐색 날짜 범위 (±N일)
LS_LIST_DELAY_MIN = float(os.getenv("LS_LIST_DELAY_MIN", "2.0"))
LS_LIST_DELAY_MAX = float(os.getenv("LS_LIST_DELAY_MAX", "3.0"))
LS_DETAIL_DELAY_MIN = float(os.getenv("LS_DETAIL_DELAY_MIN", "2.0"))
LS_DETAIL_DELAY_MAX = float(os.getenv("LS_DETAIL_DELAY_MAX", "4.0"))
PROXIES = {
    'http': SOCKS_PROXY,
    'https': SOCKS_PROXY
}
LS_PUBLIC_ORIGIN = os.getenv("LS_PUBLIC_ORIGIN", "https://www.ls-sec.co.kr")
LS_HOME_URL = f"{LS_PUBLIC_ORIGIN}/"
LS_FRONT_BOARD_PREFIX = f"{LS_PUBLIC_ORIGIN}/EtwFrontBoard/"
LS_UPLOAD_PREFIX = f"{LS_PUBLIC_ORIGIN}/upload/EtwBoardData"
LS_MSG_ORIGIN = os.getenv("LS_MSG_ORIGIN", "https://msg.ls-sec.co.kr")
LS_NLS_ORIGIN = os.getenv("LS_NLS_ORIGIN", "https://nls-sec.co.kr")
LS_MSG_PREFIX = f"{LS_MSG_ORIGIN}/"
LS_NLS_PREFIX = f"{LS_NLS_ORIGIN}/"
LS_MSG_EUM_PREFIX = f"{LS_MSG_ORIGIN}/eum"


def _article_report_date(article: dict) -> str:
    return str(article.get("report_date") or "")


def _article_unique_key(article: dict) -> str:
    return str(article.get("report_unique_key") or article.get("source_url") or "")

def get_soup_with_warp(url, headers):
    global USE_WARP_ONLY
    for attempt in range(1, LS_WARP_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, proxies=PROXIES, verify=False, timeout=50)
            response.raise_for_status()
            return BeautifulSoup(response.content, "html.parser")
        except Exception as e:
            if attempt < LS_WARP_RETRIES:
                time.sleep(attempt * 5)
            else:
                logger.error(f"LS WARP 최종 실패 (시도 {attempt}/{LS_WARP_RETRIES}): {url} ({e})")
                return None

def LS_checkNewArticle(page=1, is_imported=False, skip_boards=None, max_pages=2):
    """LS 증권 목록 스크래핑 + DB 키 비교 → 신규 레코드만 반환.

    max_pages: 스크래핑할 페이지 수 (기본 2페이지까지 긁음)
    """
    global USE_WARP_ONLY
    firm_id = 0
    json_data_list = []
    requests.packages.urllib3.disable_warnings()

    base_urls = config.get_urls("LS_0")
    
    if skip_boards is None:
        skip_boards = set()

    # ── WARP warm-up: SOCKS5 터널 선연결로 첫 요청 SSL EOF 방지 ──
    warm_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
    }
    for _ in range(3):
        try:
            requests.get(LS_HOME_URL, headers=warm_headers,
                         proxies=PROXIES, verify=False, timeout=20)
            logger.debug("LS WARP warm-up 완료")
            break
        except Exception:
            time.sleep(2)
    else:
        logger.warning("LS WARP warm-up 실패 (스크래핑 계속 진행)")

    # ── 페이지 순회 (기본 1~2페이지) ──
    for p in range(page, page + max_pages):
        if p == 1:
            TARGET_URL_TUPLE = tuple(base_urls)
        else:
            TARGET_URL_TUPLE = tuple(f"{url}&currPage={p}" for url in base_urls)

        page_has_articles = False

        for board_id, TARGET_URL in enumerate(TARGET_URL_TUPLE):
            if board_id in skip_boards:
                continue
            # 템플릿 URL(K_{filename})은 실제 게시판이 아니므로 건너뜀
            if "K_{filename}" in TARGET_URL:
                skip_boards.add(board_id)
                continue

            soupList = []
            soup = None

            import random
            time.sleep(random.uniform(LS_LIST_DELAY_MIN, LS_LIST_DELAY_MAX))

            firm_info = FirmInfo(
                firm_id=firm_id,
                board_id=board_id
            )

            # 1차 시도: 직접 접속
            direct_headers = SyncWebScraper(TARGET_URL, firm_info).headers
            if USE_WARP_ONLY:
                soup = None
            else:
                for direct_attempt in range(1, LS_DIRECT_RETRIES + 1):
                    try:
                        resp = requests.get(TARGET_URL, headers=direct_headers, verify=False, timeout=30)
                        resp.raise_for_status()
                        soup = BeautifulSoup(resp.content, "html.parser")
                        soupList = soup.select('#contents > table > tbody > tr')
                        break
                    except Exception as e:
                        soup = None
                        if direct_attempt < LS_DIRECT_RETRIES:
                            logger.info(f"LS 직접 접속 실패 {direct_attempt}/{LS_DIRECT_RETRIES}, 재시도: {TARGET_URL} ({e})")
                            time.sleep(direct_attempt)
                
                if soup is None:
                    logger.warning(f"LS 직접 접속 실패로 이후 모든 요청은 WARP를 사용합니다: {TARGET_URL}")
                    USE_WARP_ONLY = True

            if soup is None:
                soup = get_soup_with_warp(TARGET_URL, direct_headers)
                if soup:
                    soupList = soup.select('#contents > table > tbody > tr')
                else:
                    skip_boards.add(board_id)

            logger.info(f"{firm_info.get_firm_name()}의 {firm_info.get_board_name()} 게시판 p.{p}... (Found {len(soupList)} articles)")

            if not soupList and not is_imported:
                continue

            page_has_articles = True

            for list_item in soupList:
                try:
                    writer = list_item.select('td')[2].get_text().strip()
                    str_date = list_item.select('td')[3].get_text().strip()
                    a_tag = list_item.select_one('a')
                    if not a_tag: continue

                    raw_href = a_tag['href'].replace("amp;", "")
                    LIST_ARTICLE_URL = LS_FRONT_BOARD_PREFIX + raw_href
                    LIST_ARTICLE_URL = clean_url(LIST_ARTICLE_URL).replace("&currPage=1", "")
                    
                    title_text = a_tag.get_text().strip()
                    LIST_ARTICLE_TITLE = title_text[title_text.find("]")+1:].strip()

                    json_data_list.append({
                        "firm_id": firm_id,
                        "board_id": board_id,
                        "firm_nm": firm_info.get_firm_name(),
                        "report_date": re.sub(r"[-./]", "", str_date),
                        "source_url": '',
                        
                        "telegram_url": '',
                        "pdf_file_url": '',
                        "writer": writer,
                        "report_unique_key": LIST_ARTICLE_URL,
                        "article_title": LIST_ARTICLE_TITLE,
                        "save_at": datetime.now().isoformat()
                    })
                except Exception as e:
                    logger.error(f"Error parsing LS article row: {e}")
                    continue

        if not page_has_articles:
            break  # 빈 페이지면 다음 페이지 없음

    gc.collect()

    # ── DB 키 조회 → 신규 레코드만 필터 ──
    if json_data_list:
        try:
            db = get_db()
            existing_keys = db.fetch_existing_keys(firm_id=firm_id, days_limit=None)
            new_articles = [a for a in json_data_list if a.get("report_unique_key") and a["report_unique_key"] not in existing_keys]
            skipped = len(json_data_list) - len(new_articles)
            if skipped:
                logger.info(f"[LS] {skipped}건 기존 등록, 신규 {len(new_articles)}건")
            return new_articles
        except Exception as e:
            # DB가 존재하지 않는 독립 실행(GA) 환경 등에서는 오류로 중단되지 않고 필터링 없이 전체 데이터를 반환하도록 처리
            logger.warning(f"[LS] DB 조회 실패로 인해 중복 필터링 없이 전체 {len(json_data_list)}건을 반환합니다: {e}")
            return json_data_list

    return json_data_list

def clean_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    required_params = {
        'board_no': query_params.get('board_no', [''])[0],
        'board_seq': query_params.get('board_seq', [''])[0],
    }
    # 만약 currPage가 1이 아닌 다른 값이면 유지해야 할 수도 있으나, 
    # 요구사항에 따라 일단 중복 방지를 위해 필수 파라미터만 재조합
    new_query = urlencode(required_params)
    cleaned_url = urlunparse((
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        '',
        new_query,
        ''
    ))
    return cleaned_url


async def fetch(session: ClientSession, url: str, headers: dict) -> str:
    global USE_WARP_ONLY
    loop = asyncio.get_event_loop()

    # 1차 시도: 직접 접속. 짧게 2회 확인한 뒤 WARP 경로로 전환한다.
    if not USE_WARP_ONLY:
        for direct_attempt in range(1, LS_DIRECT_RETRIES + 1):
            try:
                def sync_get_direct():
                    response = requests.get(url, headers=headers, verify=False, timeout=30)
                    response.raise_for_status()
                    return response.text
                return await loop.run_in_executor(None, sync_get_direct)
            except Exception as e:
                if direct_attempt < LS_DIRECT_RETRIES:
                    logger.info(f"직접 접속 실패 {direct_attempt}/{LS_DIRECT_RETRIES}, 재시도: {url} ({e})")
                    await asyncio.sleep(direct_attempt)
        
        # 직접 접속 실패 시 플래그 전환
        logger.warning(f"상세 페이지 직접 접속 실패로 이후 WARP를 사용합니다: {url}")
        USE_WARP_ONLY = True

    # 2차 시도: WARP 프록시 (최대 5회, 재시도 중간 실패 로그 생략)
    for attempt in range(1, LS_WARP_RETRIES + 1):
        try:
            def sync_get_warp():
                response = requests.get(url, headers=headers, proxies=PROXIES, verify=False, timeout=50)
                response.raise_for_status()
                return response.text
            return await loop.run_in_executor(None, sync_get_warp)
        except Exception as e:
            if attempt < LS_WARP_RETRIES:
                await asyncio.sleep(3 * attempt)
            else:
                logger.error(f"LS WARP 상세 요청 최종 실패 (시도 {attempt}/{LS_WARP_RETRIES}): {url} ({e})")
                return None

async def process_article(session: ClientSession, article: dict, headers: dict, db=None):
    TARGET_URL = _article_unique_key(article)

    if ".pdf" in TARGET_URL:
        article["source_url"] = TARGET_URL
        article["telegram_url"] = TARGET_URL
        article["pdf_file_url"] = TARGET_URL
        # 건별 업데이트: PDF URL이 곧바로 확정되었으면 즉시 DB 반영
        if db and article.get('report_id'):
            await db.update_telegram_url(
                record_id=article['report_id'],
                telegram_url=TARGET_URL,
                article_title=article.get('article_title'),
                pdf_file_url=TARGET_URL
            )
            logger.debug(f"[건별업데이트] report_id={article['report_id']}: PDF 직접 URL → DB 반영")
        return

    html_content = await fetch(session, TARGET_URL, headers)
    if not html_content:
        return

    soup = BeautifulSoup(html_content, "html.parser")
    trs = soup.select("tr")

    for tr in trs:
        th = tr.select_one("th")
        td = tr.select_one("td")

        if th and td:
            th_text = th.get_text(strip=True)
            td_text = td.get_text(strip=True)

            if th_text == "제목":
                article["article_title"] = td_text
            elif th_text == "필명":
                # 상세 페이지에서 writer 확보 → reconstruct_msg_url_from_db()에서 emp_id 추론 가능
                if td_text:
                    article["writer"] = td_text
                    logger.debug(f"[LS][필명추출] writer={td_text}")
            elif th_text == "첨부파일":
                # ── URL 해결 전략 (3단계) ──
                # 1순위: 업로드 파일명 직접 파싱 → CDN URL (가장 빠름)
                #   upload filename: {emp_id}_{seq}_{date}.ext → K_{date}_{emp_id}_{seq}.pdf
                resolved_url = None

                upload_name = ""
                # 소스 A: 첨부파일 td a 텍스트
                for a_tag in tr.select("td a"):
                    txt = a_tag.get_text(strip=True)
                    if re.search(r'\d+_\d+_\d{8}\.\w+$', txt):
                        upload_name = txt
                        break
                # 소스 B: 본문 img alt → src basename
                if not upload_name:
                    img = soup.select_one(
                        "#contents > div.tbViewCon > div > html > body > p > img, "
                        "#contents > div.tbViewCon > div > p > img"
                    )
                    if img:
                        upload_name = img.get("alt") or ""
                        if not upload_name:
                            upload_name = os.path.basename(img.get("src", ""))

                if upload_name:
                    article["ATTACH_FILE_NAME"] = upload_name
                    direct_url = upload_filename_to_cdn_url(upload_name)
                    if direct_url:
                        try:
                            resp = await asyncio.to_thread(
                                lambda: requests.head(direct_url, headers=headers, proxies=PROXIES,
                                                      verify=False, timeout=10)
                            )
                            if resp.status_code == 200:
                                resolved_url = direct_url
                                logger.info(f"[LS][직접파싱] CDN URL: {direct_url}")
                        except Exception:
                            pass

                # 목록 단계에서 report_date 기준 CDN 추정을 먼저 수행한다.
                # 여기까지 왔다는 것은 그 추정이 실패한 경우이므로, 상세 페이지에 실제로
                # 표시된 첨부파일명으로만 fallback URL을 만든다.
                if not resolved_url:
                    resolved_url = await create_fallback_url(article, soup)

                # 최종 URL 할당
                quoted = urllib.parse.quote(resolved_url, safe=":/") if resolved_url else ""
                article["source_url"] = quoted
                article["telegram_url"] = quoted
                article["pdf_file_url"] = quoted

                # 건별 업데이트: URL이 확정되면 즉시 DB 반영 (report_id가 있는 경우)
                if db and quoted and article.get('report_id'):
                    await db.update_telegram_url(
                        record_id=article['report_id'],
                        telegram_url=quoted,
                        article_title=article.get('article_title'),
                        pdf_file_url=quoted
                    )
                    logger.debug(f"[건별업데이트] report_id={article['report_id']}: {article.get('article_title')}")

async def LS_detail(articles, firm_info=None, db=None):
    if isinstance(articles, dict):
        articles = [articles]
    elif isinstance(articles, str):
        logger.error("Error: Invalid article format. Expected a dictionary or a list.")
        return []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": LS_HOME_URL,
        "Connection": "keep-alive"
    }

    semaphore = asyncio.Semaphore(1)

    async def sem_process_article(session, article):
        async with semaphore:
            await process_article(session, article, headers, db=db)
            import random
            await asyncio.sleep(random.uniform(LS_DETAIL_DELAY_MIN, LS_DETAIL_DELAY_MAX))

    # 목록의 report_date와 writer 이력으로 같은 날짜의 CDN URL만 먼저 찾는다.
    # 날짜를 넓혀 존재하는 첫 PDF를 고르면 다른 게시물 PDF가 연결될 수 있으므로,
    # 이 단계에서는 절대 날짜 이동 탐색을 허용하지 않는다. 못 찾은 건만 상세 페이지를
    # 한 번 조회해 실제 첨부파일명을 읽는다.
    detail_targets = []
    for article in articles:
        if str(article.get("telegram_url", "")).lower().endswith(".pdf"):
            continue
        inferred_url = await reconstruct_msg_url_from_db(
            article, headers, exact_report_date=True
        )
        if not inferred_url:
            detail_targets.append(article)
            continue

        article["source_url"] = inferred_url
        article["telegram_url"] = inferred_url
        article["pdf_file_url"] = inferred_url
        logger.info(
            "[LS][목록추론] report_date={} PDF 복구: {}",
            _article_report_date(article), inferred_url,
        )
        if db and article.get("report_id"):
            await db.update_telegram_url(
                record_id=article["report_id"],
                telegram_url=inferred_url,
                article_title=article.get("article_title"),
                pdf_file_url=inferred_url,
            )

    async with aiohttp.ClientSession() as session:
        tasks = [sem_process_article(session, article) for article in detail_targets]
        await asyncio.gather(*tasks)

    return articles

async def LS_detailAll(articles=None, firm_info=None):
    db = get_db()
    if articles is None:
        articles = await db.fetch_ls_detail_targets()
    
    if not articles:
        logger.info("Detail 처리가 필요한 LS 레포트가 없습니다.")
        return []

    target_articles = [a for a in articles if not str(a.get('telegram_url', '')).lower().endswith('.pdf')]
    if not target_articles:
        return articles

    # ── WARP warm-up (상세 페이지 fetch()에서도 첫 연결 SSL EOF 방지) ──
    warm_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
    }
    for _ in range(3):
        try:
            await asyncio.to_thread(
                    lambda: requests.get(LS_HOME_URL, headers=warm_headers,
                                     proxies=PROXIES, verify=False, timeout=20)
            )
            logger.debug("LS_detail WARP warm-up 완료")
            break
        except Exception:
            await asyncio.sleep(2)
    else:
        logger.warning("LS_detail WARP warm-up 실패 (계속 진행)")

    logger.info(f"총 {len(target_articles)}개의 LS 레포트에 대해 상세 정보를 추출합니다.")
    # process_article 내에서 건별 DB 업데이트가 이루어지므로 LS_detail에 db 전달
    updated_articles = await LS_detail(target_articles, firm_info, db=db)
    
    # 안전망: 건별 업데이트가 처리하지 못한 건들 (report_id 없는 경우 등) 후처리
    for article in updated_articles:
        if article.get('telegram_url') and article.get('report_id') \
           and str(article.get('telegram_url')).lower().endswith('.pdf'):
            await db.update_telegram_url(
                record_id=article['report_id'], 
                telegram_url=article['telegram_url'],
                article_title=article.get('article_title'),
                pdf_file_url=article.get('pdf_file_url') or article.get('telegram_url')
            )
            logger.debug(f"DB 업데이트 완료: {article.get('article_title')}")
            
    return updated_articles

def upload_filename_to_cdn_url(upload_url_or_name: str) -> str | None:
    """
    upload URL(or filename)에서 emp_id, seq, date를 추출하여 CDN URL로 변환.

    upload filename 패턴: {emp_id}_{seq}_{date}.ext
      예) 31565_327_20260504.PNG → K_20260504_31565_327.pdf

    run/fix_ls_db.py 등에서도 재사용 가능.
    """
    basename = os.path.basename(upload_url_or_name)
    m = re.match(r'^(\d+)_(\d+)_(\d{8})\.', basename)
    if m:
        emp_id, seq, date_str = m.group(1), m.group(2), m.group(3)
        return f"{LS_MSG_EUM_PREFIX}/K_{date_str}_{emp_id}_{seq}.pdf"
    return None


async def get_valid_url(new_filename, date_part, article, headers):
    base_url = f"{LS_MSG_EUM_PREFIX}/K_{{filename}}"
    try:
        date_obj = datetime.strptime(date_part, "%Y%m%d")
    except ValueError:
        return await create_fallback_url(article, None)

    # 탐색 범위를 전후 LS_SEARCH_DAYS일로 확대
    date_range = [date_obj + timedelta(days=i) for i in range(-LS_SEARCH_DAYS, LS_SEARCH_DAYS + 1)]

    # 병렬 HEAD probing (최대 5개 동시 요청으로 rate limit 방지)
    sem = asyncio.Semaphore(5)
    async def check_url(test_url: str) -> str | None:
        async with sem:
            try:
                response = await asyncio.to_thread(
                    lambda: requests.get(test_url, headers=headers, verify=False,
                                         proxies=PROXIES, timeout=15)
                )
                if response.status_code == 200:
                    return test_url
            except Exception:
                pass
            return None

    tasks = []
    for test_date in date_range:
        test_date_str = test_date.strftime("%Y%m%d")
        test_filename = new_filename.replace(date_part, test_date_str)
        test_url = base_url.format(filename=test_filename)
        tasks.append(check_url(test_url))

    results = await asyncio.gather(*tasks)
    for url in results:
        if url:
            logger.debug(f"Valid URL found: {url}")
            return url

    return await create_fallback_url(article, None)


async def create_fallback_url(article, soup=None):
    URL_PARAM = _article_report_date(article)
    if not URL_PARAM:
        return ""
    URL_PARAM_0 = "B" + URL_PARAM[:6]
    
    attach_file_name = article.get("ATTACH_FILE_NAME", "")
    if not attach_file_name and soup:
        attach_tag = soup.select_one(".attach > a")
        if attach_tag:
            attach_file_name = attach_tag.get_text(strip=True)
            
    if attach_file_name:
        safe_name = urllib.parse.quote(attach_file_name)
        # EtwBoardData 앞에 EtwFrontBoard가 붙는 경우가 많음
        fallback_url = f"{LS_UPLOAD_PREFIX}/{URL_PARAM_0}/{safe_name}"
        logger.debug(f"Fallback URL created: {fallback_url}")
        return fallback_url
    
    return ""

async def reconstruct_msg_url_from_db(article, headers, exact_report_date=False):
    """DB에 있는 성공한 msg URL 데이터를 기반으로 writer ID와 seq를 추론해서 msg URL 재구성.

    동일 작성자의 기존 성공 URL에서 writer_id(emp_id)와 seq 패턴을 추출하여
    K_{date}_{emp_id}_{seq}.pdf 형태의 올바른 CDN URL을 생성/probing한다.

    개선 사항 (2026-05):
    - seq 탐색 범위: 과거 seq까지 포함하도록 개선 (max_seq 기준 ±30)
    - 날짜 탐색 범위: ±LS_SEARCH_DAYS (기본 ±10일)
    - 선형 보간 기반 seq 추정 (fix_ls_db_by_empid.py 방식 참고)
    """
    writer_name = article.get("writer", "")
    report_date = _article_report_date(article)
    if not writer_name or not report_date:
        return None

    try:
        from models.db_factory import get_db
        db = get_db()

        # 1. 해당 writer의 성공 URL 1건 찾기 (emp_id 확인용)
        rows = db._fetchall("""
            SELECT telegram_url
            FROM tbl_sec_reports
            WHERE firm_id = 0
              AND writer = %s
              AND telegram_url LIKE 'https://msg.ls-sec.co.kr/eum/K_%%'
            ORDER BY save_at DESC
            LIMIT 1
        """, (writer_name,))

        if not rows:
            logger.debug(f"[LS][DB추론] writer={writer_name}: 성공 msg URL 없음")
            return None

        sample_url = rows[0]["telegram_url"]
        m = re.search(r'K_(\d{8})_(.+)\.pdf', sample_url)
        if not m:
            return None

        suffix = m.group(2)
        parts = suffix.rsplit('_', 1)
        if len(parts) != 2:
            return None

        writer_id = parts[0]

        # 2. 해당 writer_id의 전체 seq 이력 조회
        all_urls = db._fetchall("""
            SELECT telegram_url, report_date
            FROM tbl_sec_reports
            WHERE firm_id = 0
              AND telegram_url LIKE 'https://msg.ls-sec.co.kr/eum/K_%%'
              AND telegram_url LIKE '%%' || %s || '_%%'
            ORDER BY telegram_url DESC
            LIMIT 200
        """, (writer_id,))

        seq_history = []  # [(report_date, seq)]
        max_seq = 0
        for row in all_urls:
            m2 = re.search(rf'K_(\d{{8}})_{writer_id}_(\d+).pdf', row['telegram_url'])
            if m2:
                s = int(m2.group(2))
                seq_history.append((str(row['report_date']).replace("-", ""), s))
                if s > max_seq:
                    max_seq = s

        if not seq_history:
            logger.debug(f"[LS][DB추론] writer={writer_name}: seq 추출 실패")
            return None

        # 3. 선형 보간으로 예상 seq 계산 (fix_ls_db_by_empid.py 방식)
        report_date_int = report_date
        sorted_h = sorted(seq_history, key=lambda x: x[0])
        before = [(dt, sq) for dt, sq in sorted_h if dt <= report_date_int]
        after  = [(dt, sq) for dt, sq in sorted_h if dt > report_date_int]

        def date_diff_days(d1, d2):
            try:
                return abs((datetime.strptime(d1, "%Y%m%d") - datetime.strptime(d2, "%Y%m%d")).days)
            except Exception:
                return 999

        est_seq = max_seq
        if before and after:
            dt1, sq1 = before[-1]
            dt2, sq2 = after[0]
            d1 = date_diff_days(report_date_int, dt1)
            d2 = date_diff_days(report_date_int, dt2)
            total = max(d1 + d2, 1)
            est_seq = int(sq1 + (sq2 - sq1) * d1 / total)
        elif before:
            # 최근 두 점으로 속도 추정
            dt1, sq1 = before[-1]
            d = date_diff_days(report_date_int, dt1)
            if len(before) >= 2:
                dt0, sq0 = before[-2]
                daily = abs(sq1 - sq0) / max(date_diff_days(dt1, dt0), 1)
            else:
                daily = 3
            est_seq = int(sq1 + daily * d)

        est_seq = max(1, est_seq)

        # 4. 후보 URL 목록 생성 (넓은 범위 탐색)
        try:
            base_date = datetime.strptime(report_date, "%Y%m%d")
        except ValueError:
            return None

        candidates = []
        # 목록 신규 수집에서는 목록의 report_date와 PDF 파일 날짜가 일치해야 한다.
        # 그 외 보수/백필 경로만 기존의 날짜 이동 탐색을 사용한다.
        day_offsets = [0] if exact_report_date else (
            [0] + list(range(1, LS_SEARCH_DAYS + 1))
            + list(range(-1, -LS_SEARCH_DAYS - 1, -1))
        )
        for day_offset in day_offsets:
            test_date = (base_date + timedelta(days=day_offset)).strftime("%Y%m%d")
            # seq: 예상값 기준 ±50, 최소 1
            for seq_offset in range(-50, 51):
                test_seq = max(1, est_seq + seq_offset)
                test_url = f"{LS_MSG_EUM_PREFIX}/K_{test_date}_{writer_id}_{test_seq}.pdf"
                candidates.append(test_url)

        # 중복 제거 (날짜*seq 조합이 중복될 수 있음)
        candidates = list(dict.fromkeys(candidates))

        # 5. 병렬 탐색 (20개씩 동시 요청)
        found_url = None
        concurrency = 20
        sem = asyncio.Semaphore(concurrency)

        async def check_url(url: str) -> str | None:
            async with sem:
                try:
                    resp = await asyncio.to_thread(
                        lambda: requests.get(url, headers=headers, proxies=PROXIES,
                                            verify=False, timeout=10)
                    )
                    if resp.status_code == 200:
                        return url
                except Exception:
                    pass
                return None

        for i in range(0, len(candidates), concurrency):
            batch = candidates[i:i + concurrency]
            results = await asyncio.gather(*[check_url(u) for u in batch])
            for r in results:
                if r:
                    found_url = r
                    break
            if found_url:
                break

        if found_url:
            logger.info(f"[LS][DB추론] writer={writer_name}, url={found_url}")
            return found_url

        logger.debug(f"[LS][DB추론] writer={writer_name}, est_seq={est_seq}, max_seq={max_seq}, not found")
        return None
    except Exception as e:
        logger.error(f"[LS][DB추론] {writer_name}: {e}")
        return None


async def LS_enrich(db, records, firm_info, is_idle_time):
    """LS enrichment: msg URL 복구 + upload fallback + 유휴시간 backlog"""
    update_records = await LS_detail(articles=records, firm_info=firm_info)
    tasks = [db.update_telegram_url(r['report_id'], r['telegram_url'], r.get('article_title'),
              pdf_file_url=r.get('pdf_file_url') or r['telegram_url']) for r in update_records if r.get('telegram_url')]
    if tasks:
        await asyncio.gather(*tasks)

    upload_fallback_prefix = f"{LS_PUBLIC_ORIGIN}/upload/"

    fallback_records = db._fetchall('''
        SELECT report_id, article_title, writer, telegram_url,
               report_date, report_unique_key
        FROM v_sec_reports_canonical
        WHERE firm_id = 0 AND telegram_url LIKE %s
          AND save_at >= NOW() - INTERVAL '1 day'
          AND report_unique_key IS NOT NULL AND report_unique_key != ''
        ORDER BY save_at DESC LIMIT 50
    ''', (f"{upload_fallback_prefix}%",))
    if fallback_records:
        logger.info(f"[LS] upload/ fallback {len(fallback_records)}건 재시도...")
        refixed = await LS_detail(articles=fallback_records, firm_info=firm_info)
        tasks = [db.update_telegram_url(r['report_id'], r['telegram_url'], r.get('article_title'),
                  pdf_file_url=r.get('pdf_file_url') or r['telegram_url']) for r in refixed
                 if r.get('telegram_url', '').startswith(LS_MSG_PREFIX)]
        if tasks:
            await asyncio.gather(*tasks)
            logger.success(f"[LS] upload/ fallback {len(tasks)}건 msg URL 재복구 완료")

    if is_idle_time:
        backlog = db._fetchall('''
            SELECT report_id, article_title, writer, telegram_url,
                   report_date, report_unique_key
            FROM v_sec_reports_canonical
            WHERE firm_id = 0 AND (telegram_url IS NULL OR telegram_url = ''
               OR telegram_url NOT LIKE %s)
              AND report_unique_key IS NOT NULL AND report_unique_key != ''
            ORDER BY save_at DESC LIMIT 200
        ''', (f"{LS_MSG_PREFIX}%",))
        if backlog:
            logger.info(f"[LS][유휴] 전체 backlog {len(backlog)}건 재처리...")
            fixed = await LS_detail(articles=backlog, firm_info=firm_info)
            tasks = [db.update_telegram_url(r['report_id'], r['telegram_url'], r.get('article_title'),
                      pdf_file_url=r.get('pdf_file_url') or r['telegram_url']) for r in fixed
                     if r.get('telegram_url', '').startswith(LS_MSG_PREFIX)]
            if tasks:
                await asyncio.gather(*tasks)
                logger.success(f"[LS][유휴] {len(tasks)}건 msg URL 복구 완료")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'fix':
        logger.info("상세 정보 누락 건 복구 모드(fix) 실행...")
        asyncio.run(LS_detailAll())
    else:
        page = 1
        all_articles = []
        while True:
            logger.debug(f"LS Scraper: Processing Page {page}")
            articles = LS_checkNewArticle(page, is_imported=False, skip_boards=skip_boards)
            if not any(articles):
                break
            all_articles.extend(articles)
            page += 1

        if not all_articles:
            logger.info("No LS articles found.")
        else:
            db = get_db()
            inserted_count, updated_count = db.insert_json_data_list(all_articles)
            logger.success(f"LS: Inserted {inserted_count}, Updated {updated_count} articles.")
