import asyncio
import time
import aiohttp
import requests
import json
import re
import chardet  # 인코딩 감지용 (필요 시 설치: pip install chardet)
from bs4 import BeautifulSoup
from loguru import logger

class SyncWebScraper:
    def __init__(self, target_url, firm_info=None, proxies=None, headers=None):
        """
        :param target_url: 요청할 URL
        :param firm_info: FirmInfo 인스턴스 (선택. 없으면 기본 헤더 사용)
        :param proxies: 선택적 프록시 설정
        :param headers: 명시적 헤더 딕셔너리 (권장). 없으면 firm_info 기반 추론.
        """
        self.target_url = target_url
        self.firm_info = firm_info
        self.headers = headers or self._set_headers()
        self.proxies = proxies

    def _set_headers(self):
        """
        [DEPRECATED] sec_firm_order 값에 따라 헤더를 추론.
        신규 모듈은 SyncWebScraper(url, headers={...}) 로 명시적 전달 권장.
        이 메서드는 하위 호환용. 기존 if/elif 분기는 점진적으로 제거 예정.
        """
        if self.firm_info is None or self.firm_info.sec_firm_order == 0:
            # sec_firm_order가 0번에 맞는 헤더 설정 (LS증권)
            return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1"
            }
        elif self.firm_info.sec_firm_order == 1:
            # 회사 1번에 맞는 헤더 설정
            return {
                "User-Agent": "Mozilla/5.0"
            }
        elif self.firm_info.sec_firm_order == 2:
            # 회사 2번에 맞는 헤더 설정 (예시)
            return {
                "User-Agent": "Mozilla/5.0 (Linux; Android 10; SM-G970F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36"
            }
        elif self.firm_info.sec_firm_order == 4:
            # 회사 4번에 맞는 헤더 설정 (예시)
            return {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Linux; Android 13; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36",
                "X-Requested-With": "XMLHttpRequest"
            }
        elif self.firm_info.sec_firm_order == 7:
            # sec_firm_order가 7번일 때 Referer 추가
            return {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Referer": "https://www.shinyoung.com/?page=10078&head=0"
            }
        else:
            # 기본 Header
            return {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            }

    def _clean_response_text(self, text):
            """응답 텍스트에서 제어 문자 및 비표준 문자를 제거하고 정규화"""
            try:
                # 제어 문자(ASCII 0x00-0x1F, 0x7F) 제거
                cleaned_text = re.sub(r'[\x00-\x1F\x7F]+', '', text)
                # 연속된 공백을 단일 공백으로 변환
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                return cleaned_text
            except Exception as e:
                logger.error(f"[텍스트 정규화 오류] {e}")
                return text

    def _detect_encoding(self, response):
        """응답 데이터의 인코딩 감지"""
        try:
            result = chardet.detect(response.content)
            encoding = result['encoding'] or 'utf-8'
            logger.debug(f"[감지된 인코딩] {encoding}")
            return encoding
        except Exception as e:
            logger.error(f"[인코딩 감지 오류] {e}")
            return 'utf-8'
         
    def _get_css_selector(self):
        """
        sec_firm_order 값에 따라 CSS 선택자를 반환하는 메서드
        :return: 적절한 CSS 선택자 문자열
        """
        if self.firm_info is None or self.firm_info.sec_firm_order == 0:
            # 기본 CSS 선택자 (sec_firm_order가 0일 때)
            return '#contents > table > tbody > tr'
        elif self.firm_info.sec_firm_order == 1:
            # 회사 1번에 대한 CSS 선택자
            return '#mainContent > div > table > tbody > tr'
        elif self.firm_info.sec_firm_order == 2:
            # 회사 2번에 대한 CSS 선택자 (예시)
            return '#customContent > section > div > table > tr'
        # ... 필요한 경우 다른 회사별 CSS 선택자 추가 가능
        elif self.firm_info.sec_firm_order == 16:
            return '#sub-container > div.table-wrap > table > tbody > tr'
        else:
            return None

    def _parse_list_item(self, soup_list):
        """
        sec_firm_order 값에 따라 각기 다른 방식으로 리스트 데이터를 파싱하는 메서드
        :param soup_list: BeautifulSoup으로 파싱된 리스트 데이터
        :return: 파싱된 데이터를 포함하는 리스트
        """
        result = []

        if self.firm_info.sec_firm_order == 0:
            # sec_firm_order가 0일 때 처리 (제공된 코드 기반)
            for item in soup_list:
                date = item.select('td')[3].get_text()
                list_links = item.select('a')

                LIST_ARTICLE_URL = 'https://www.ls-sec.co.kr/EtwFrontBoard/' + list_links[0]['href'].replace("amp;", "")
                LIST_ARTICLE_TITLE = list_links[0].get_text()
                LIST_ARTICLE_TITLE = LIST_ARTICLE_TITLE[LIST_ARTICLE_TITLE.find("]") + 1:].strip()
                POST_DATE = date.strip()

                result.append({
                    'LIST_ARTICLE_URL': LIST_ARTICLE_URL,
                    'LIST_ARTICLE_TITLE': LIST_ARTICLE_TITLE,
                    'POST_DATE': POST_DATE
                })

        elif self.firm_info.sec_firm_order == 1:
            pass

        elif self.firm_info.sec_firm_order == 2:
            # 회사 2번에 대한 파싱 로직 (추가적인 파싱 로직 필요)
            for item in soup_list:
                # 예시: 리스트의 제목과 링크 가져오기
                list_links = item.select('a')
                date = item.select('td')[1].get_text()

                LIST_ARTICLE_URL = 'https://www.company2.com' + list_links[0]['href']
                LIST_ARTICLE_TITLE = list_links[0].get_text().strip()
                POST_DATE = date.strip()

                result.append({
                    'LIST_ARTICLE_URL': LIST_ARTICLE_URL,
                    'LIST_ARTICLE_TITLE': LIST_ARTICLE_TITLE,
                    'POST_DATE': POST_DATE
                })
        else: return
        return result
    
    def Get(self, params=None, retries=5, silent_retries=5):
        """
        GET 요청을 통해 데이터를 가져오는 메서드
        :param params: 요청 시 함께 보낼 파라미터 (기본값은 None)
        :param retries: 최대 시도 횟수 (기본값 5)
        :param silent_retries: 에러 로그를 남기지 않을 시도 횟수 (기본값 5)
        :return: 파싱된 soupList 또는 None
        """
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(self.target_url, params=params, headers=self.headers, verify=False, proxies=self.proxies, timeout=20)
                response.raise_for_status()  # 요청 실패 시 예외 발생
                
                # HTML 파싱
                soup = BeautifulSoup(response.content, "html.parser")
                logger.debug(f"WebScraper Get successful: {self.target_url} (Attempt {attempt})")
                return soup
                
            except requests.exceptions.RequestException as e:
                if attempt < retries:
                    time.sleep(1 * attempt) # 지수 백오프 비스무리하게 대기
                else:
                    if attempt <= silent_retries:
                        logger.debug(f"GET 요청 최종 실패 (시도 {attempt}/{retries}): {e} (URL: {self.target_url})")
                    else:
                        logger.warning(f"GET 요청 최종 실패 (시도 {attempt}/{retries}): {e} (URL: {self.target_url})")
                    return None

    def GetJson(self, params=None, retries=5, silent_retries=5):
        """HTTP GET 요청을 보내고 JSON 응답을 반환"""
        for attempt in range(1, retries + 1):
            try:
                # HTTP GET 요청
                response = requests.get(self.target_url, headers=self.headers, params=params, timeout=10, verify=False, proxies=self.proxies)
                logger.debug(f"WebScraper GetJson: {self.target_url} (Attempt {attempt})")

                # HTTP 상태 코드 확인
                response.raise_for_status()

                # 인코딩 감지 및 설정
                try:
                    result = chardet.detect(response.content)
                    encoding = result['encoding'] or 'utf-8'
                except Exception:
                    encoding = 'utf-8'
                response.encoding = encoding
                raw_text = response.text

                # 응답 텍스트 정규화
                try:
                    # 제어 문자(ASCII 0x00-0x1F, 0x7F) 및 BOM(\uFEFF) 제거
                    cleaned_text = re.sub(r'[\x00-\x1F\x7F-\x9F\uFEFF]+', '', raw_text)
                    # 연속된 공백을 단일 공백으로 변환
                    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                except Exception:
                    cleaned_text = raw_text

                # JSON 파싱 시도
                try:
                    json_data = json.loads(cleaned_text)
                    return json_data
                except json.JSONDecodeError as e:
                    if attempt == retries:
                        logger.warning(f"[JSON 파싱 최종 실패] {e}")
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < retries:
                    time.sleep(1 * attempt)
                else:
                    logger.warning(f"[HTTP 요청 최종 실패] 시도 {attempt}/{retries}: {e} (URL: {self.target_url})")
                    return None
            except Exception as e:
                if attempt == retries:
                    logger.warning(f"[알 수 없는 최종 오류] {e}")
                return None

    def Post(self, data=None):
        """
        POST 요청을 통해 데이터를 가져오는 메서드
        """
        try:
            response = requests.post(self.target_url, data=data, headers=self.headers, verify=False)
            response.raise_for_status()  # 요청 실패 시 예외 발생
        except requests.exceptions.RequestException as e:
            logger.error(f"POST 요청 에러: {e}")
            return None

        soup = BeautifulSoup(response.content, "html.parser")
        logger.debug(f"WebScraper Post successful: {self.target_url}")
        return soup

    def PostJson(self, params=None, json=None):
        response = requests.post(self.target_url, headers=self.headers, params=params, json=json)
        logger.debug(f"WebScraper PostJson: {self.target_url}")
        return response.json()


class AsyncWebScraper:
    def __init__(self, target_url, headers=None):
        self.target_url = target_url
        self.headers = headers or {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}

    async def Get(self, session=None, params=None, retries=5, silent_retries=5):
        """비동기 GET 요청을 통해 데이터를 가져오는 메서드"""
        close_session = False
        timeout_config = aiohttp.ClientTimeout(total=15)
        if session is None:
            session = aiohttp.ClientSession(timeout=timeout_config)
            close_session = True

        try:
            for attempt in range(1, retries + 1):
                try:
                    async with session.get(self.target_url, headers=self.headers, params=params, timeout=timeout_config) as response:
                        response.raise_for_status()
                        html = await response.text()
                        return BeautifulSoup(html, "html.parser")
                except Exception as e:
                    if attempt < retries:
                        await asyncio.sleep(1 * attempt)
                    else:
                        if attempt <= silent_retries:
                            logger.debug(f"Async GET 요청 최종 실패 (시도 {attempt}/{retries}): {e} (URL: {self.target_url})")
                        else:
                            logger.warning(f"Async GET 요청 최종 실패 (시도 {attempt}/{retries}): {e} (URL: {self.target_url})")
                        return None
        finally:
            if close_session:
                await session.close()

    async def Post(self, session=None, data=None):
        """비동기 POST 요청을 통해 데이터를 가져오는 메서드"""
        close_session = False
        timeout_config = aiohttp.ClientTimeout(total=15)
        if session is None:
            session = aiohttp.ClientSession(timeout=timeout_config)
            close_session = True

        try:
            async with session.post(self.target_url, headers=self.headers, data=data, timeout=timeout_config) as response:
                response.raise_for_status()
                html = await response.text()
                return BeautifulSoup(html, "html.parser")
        finally:
            if close_session:
                await session.close()

    async def GetJson(self, session=None, params=None, retries=5, silent_retries=5):
        """비동기 GET 요청을 통해 JSON 데이터를 가져오는 메서드"""
        close_session = False
        timeout_config = aiohttp.ClientTimeout(total=15)
        if session is None:
            session = aiohttp.ClientSession(timeout=timeout_config)
            close_session = True

        try:
            for attempt in range(1, retries + 1):
                try:
                    async with session.get(self.target_url, headers=self.headers, params=params, timeout=timeout_config) as response:
                        response.raise_for_status()
                        logger.debug(f"AsyncWebScraper GetJson successful: {self.target_url} (Attempt {attempt})")
                        return await response.json()
                except Exception as e:
                    if attempt < retries:
                        await asyncio.sleep(1 * attempt)
                    else:
                        if attempt <= silent_retries:
                            logger.debug(f"Async GetJson 요청 최종 실패 (시도 {attempt}/{retries}): {e} (URL: {self.target_url})")
                        else:
                            logger.warning(f"Async GetJson 요청 최종 실패 (시도 {attempt}/{retries}): {e} (URL: {self.target_url})")
                        return None
        finally:
            if close_session:
                await session.close()

    async def PostJson(self, headers=None, session=None, params=None, json_data=None):
        """
        비동기 POST 요청을 통해 JSON 데이터를 가져오는 메서드.
        """
        close_session = False
        timeout_config = aiohttp.ClientTimeout(total=15)
        if session is None:
            conn = aiohttp.TCPConnector(ssl=False, force_close=True)
            session = aiohttp.ClientSession(connector=conn, timeout=timeout_config)
            close_session = True

        use_headers = headers
        if use_headers is None:
            use_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                "Referrer": self.target_url
            }

        try:
            async with session.post(self.target_url, headers=use_headers, data=params, json=json_data, timeout=timeout_config) as response:
                response.raise_for_status()
                logger.debug(f"AsyncWebScraper PostJson successful: {self.target_url}")
                return await response.json(content_type=None)
        finally:
            if close_session:
                await session.close()
