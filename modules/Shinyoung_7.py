from loguru import logger
# -*- coding:utf-8 -*- 
import gc
import requests
import json
import re
from datetime import datetime

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo
from models.WebScraper import SyncWebScraper
from models.ConfigManager import config


def Shinyoung_checkNewArticle():
    sec_firm_order      = 7
    article_board_order = 0
    json_data_list = []

    requests.packages.urllib3.disable_warnings()

    urls = config.get_urls("Shinyoung_7")
    if not urls:
        logger.warning("No URLs found for Shinyoung_7")
        return []
    TARGET_URL = urls[0]
    
    firm_info = FirmInfo(
        sec_firm_order=sec_firm_order,
        article_board_order=article_board_order
    )

    # POST 요청을 보낼 데이터
    payload = {
        "KEYWORD": "",
        "rows": "50",
        "page": "1"
    }

    scraper = SyncWebScraper(TARGET_URL, firm_info)
    
    # HTML parse
    jres = scraper.PostJson(params=payload)

    # logger.debug(jres['rows'])
    soupList = jres['rows']
    
    # JSON To List
    for list in soupList:
        # logger.debug('list***************** \n',list)
        mkt_tp              = "KR"    
        reg_dt              = re.sub(r"[-./]", "", list['APPDATE'])
        writer              = list['EMPNM']
        # logger.debug('NT_NO=',list['NT_NO'], 'CMS_CD=',cmsCd[article_board_order])
        LIST_ARTICLE_URL = Shinyoung_detail(SEQ=list['SEQ'], BBSNO=list['BBSNO'])
        LIST_ARTICLE_TITLE = list['TITLE']
        if "해외주식" in LIST_ARTICLE_TITLE :
            mkt_tp = "GLOBAL"
        download_url = LIST_ARTICLE_URL
        json_data_list.append({
            "sec_firm_order":sec_firm_order,
            "article_board_order":article_board_order,
            "firm_nm":firm_info.get_firm_name(),
            "article_title":LIST_ARTICLE_TITLE,
            "reg_dt":reg_dt,
            "writer":writer,
            "telegram_url": download_url,
            "mkt_tp": mkt_tp,
            "key":LIST_ARTICLE_URL,
                    "report_unique_key": LIST_ARTICLE_URL,
            "save_time": datetime.now().isoformat()
        })
            

    # 메모리 정리
    del soupList
    gc.collect()
    return json_data_list

def Shinyoung_detail(SEQ, BBSNO):
    # logger.debug('******************Shinyoung_detail***************')
    # ntNo = NT_NO
    # cmsCd = CMS_CD
    # POST 요청에 사용할 URL
    url = "https://www.shinyoung.com/Common/authTr/devPass"

    # 추가할 request header
    headers = {
        "Accept": "text/plain, */*; q=0.01",
        "Connection": "keep-alive",
        "Content-Length": "0",
        "Host": "www.shinyoung.com",
        "Origin": "https://www.shinyoung.com",
        "Referer": "https://www.shinyoung.com/?page=10078&head=0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }
    session = requests.Session()
    # POST 요청 보내기
    response = session.post(url, headers=headers)

    # 응답의 내용 확인
    if response.status_code != 200:
        logger.warning("요청에 실패하였습니다. 상태 코드:", response.status_code)
    
    # 서버에서 반환한 응답 확인 및 새로운 쿠키가 있다면 세션에 추가
    if 'Set-Cookie' in response.headers:
        # 새로운 쿠키를 세션에 추가
        new_cookie = response.headers['Set-Cookie']
        session.cookies.update({'new_cookie_name': new_cookie})

    #### https://www.shinyoung.com/Common/checkAuth

    url = "https://www.shinyoung.com/Common/checkAuth"

    # 추가할 request header
    headers = {
    "Accept": "text/plain, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Content-Length": "0",
    "Host": "www.shinyoung.com",
    "Origin": "https://www.shinyoung.com",
    "Referer": "https://www.shinyoung.com/?page=10078&head=0",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\""
    }

    # POST 요청 보내기
    response = session.post(url, headers=headers)

    # 응답의 내용 확인
    if response.status_code != 200:
        logger.warning("요청에 실패하였습니다. 상태 코드:", response.status_code)
    # POST 요청에 사용할 URL
    url = "https://www.shinyoung.com/Common/authTr/downloadFilePath"

    # POST 요청에 포함될 데이터
    data = {
        'SEQ': SEQ,
        'BBSNO': BBSNO
    }
    
    headers = {
        "Accept": "text/plain, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Content-Length": "18",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.shinyoung.com",
        "Origin": "https://www.shinyoung.com",
        "Referer": "https://www.shinyoung.com/?page=10078&head=0",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    # POST 요청 보내기
    response = session.post(url, data=data, headers=headers)

    # 응답의 내용 확인
    if response.status_code != 200:
        logger.warning("요청에 실패하였습니다. 상태 코드:", response.status_code)

    jres = json.loads(response.text)
    
    base_url = 'https://www.shinyoung.com/files/'

    url = base_url + jres['FILEINFO']['FILEPATH']

    # logger.debug('*******************완성된 URL',url)
    return url

if __name__ == "__main__":
    Shinyoung_checkNewArticle()