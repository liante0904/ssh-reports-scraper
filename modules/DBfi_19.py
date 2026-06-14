import asyncio
import aiohttp
import json
import ssl
import re
from datetime import datetime
import os
import sys
import urllib.parse as urlparse
from loguru import logger
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo
from models.ConfigManager import config
from models.db_factory import get_db

# 시크릿 설정 로드
dbfi_cfg = config.get_urls("DBfi_19")
BASE_URL = dbfi_cfg["base_url"]
VIEWER_BASE = dbfi_cfg["viewer_base_url"]
URL_PATHS = dbfi_cfg["url_paths"]

HEADERS_TEMPLATE = {
    "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148",
    "Content-Type": "application/x-www-form-urlencoded;charset=utf-8"
}

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
ssl_context.set_ciphers("DEFAULT")

# WARP 프록시 설정 (LS와 동일)
SOCKS_PROXY = os.getenv("SOCKS_PROXY_URL", "socks5h://localhost:9091")
PROXIES = {
    'http': SOCKS_PROXY,
    'https': SOCKS_PROXY
}
DBF_DIRECT_RETRIES = int(os.getenv("DBF_DIRECT_RETRIES", "2"))
DBF_WARP_RETRIES = int(os.getenv("DBF_WARP_RETRIES", "3"))


async def extract_dbfi_pdf_url(session, encoded_url):
    """
    DB증권 gate 토큰에서 실제 PDF 다운로드 URL을 추출합니다.
    """
    if not encoded_url:
        return None

    token = urlparse.unquote(encoded_url)
    gate_q = urlparse.quote(token, safe="")
    gate_url = f"{VIEWER_BASE}/pv/gate?q={gate_q}"

    pv_headers = {
        "User-Agent": HEADERS_TEMPLATE["User-Agent"],
        "Content-Type": HEADERS_TEMPLATE["Content-Type"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": gate_url,
    }

    try:
        async with session.post(f"{VIEWER_BASE}/pv/auth", headers=pv_headers) as auth_response:
            await auth_response.text()

        viewer_payload = {
            "q": token,
            "c": "",
            "target": "",
            "docId": "",
        }
        async with session.post(
            f"{VIEWER_BASE}/pv/viewer",
            headers=pv_headers,
            data=viewer_payload,
        ) as viewer_response:
            if viewer_response.status != 200:
                logger.warning(f"DBfi: viewer request failed ({viewer_response.status})")
                return None

            viewer_html = await viewer_response.text()
            match = re.search(r'<div[^>]*id="([^"]+)"[^>]*class="item"', viewer_html)
            if not match:
                match = re.search(r'<div[^>]*class="item"[^>]*id="([^"]+)"', viewer_html)

            if not match:
                logger.warning("DBfi: Could not find StreamDocs document id in viewer HTML")
                return None

            doc_id = match.group(1)
            title_match = re.search(
                rf'<div[^>]*id="{re.escape(doc_id)}"[^>]*class="item"[^>]*>\s*<span>(.*?)</span>',
                viewer_html,
                flags=re.S,
            )
            file_name = title_match.group(1).strip() if title_match else "리서치"
            pdf_url = f"{VIEWER_BASE}/streamdocs/v4/documents/{doc_id}"
            return {
                "gate_url": gate_url,
                "viewer_url": f"{VIEWER_BASE}/pv/viewer",
                "doc_id": doc_id,
                "file_name": file_name,
                "pdf_url": pdf_url,
            }
    except Exception as e:
        logger.error(f"DBfi: Failed to extract PDF URL: {e}")
        return None


async def DBfi_checkNewArticle():
    sec_firm_order = 19
    
    if not URL_PATHS:
        logger.error("DBfi: No URL_PATHS found in config. Check secrets.json.")
        return []

    # 1. 150건 가져오기 (카테고리별 50건)
    timeout = aiohttp.ClientTimeout(total=15)
    raw_items = []

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context), timeout=timeout) as session:
        for item in URL_PATHS:
            if isinstance(item, str):
                url_path, board_order = item, 0
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                url_path, board_order = item[0], item[1]
            elif isinstance(item, (list, tuple)) and len(item) == 1:
                url_path, board_order = item[0], 0
            else:
                logger.warning(f"DBfi: Invalid URL_PATH item ignored: {item}")
                continue

            headers = {
                **HEADERS_TEMPLATE,
                "Referer": f"{BASE_URL}/mre/mre_CompanyAll_lst.do",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            }
            try:
                async with session.post(f"{BASE_URL}{url_path}", headers=headers) as response:
                    if response.status != 200:
                        logger.warning(f"DBfi: 목록 조회 실패 {url_path} ({response.status})")
                        continue
                    jres = await response.json()
                    items = jres.get("data", [])[:50]
                    
                    for item in items:
                        raw_items.append((item, board_order))
                        
                    logger.info(f"DBfi: {url_path} → {len(items)}건 수집 완료")
            except Exception as e:
                logger.error(f"DBfi: {url_path} 네트워크 오류: {e}")

    logger.info(f"DBfi: 서버에서 총 {len(raw_items)}건 수집 완료")

    # 2. 우리 DB 전체에서 중복 제거
    db = get_db()
    existing_keys = db.fetch_existing_keys(sec_firm_order, days_limit=None)
    # 도메인 변경 대응: m.db-fi.com → m.dbsec.co.kr 양쪽 키 모두 허용
    BASE_URL_LEGACY = "https://m.db-fi.com"
    _normalized = set(existing_keys)
    for k in existing_keys:
        if BASE_URL_LEGACY in k:
            _normalized.add(k.replace(BASE_URL_LEGACY, BASE_URL))
        elif BASE_URL in k:
            _normalized.add(k.replace(BASE_URL, BASE_URL_LEGACY))
    existing_keys = _normalized
    logger.info(f"DBfi: DB 전체 기존 key {len(existing_keys)}개 확인 (정규화 포함)")

    candidates = []
    for item, board_order in raw_items:
        key = f"{BASE_URL}/appData/descRsh/{item['rid']}.json"
        if key in existing_keys:
            continue
        
        firm_info = FirmInfo(sec_firm_order=sec_firm_order, article_board_order=board_order)
        candidates.append({
            "sec_firm_order": sec_firm_order,
            "article_board_order": board_order,
            "firm_nm": firm_info.get_firm_name(),
            "reg_dt": item["rdt"][:8],
            "article_url": "",
            "telegram_url": "",
            "pdf_url": "",
            "article_title": item["tit"],
            "writer": item["wnm"],
            "CATEGORY": item["div"],
            "key": key,
                    "report_unique_key": key,
            "save_time": datetime.now().isoformat(),
        })

    logger.info(f"DBfi: DB 전체 대조 후 신규 건수 {len(candidates)}건")
    if not candidates:
        return []

    # 3. 중복이 아닌 건에 대해서 세부 pdf url 가져오기
    return await fetch_detailed_url(candidates)


# 상세 URL로 후처리 데이터 획득 함수
async def fetch_detailed_url(articles):
    def _normalize_text(value: str) -> str:
        return re.sub(r"\s+", " ", (value or "").strip())

    def _article_signature(article: dict) -> tuple[str, str, str, str]:
        # DB증권은 동일 리포트가 여러 게시판에 중복 노출되는 경우가 있어
        # 제목/작성자/등록일/카테고리 기준으로 대표 건만 상세 조회합니다.
        return (
            _normalize_text(article.get("reg_dt", "")),
            _normalize_text(article.get("article_title", "")),
            _normalize_text(article.get("writer", "")),
            _normalize_text(article.get("CATEGORY", "")),
        )

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context), timeout=timeout) as session:
        # Group by article signature first, then choose a single representative key
        # to avoid hitting detail endpoints for the same report multiple times.
        pending_by_signature = {}
        skipped_existing = 0
        collapsed_duplicates = 0
        for article in articles:
            if article.get("telegram_url") and article.get("pdf_url"):
                continue
            key_url = article.get("key")
            if not key_url:
                continue
            signature = _article_signature(article)
            bucket = pending_by_signature.setdefault(
                signature,
                {"representative_key": key_url, "articles": [], "keys": set()},
            )
            if bucket["articles"]:
                collapsed_duplicates += 1
            bucket["keys"].add(key_url)
            bucket["articles"].append(article)

        encoded_url_cache = {}
        extracted_cache = {}
        representative_items = list(pending_by_signature.values())
        logger.info(
            "DBfi: detail enrichment targets={} representative_keys={} skipped_existing={} collapsed_duplicates={}",
            sum(len(item["articles"]) for item in representative_items),
            len(representative_items),
            skipped_existing,
            collapsed_duplicates,
        )

        # Semaphore 설정으로 과도한 동시 요청을 방지하며 속도 최적화
        sem = asyncio.Semaphore(3)

        async def process_item(item):
            key_url = item["representative_key"]
            key_articles = item["articles"]
            try:
                # 동시 요청 제어를 Semaphore 블록 내에서 실행
                async with sem:
                    encoded_url = encoded_url_cache.get(key_url)
                    if not encoded_url:
                        async with session.post(key_url, headers=HEADERS_TEMPLATE) as response:
                            if response.status != 200:
                                logger.warning(f"Failed to fetch details from {key_url}. Status code: {response.status}")
                                return
                            try:
                                detail_data = await response.json()
                            except json.JSONDecodeError:
                                logger.error(f"Failed to parse JSON for {key_url}")
                                return
                            encoded_url = detail_data.get("data", {}).get("url", "")
                            encoded_url_cache[key_url] = encoded_url

                    if not encoded_url:
                        logger.warning(f"DBfi: Empty document id for {key_url}")
                        return

                    extracted = extracted_cache.get(encoded_url)
                    if extracted is None:
                        extracted = await extract_dbfi_pdf_url(session, encoded_url)
                        extracted_cache[encoded_url] = extracted

                    if not extracted:
                        logger.warning(f"DBfi: PDF URL extraction failed for {key_url}")
                        return

                    gate_url = extracted["gate_url"]
                    pdf_url = extracted["pdf_url"]
                    for article in key_articles:
                        article["telegram_url"] = gate_url
                        article["pdf_url"] = pdf_url
                        article["FILE_NAME"] = extracted["file_name"]
                        article["DOC_ID"] = extracted["doc_id"]
                        article["GATE_URL"] = gate_url
                        article["VIEWER_URL"] = extracted["viewer_url"]
                    logger.info(
                        "DBfi: key={} affected={} file={} docId={} gate={}",
                        key_url,
                        len(key_articles),
                        extracted["file_name"],
                        extracted["doc_id"],
                        gate_url,
                    )
            except Exception as e:
                logger.error(f"Network or timeout error while fetching detailed URL ({key_url}): {e}")

        # 모든 대표 아이템 상세 조회를 병렬로 실행
        tasks = [process_item(item) for item in representative_items]
        await asyncio.gather(*tasks)

    return articles


async def DBfi_detail(articles, firm_info=None, db=None):
    """
    DB증권 enrichment 전용 함수 (WARP 대응).
    기존 DB 레코드의 bad URL을 복구합니다.
    - key URL → encoded_url → gate_url + pdf_url
    - 건별 DB 업데이트 (db 파라미터 전달 시)
    - 직접 접속 실패 시 WARP SOCKS5 프록시 fallback
    """
    if not articles:
        return []

    import time
    loop = asyncio.get_event_loop()

    def _request_sync(url, headers, data=None, use_warp=False):
        """sync POST 요청 (직접 or WARP)"""
        kwargs = dict(headers=headers, verify=False, timeout=20)
        if use_warp:
            kwargs['proxies'] = PROXIES
            kwargs['timeout'] = 30
        try:
            resp = requests.post(url, data=data, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception:
            return None

    def _fetch_key_url_sync(key_url):
        """key URL POST → encoded_url (직접→WARP fallback)"""
        for attempt in range(1, DBF_DIRECT_RETRIES + 1):
            resp = _request_sync(key_url, HEADERS_TEMPLATE)
            if resp:
                try:
                    data = resp.json()
                    return data.get("data", {}).get("url", "")
                except Exception:
                    pass
            if attempt < DBF_DIRECT_RETRIES:
                time.sleep(attempt)
        for attempt in range(1, DBF_WARP_RETRIES + 1):
            resp = _request_sync(key_url, HEADERS_TEMPLATE, use_warp=True)
            if resp:
                try:
                    data = resp.json()
                    return data.get("data", {}).get("url", "")
                except Exception:
                    pass
            if attempt < DBF_WARP_RETRIES:
                time.sleep(attempt)
        return ""

    def _extract_doc_id_from_html(html_text):
        """HTML에서 doc_id 추출 (8가지 패턴)"""
        patterns = [
            r'<div[^>]*id="([^"]+)"[^>]*class="item"',
            r'<div[^>]*class="item"[^>]*id="([^"]+)"',
            r'docId["\']\s*:\s*["\']([^"\']+)["\']',
            r'/streamdocs/v4/documents/([a-zA-Z0-9_-]+)',
            r'data-docid["\']?\s*=\s*["\']([^"\']+)["\']',
            r'location\.href\s*[=]\s*["\'].*?doc[Ii][Dd]=([^"\'&]+)',
            r'openDoc\s*\(\s*["\']([^"\']+)["\']',
            r'https?://[^"\']*?streamdocs[^"\']*?documents/([a-zA-Z0-9_-]+)',
        ]
        for pat in patterns:
            m = re.search(pat, html_text)
            if m:
                return m.group(1)
        return None

    def _extract_pdf_sync(encoded_url):
        """gate/viewer flow - 3단계 전략"""
        if not encoded_url:
            return None
        import base64
        token = urlparse.unquote(encoded_url)
        gate_q = urlparse.quote(token, safe="")
        gate_url = f"{VIEWER_BASE}/pv/gate?q={gate_q}"

        pv_headers = {
            "User-Agent": HEADERS_TEMPLATE["User-Agent"],
            "Content-Type": HEADERS_TEMPLATE["Content-Type"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": gate_url,
        }

        def _do_extract(use_warp=False):
            kwargs = dict(headers=pv_headers, verify=False, timeout=30)
            if use_warp:
                kwargs['proxies'] = PROXIES
                kwargs['timeout'] = 45
            try:
                s = requests.Session()

                gate_resp = None
                try:
                    gk = dict(headers=pv_headers, verify=False, timeout=15)
                    if use_warp:
                        gk['proxies'] = PROXIES
                    gate_resp = s.get(gate_url, **gk)
                except Exception:
                    pass

                s.post(f"{VIEWER_BASE}/pv/auth", **kwargs)

                payload = {"q": token, "c": "", "target": "", "docId": ""}
                vr = s.post(f"{VIEWER_BASE}/pv/viewer", data=payload, **kwargs)
                html = vr.text if vr.status_code == 200 else ""
                html_len = len(html)

                # 전략 A: viewer HTML
                doc_id = _extract_doc_id_from_html(html) if html else None

                # 전략 B: viewer 짧으면 gate 응답
                if not doc_id and html_len < 3000 and gate_resp and gate_resp.status_code == 200:
                    doc_id = _extract_doc_id_from_html(gate_resp.text)
                    if doc_id:
                        logger.info(f"  ✓ doc_id from gate response (viewer_short={html_len}): {doc_id}")

                # 전략 C: base64 토큰 → streamdocs 직접 HEAD
                if not doc_id:
                    try:
                        decoded = base64.b64decode(token).decode('utf-8', errors='replace')
                        clean_id = re.sub(r'[^a-zA-Z0-9_-]', '', decoded)
                        if len(clean_id) >= 10:
                            test_url = f"{VIEWER_BASE}/streamdocs/v4/documents/{clean_id}"
                            hd = dict(headers=pv_headers, verify=False, timeout=10)
                            if use_warp:
                                hd['proxies'] = PROXIES
                            tr = requests.head(test_url, **hd)
                            if tr.status_code == 200:
                                doc_id = clean_id
                                logger.info(f"  ✓ doc_id from base64 token: {doc_id}")
                    except Exception:
                        pass

                if not doc_id:
                    logger.warning(f"  doc_id 미발견 (warp={use_warp}, html_len={html_len})")
                    if html:
                        logger.debug(f"  viewer HTML: {html[:2000]}")
                    return {"gate_url": gate_url, "viewer_url": f"{VIEWER_BASE}/pv/viewer",
                            "doc_id": "", "file_name": "", "pdf_url": gate_url}

                pdf_url = f"{VIEWER_BASE}/streamdocs/v4/documents/{doc_id}"
                logger.info(f"  ✓ doc_id={doc_id} (warp={use_warp})")
                return {"gate_url": gate_url, "viewer_url": f"{VIEWER_BASE}/pv/viewer",
                        "doc_id": doc_id, "file_name": "", "pdf_url": pdf_url}
            except Exception as e:
                logger.warning(f"  extract 예외 (warp={use_warp}): {e}")
                return None

        result = _do_extract(use_warp=False)
        if result:
            return result
        return _do_extract(use_warp=True)

    # ── Pass 1: key URL → gate_url (빠름) ──
    pass1_ok = 0
    for article in articles:
        key_url = article.get("key")
        if not key_url:
            continue
        try:
            encoded_url = await loop.run_in_executor(None, _fetch_key_url_sync, key_url)
            if not encoded_url:
                continue

            token = urlparse.unquote(encoded_url)
            gate_q = urlparse.quote(token, safe="")
            gate_url = f"{VIEWER_BASE}/pv/gate?q={gate_q}"

            article["telegram_url"] = gate_url
            article["pdf_url"] = gate_url
            article["GATE_URL"] = gate_url
            article["_token"] = encoded_url

            if db and article.get('report_id'):
                await db.update_telegram_url(
                    record_id=article['report_id'],
                    telegram_url=gate_url,
                    article_title=article.get('article_title'),
                    pdf_url=gate_url
                )
            logger.success(f"[DBfi][Pass1] gate_url 저장: {str(article.get('article_title', ''))[:40]}")
            pass1_ok += 1
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.error(f"[DBfi][Pass1] 오류: {key_url} ({e})")

    if pass1_ok:
        logger.success(f"[DBfi][Pass1] 완료: {pass1_ok}/{len(articles)}건 gate_url 저장")

    # ── Pass 2: gate_token → streamdocs pdf_url ──
    pass2_ok = 0
    for article in articles:
        token = article.get("_token")
        gate_url = article.get("GATE_URL")
        if not token or not gate_url:
            continue
        try:
            extracted = await loop.run_in_executor(None, _extract_pdf_sync, token)
            if not extracted or not extracted.get("doc_id"):
                continue

            article["pdf_url"] = extracted["pdf_url"]
            article["DOC_ID"] = extracted["doc_id"]

            if db and article.get('report_id'):
                await db.update_telegram_url(
                    record_id=article['report_id'],
                    telegram_url=gate_url,
                    article_title=article.get('article_title'),
                    pdf_url=extracted['pdf_url']
                )
            logger.success(f"[DBfi][Pass2] pdf_url 갱신: {extracted['pdf_url'][:60]}...")
            pass2_ok += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"[DBfi][Pass2] 오류: {e}")

    if pass2_ok:
        logger.success(f"[DBfi][Pass2] 완료: {pass2_ok}건 pdf_url streamdocs로 갱신")

    return articles


async def main():
    articles = await DBfi_checkNewArticle()
    detailed_articles = await fetch_detailed_url(articles)
    logger.info(f"Fetched total {len(detailed_articles)} detailed articles.")
    for article in detailed_articles[:5]:
        logger.info(
            "DBfi sample | title={} | file={} | pdf={}",
            article.get("article_title"),
            article.get("FILE_NAME"),
            article.get("pdf_url"),
        )


if __name__ == "__main__":
    asyncio.run(main())
