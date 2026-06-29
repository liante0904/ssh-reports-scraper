import asyncio
import json
import re
from datetime import datetime
import os
import sys
import time
import urllib.parse as urlparse
from loguru import logger
import requests
import urllib3
urllib3.disable_warnings()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.db_factory import get_db
from modules.DBfi_19 import HEADERS_TEMPLATE, VIEWER_BASE, SOCKS_PROXY

PROXIES = {'http': SOCKS_PROXY, 'https': SOCKS_PROXY}


def _request_with_warp(url, headers, data=None):
    """직접 접속 → 실패 시 WARP fallback"""
    # 직접 시도 (2회, 10s timeout)
    for attempt in range(1, 3):
        try:
            r = requests.post(url, headers=headers, data=data, verify=False, timeout=10)
            r.raise_for_status()
            return r
        except Exception:
            if attempt < 2:
                time.sleep(attempt)
    # WARP fallback (3회, 30s timeout)
    for attempt in range(1, 4):
        try:
            r = requests.post(url, headers=headers, data=data,
                              proxies=PROXIES, verify=False, timeout=30)
            r.raise_for_status()
            return r
        except Exception:
            if attempt < 3:
                time.sleep(attempt)
    return None


def _extract_doc_id_from_html(html_text):
    """HTML에서 doc_id 추출 (8가지 패턴)"""
    patterns = [
        # A: <div id="..." class="item">
        r'<div[^>]*id="([^"]+)"[^>]*class="item"',
        # B: <div class="item" id="...">
        r'<div[^>]*class="item"[^>]*id="([^"]+)"',
        # C: JS 변수 docId
        r'docId["\']\s*:\s*["\']([^"\']+)["\']',
        # D: /streamdocs/v4/documents/ 경로 직접
        r'/streamdocs/v4/documents/([a-zA-Z0-9_-]+)',
        # E: data-docid="..."
        r'data-docid["\']?\s*=\s*["\']([^"\']+)["\']',
        # F: location.href에 docId
        r'location\.href\s*[=]\s*["\'].*?doc[Ii][Dd]=([^"\'&]+)',
        # G: openDoc("...") 호출
        r'openDoc\s*\(\s*["\']([^"\']+)["\']',
        # H: PDF URL이 HTML에 직접 포함
        r'https?://[^"\']*?streamdocs[^"\']*?documents/([a-zA-Z0-9_-]+)',
    ]
    for pat in patterns:
        m = re.search(pat, html_text)
        if m:
            return m.group(1)
    return None


def _extract_pdf_sync(encoded_url):
    """gate/viewer flow (sync + WARP fallback) - 3단계 전략"""
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

    def _do(use_warp=False):
        kwargs = dict(headers=pv_headers, verify=False, timeout=30)
        if use_warp:
            kwargs['proxies'] = PROXIES
            kwargs['timeout'] = 45
        try:
            s = requests.Session()

            # 1단계: gate GET (세션 초기화)
            gate_resp = None
            try:
                gk = dict(headers=pv_headers, verify=False, timeout=15)
                if use_warp:
                    gk['proxies'] = PROXIES
                gate_resp = s.get(gate_url, **gk)
            except Exception:
                pass

            # 2단계: auth POST
            s.post(f"{VIEWER_BASE}/pv/auth", **kwargs)

            # 3단계: viewer POST
            payload = {"q": token, "c": "", "target": "", "docId": ""}
            vr = s.post(f"{VIEWER_BASE}/pv/viewer", data=payload, **kwargs)
            html = vr.text if vr.status_code == 200 else ""
            html_len = len(html)

            # 전략 A: viewer HTML에서 doc_id 추출
            doc_id = _extract_doc_id_from_html(html) if html else None

            # 전략 B: viewer가 짧으면(1786) gate 응답에서 doc_id 추출
            if not doc_id and html_len < 3000 and gate_resp and gate_resp.status_code == 200:
                doc_id = _extract_doc_id_from_html(gate_resp.text)
                if doc_id:
                    logger.info(f"  ✓ doc_id from gate response (viewer_short={html_len}): {doc_id}")

            # 전략 C: base64 토큰 디코딩 → streamdocs 직접 HEAD
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
                return {"gate_url": gate_url, "doc_id": "", "file_name": "", "pdf_url": gate_url}

            pdf_url = f"{VIEWER_BASE}/streamdocs/v4/documents/{doc_id}"
            logger.info(f"  ✓ doc_id={doc_id} (warp={use_warp})")
            return {"gate_url": gate_url, "doc_id": doc_id, "file_name": "", "pdf_url": pdf_url}
        except Exception as e:
            logger.warning(f"  extract 예외 (warp={use_warp}): {e}")
            return None

    result = _do(use_warp=False)
    if result:
        return result
    return _do(use_warp=True)


async def fix_dbfi_urls():
    db = get_db()
    loop = asyncio.get_event_loop()

    # 1. 수정 대상 조회
    query = """
    SELECT report_id, "key", "telegram_url", "pdf_url", pdf_sync_status
    FROM "tbl_sec_reports"
    WHERE sec_firm_order = 19
      AND (
          "telegram_url" NOT LIKE 'https://whub.dbsec.co.kr/pv/gate%%'
          OR pdf_sync_status != 2
      )
    ORDER BY report_id DESC
    """

    try:
        rows = await db.execute_query(query)
        logger.info(f"Found {len(rows)} DBfi reports to fix.")
    except Exception as e:
        logger.error(f"Failed to query reports: {e}")
        return

    if not rows:
        logger.info("No reports need fixing.")
        return

    # ── Pass 1: key URL → gate_url (빠름, 모두 저장) ──
    pass1_ok = 0
    pass1_records = []
    for row in rows:
        report_id = row['report_id']
        key_url = row['key']
        article_title = row.get('article_title', '')

        logger.info(f"[Pass1] report_id={report_id}, key={key_url}")
        try:
            resp = await loop.run_in_executor(None, _request_with_warp, key_url, HEADERS_TEMPLATE)
            if not resp:
                logger.warning(f"  key_url 실패")
                continue

            detail_data = resp.json()
            encoded_url = detail_data.get("data", {}).get("url", "")
            if not encoded_url:
                logger.warning(f"  encoded_url 없음")
                continue

            token = urlparse.unquote(encoded_url)
            gate_q = urlparse.quote(token, safe="")
            gate_url = f"{VIEWER_BASE}/pv/gate?q={gate_q}"

            await db.execute_query("""
                UPDATE "tbl_sec_reports"
                SET "telegram_url" = %s, "pdf_url" = %s, pdf_sync_status = 0
                WHERE report_id = %s
            """, (gate_url, gate_url, report_id))
            logger.success(f"  ✓ gate_url 저장: {gate_url[:60]}...")
            pass1_ok += 1
            pass1_records.append((report_id, encoded_url, gate_url))

            await asyncio.sleep(0.2)
        except Exception as e:
            logger.error(f"  오류: {e}")

    logger.info(f"[DBfi][Pass1] 완료: {pass1_ok}/{len(rows)}건 gate_url 저장")

    # ── Pass 2: gate_token → streamdocs pdf_url (추가 복구) ──
    pass2_ok = 0
    for report_id, encoded_url, gate_url in pass1_records:
        logger.info(f"[Pass2] report_id={report_id}")
        try:
            extracted = await loop.run_in_executor(None, _extract_pdf_sync, encoded_url)
            if not extracted or not extracted.get("doc_id"):
                continue

            new_pdf = extracted['pdf_url']
            await db.execute_query("""
                UPDATE "tbl_sec_reports"
                SET "pdf_url" = %s
                WHERE report_id = %s
            """, (new_pdf, report_id))
            logger.success(f"  ✓ pdf_url streamdocs로 갱신: {new_pdf[:60]}...")
            pass2_ok += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"  오류: {e}")

    if pass2_ok:
        logger.info(f"[DBfi][Pass2] 완료: {pass2_ok}건 pdf_url streamdocs로 갱신")
    logger.info(f"[DBfi] fix 최종: gate_url {pass1_ok}건 + pdf_url {pass2_ok}건")


if __name__ == "__main__":
    logger.add("logs/fix_dbfi_urls.log", rotation="10 MB")
    asyncio.run(fix_dbfi_urls())
