"""iM Securities — 순수 스크래핑 코어."""
import sys
import base64, json, random, re, time, requests
from datetime import datetime, timezone, timedelta

# ── Anti-detection: rotated per-session to reduce fingerprint consistency ──
_USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-S908N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; SM-S928N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.119 Mobile Safari/537.36",
]
_ACCEPT_LANGUAGES = [
    "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "ko,en-US;q=0.9,en;q=0.8",
]

_SEC_CH_UA_MOBILE = [
    '"?1"',
    '"?0"',
]
_SEC_CH_UA_PLATFORM = [
    '"Android"',
    '"iOS"',
    '"macOS"',
]


def _gen_secure_key():
    return base64.b64encode(f"sJS{int(time.time() * 1000)}".encode()).decode()


def _random_delay(min_s=0.1, max_s=0.4):
    """Small jitter between individual attachment requests (shallow slope)."""
    time.sleep(random.uniform(min_s, max_s))


def _page_delay():
    """Slightly longer pause between board-page fetches."""
    time.sleep(random.uniform(0.4, 1.2))


def _pick_headers(base_url, bid):
    """Build browser-like request headers with randomisation."""
    ua = random.choice(_USER_AGENTS)
    return {
        "Referer": f"{base_url}/mobile/invest/invest02.jsp?bid={bid}&isSmartHi=N",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": random.choice(_ACCEPT_LANGUAGES),
        "X-Requested-With": "XMLHttpRequest",
        "Origin": base_url,
        "User-Agent": ua,
        "sec-ch-ua": '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
        "sec-ch-ua-mobile": random.choice(_SEC_CH_UA_MOBILE),
        "sec-ch-ua-platform": random.choice(_SEC_CH_UA_PLATFORM),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def scrape_imfn(cfg) -> list[dict]:
    """cfg: base URL string 또는 config dict."""
    bids = ["R_E08", "R_E09", "R_E14", "R_E03", "R_E04", "R_E05"]
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    base_url = cfg.get("urls", [cfg.get("url", "")])[0] if isinstance(cfg.get("urls"), list) else cfg.get("url", "")
    if not base_url: return []
    requests.packages.urllib3.disable_warnings()
    sess = requests.Session()

    # Pick a consistent User-Agent for the session (rotated per run)
    sess_ua = random.choice(_USER_AGENTS)
    sess.headers.update({
        "User-Agent": sess_ua,
        "Accept-Language": random.choice(_ACCEPT_LANGUAGES),
        "Cache-Control": "no-cache",
    })
    result = []

    secure_key = _gen_secure_key()

    # Register secure key (once) — this also establishes the server-side JSESSIONID
    try:
        sess.post(f"{base_url}/inc/common/PrivateSecuerKey.jsp",
                  headers={"Referer": f"{base_url}/mobile/invest/invest02.jsp?bid=R_E08&isSmartHi=N",
                           "User-Agent": sess_ua},
                  data={"_secureKey": secure_key}, timeout=15, verify=False)
    except Exception:
        pass

    for board_order, bid in enumerate(bids):
        page = 1
        while page <= 3:
            headers = _pick_headers(base_url, bid)
            data = {"tr_cd": "db/board/TWBBACL/board_list", "bid": bid,
                    "cur_page": str(page), "num_page": "100", "secureKey": secure_key}
            try:
                resp = sess.post(f"{base_url}/_json/source.jsp", headers=headers, data=data, timeout=15, verify=False)
                if resp.status_code != 200:
                    break
                items = json.loads(resp.content.decode('utf-8'))[0]
                if not items:
                    break
            except Exception:
                break

            for item in items:
                try:
                    _random_delay(0.08, 0.25)  # human-speed pacing between attachment calls
                    attach_params = {"bid": item["bid"], "aid": item["aid"],
                                     "tr_cd": "db/research/twbbacl_attach", "secureKey": secure_key}
                    attach_resp = sess.post(f"{base_url}/_json/source.jsp",
                                            headers=headers,
                                            data=attach_params, timeout=15, verify=False)
                    jres = json.loads(attach_resp.content.decode('utf-8'))[0][0]
                    attach_url = f"https://www.imfnsec.com/upload/{jres['file_dir']}/{jres['file_name']}"

                    result.append({
                        "firm_id": 18, "board_id": board_order,
                        "firm_nm": "IM증권", "report_date": re.sub(r"[-./]", "", item["reg_dt"]),
                        "telegram_url": attach_url, "pdf_file_url": attach_url,
                        "article_title": item["title"], "writer": item["username"],
                        "report_unique_key": attach_url, "source_url": attach_url,
                        "save_at": datetime.now(timezone(timedelta(hours=9))).isoformat(),
                    })
                except Exception:
                    _random_delay(0.05, 0.12)  # short breath even on failure
                    continue
            _page_delay()
            page += 1

    print(f"[imfn] {len(result)} articles collected", file=sys.stderr)
    return result
