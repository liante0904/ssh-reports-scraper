import sys
"""iM Securities — 순수 스크래핑 코어."""
import base64, hashlib, json, random, re, time, requests
from datetime import datetime, timezone, timedelta


def _gen_secure_key():
    return base64.b64encode(f"sJS{int(time.time() * 1000)}".encode()).decode()


def _gen_cookie():
    sid = hashlib.md5(f"session{random.randint(1000,9999)}{time.time()}".encode()).hexdigest()
    ace = f"UID-{hashlib.md5(str(random.randint(0,1000000)).encode()).hexdigest()}"
    return f"JSESSIONID={sid}; ACEFCID={ace}; ACEUACS=undefined;"


def scrape_imfn(base_url: str) -> list[dict]:
    bids = ["R_E08", "R_E09", "R_E14", "R_E03", "R_E04", "R_E05"]
    requests.packages.urllib3.disable_warnings()
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15",
        "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
    })
    result = []

    secure_key = _gen_secure_key()
    cookie = _gen_cookie()

    # Register secure key (once)
    try:
        sess.post(f"{base_url}/inc/common/PrivateSecuerKey.jsp",
                  headers={"Referer": f"{base_url}/mobile/invest/invest02.jsp?bid=R_E08&isSmartHi=N"},
                  data={"_secureKey": secure_key}, timeout=15, verify=False)
    except Exception:
        pass

    for board_order, bid in enumerate(bids):
        headers = {
            "Referer": f"{base_url}/mobile/invest/invest02.jsp?bid={bid}&isSmartHi=N",
            "Cookie": cookie,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
        page = 1
        while page <= 3:
            data = {"tr_cd": "db/board/TWBBACL/board_list", "bid": bid,
                    "cur_page": str(page), "num_page": "100", "secureKey": secure_key}
            try:
                resp = sess.post(f"{base_url}/_json/source.jsp", headers=headers, data=data, timeout=15, verify=False)
                if resp.status_code != 200:
                    break
                items = json.loads(resp.text)[0]
                if not items:
                    break
            except Exception:
                break

            for item in items:
                try:
                    # Fetch attach URL
                    attach_params = {"bid": item["bid"], "aid": item["aid"],
                                     "tr_cd": "db/research/twbbacl_attach", "secureKey": secure_key}
                    attach_resp = sess.post(f"{base_url}/_json/source.jsp",
                                            headers={**headers, "Origin": base_url},
                                            data=attach_params, timeout=15, verify=False)
                    jres = json.loads(attach_resp.text)[0][0]
                    attach_url = f"https://www.imfnsec.com/upload/{jres['file_dir']}/{jres['file_name']}"

                    result.append({
                        "sec_firm_order": 18, "article_board_order": board_order,
                        "firm_nm": "IM증권", "reg_dt": re.sub(r"[-./]", "", item["reg_dt"]),
                        "telegram_url": attach_url, "pdf_url": attach_url,
                        "article_title": item["title"], "writer": item["username"],
                        "key": attach_url, "report_unique_key": attach_url,
                        "save_time": datetime.now(timezone(timedelta(hours=9))).isoformat(),
                    })
                except Exception:
                    continue
            page += 1

    print(f"[imfn] {len(result)} articles collected", file=sys.stderr)
    return result
