import sys
"""Shinyoung Securities — 순수 스크래핑 코어. 모든 scraping detail은 cfg JSON으로 주입."""
import json, re, requests
from datetime import datetime, timezone, timedelta
from scrapers.config_guard import normalize_cfg


def scrape_shinyoung(cfg: dict) -> list[dict]:
    cfg = normalize_cfg(cfg, firm_key="Shinyoung")
    urls = cfg.get("urls") or []
    if cfg.get("url") and not urls:
        urls = [cfg["url"]]
    item_keys = {
        "title": "TITLE",
        "report_date": "APPDATE",
        "writer": "EMPNM",
        "seq": "SEQ",
        "seq_val": "SEQ",
        "bbsno": "BBSNO",
        "bbsno_val": "BBSNO",
        **cfg.get("item_keys", {}),
    }
    auth_headers = {
        "Accept": "text/plain, */*; q=0.01",
        "Connection": "keep-alive",
        "Host": "www.shinyoung.com",
        "Origin": "https://www.shinyoung.com",
        "Referer": "https://www.shinyoung.com/?page=10078&head=0",
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
        **cfg.get("auth_headers", {}),
    }
    auth_urls = {
        "step1": "https://www.shinyoung.com/Common/authTr/devPass",
        "step2": "https://www.shinyoung.com/Common/checkAuth",
        "step3": "https://www.shinyoung.com/Common/authTr/downloadFilePath",
        **cfg.get("auth_urls", {}),
    }
    list_url = cfg.get("list_url") or (urls[0] if urls else "")
    if not list_url:
        raise ValueError("Shinyoung: list_url or URL list is required")
    requests.packages.urllib3.disable_warnings()
    sess = requests.Session()
    result = []

    resp = sess.post(
        list_url,
        params=cfg.get("list_payload", {"KEYWORD": "", "rows": "50", "page": "1"}),
        timeout=30,
        verify=False,
    )
    resp.raise_for_status()
    list_json = resp.json()
    items = list_json.get(cfg.get("list_result_key", "rows")) or list_json.get("rows", [])

    skipped = 0
    for item in items:
        try:
            title = item[item_keys["title"]]
            mkt_keyword = cfg.get("mkt_tp_keyword", "해외주식")
            mkt_tp = mkt_keyword if mkt_keyword in title else "KR"

            # 3-step auth per article for PDF URL
            sess.post(auth_urls["step1"], headers=auth_headers, timeout=30, verify=False)
            sess.post(auth_urls["step2"], headers=auth_headers, timeout=30, verify=False)
            r3 = sess.post(auth_urls["step3"],
                           data={item_keys["seq"]: item[item_keys["seq_val"]],
                                 item_keys["bbsno"]: item[item_keys["bbsno_val"]]},
                           headers={**auth_headers, "Content-Type": cfg.get("auth_content_type", "application/x-www-form-urlencoded; charset=UTF-8")},
                           timeout=30,
                           verify=False)
            jres = json.loads(r3.text)
            for key in cfg.get("download_json_path", "FILEINFO.FILEPATH").split("."):
                jres = jres[key]
            dl = cfg.get("download_url_tpl") or (urls[1] if len(urls) > 1 else "https://www.shinyoung.com/files/")
            dl += jres

            # 2026.06.21 fix: GA Import 중복제거 및 DB 업서트 시 식별값으로 사용될 key, report_unique_key 설정 추가
            result.append({
                "firm_id": 7, "board_id": 0, "firm_nm": cfg.get("firm_nm", "신영증권"),
                "report_date": re.sub(r"[-./]", "", item[item_keys["report_date"]]),
                "writer": item.get(item_keys["writer"], ""),
                "article_title": title, "telegram_url": dl, "download_url": dl,
                "report_unique_key": dl,
                "save_at": datetime.now(timezone(timedelta(hours=9))).isoformat(),
            })
        except Exception as exc:
            skipped += 1
            if skipped <= 3:
                print(f"[shinyoung] skipped item: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
    if skipped:
        print(f"[shinyoung] skipped {skipped} items", file=sys.stderr)
    print(f"[shinyoung] {len(result)} articles collected", file=sys.stderr)
    return result
