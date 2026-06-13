"""Shinyoung Securities — 순수 스크래핑 코어. 모든 scraping detail은 cfg JSON으로 주입."""
import json, re, requests
from datetime import datetime, timezone, timedelta


def scrape_shinyoung(cfg: dict) -> list[dict]:
    requests.packages.urllib3.disable_warnings()
    sess = requests.Session()
    result = []

    resp = sess.post(cfg["list_url"], params=cfg["list_payload"], timeout=30, verify=False)
    items = resp.json().get(cfg["list_result_key"], [])

    for item in items:
        try:
            title = item[cfg["item_keys"]["title"]]
            mkt_tp = cfg["mkt_tp_keyword"] if cfg["mkt_tp_keyword"] in title else "KR"

            # 3-step auth per article for PDF URL
            ah = cfg["auth_headers"]
            sess.post(cfg["auth_urls"]["step1"], headers=ah)
            sess.post(cfg["auth_urls"]["step2"], headers=ah)
            r3 = sess.post(cfg["auth_urls"]["step3"],
                           data={cfg["item_keys"]["seq"]: item[cfg["item_keys"]["seq_val"]],
                                 cfg["item_keys"]["bbsno"]: item[cfg["item_keys"]["bbsno_val"]]},
                           headers={**ah, "Content-Type": cfg["auth_content_type"]})
            jres = json.loads(r3.text)
            for key in cfg["download_json_path"].split("."):
                jres = jres[key]
            dl = cfg["download_url_tpl"] + jres

            result.append({
                "sec_firm_order": 7, "article_board_order": 0, "firm_nm": cfg.get("firm_nm", "신영증권"),
                "reg_dt": re.sub(r"[-./]", "", item[cfg["item_keys"]["reg_dt"]]),
                "writer": item.get(cfg["item_keys"]["writer"], ""),
                "article_title": title, "telegram_url": dl, "download_url": dl,
                "mkt_tp": mkt_tp, "key": dl, "report_unique_key": dl,
                "save_time": datetime.now(timezone(timedelta(hours=9))).isoformat(),
            })
        except Exception:
            continue
    print(f"[shinyoung] {len(result)} articles collected", file=sys.stderr)
    return result
