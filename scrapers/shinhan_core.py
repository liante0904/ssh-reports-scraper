import sys
"""Shinhan Securities — 순수 스크래핑 코어."""
import json, re, requests
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

LOOKBACK_DAYS = 45
BOARD_MAP = {'giindustry':0,'gicompanyanalyst':1,'giresearchIPO':2,'foreignstock':3,
             'alternative':4,'foreignbond':5,'gibond':6,'gicomment':7,'gieconomy':8,
             'gifuture':9,'gigoodpolio':10,'giperiodicaldaily':11}
BBS_BOARDS = ["foreignstock","giresearchIPO","gieconomy","gicomment","gibond",
              "foreignbond","gifuture","alternative"]
MOBILE_API = "https://m.shinhansec.com/mweb/api/invt/shrh/ishrhShrhList"
BBS_API = "https://bbs2.shinhansec.com/mobile/json.list.do"


def canonical_shinhan_url(url: str) -> str:
    """Normalize Shinhan report URLs so domain/protocol/path migrations do not create new keys."""
    raw = str(url or "").strip()
    if not raw:
        return ""
    raw = raw.replace("shinhaninvest.com", "shinhansec.com")
    parsed = urlsplit(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw

    netloc = parsed.netloc.lower()
    path = parsed.path.replace("/file.do", "/file.pdf.do")
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    return urlunsplit(("https", netloc, path, query, ""))

def scrape_shinhan(cfg: dict) -> list[dict]:
    """cfg: {mobile_api_url, str_boards, bbs_boards}"""
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()
    headers = {"User-Agent":"Mozilla/5.0","Content-Type":"application/json",
               "Referer":"https://m.shinhansec.com/mweb/invt/shrh/ishrh1001"}
    result = []
    cutoff = (datetime.now(timezone(timedelta(hours=9))) - timedelta(days=45)).strftime("%Y%m%d")

    # STR boards (POST to mobile API)
    for bbs_name in cfg.get("str_boards","giperiodicaldaily|gistockchart|plananalysis|gicompanyanalyst|giindustry|gieconomy|fxmarket|commodity|gibond|foreignbond").split("|"):
        repeat_key = ""
        for _ in range(30):
            data = {"url":"/mweb/api/invt/shrh/ishrhShrhList","callbackFun":"ishrh1001.tab1_callbackFun",
                    "bbs_name":bbs_name,"repeatKeyP":"","repeatKeyN":repeat_key,
                    "curPage":1,"lastPageFlag":"true","tran":False}
            try:
                resp = requests.post(cfg.get("mobile_api_url",MOBILE_API), headers=headers, json=data, timeout=15, verify=False)
                if resp.status_code != 200: break
                jres = resp.json()
                if jres.get("header",{}).get("resultCode") != "00000": break
            except Exception: break
            items = jres.get("body",{}).get("list01",{}).get("outputList",[]) or []
            for item in items:
                reg_dt = re.sub(r"[^0-9]","",str(item.get("date","")))[:8]
                if not reg_dt or reg_dt < cutoff: continue
                dl = canonical_shinhan_url(item.get("attachment_url") or "")
                if not dl.startswith("http"): continue
                board = BOARD_MAP.get(bbs_name, 99)
                result.append({"firm_id":1,"board_id":board,
                    "firm_nm":"신한증권","report_date":reg_dt,"download_url":dl,"telegram_url":dl,
                    "article_title":item.get("title","").strip(),"writer":item.get("nickname","").strip(),
                    "report_unique_key":dl,
                    "save_at":datetime.now(timezone(timedelta(hours=9))).isoformat()})
            next_key = jres.get("header",{}).get("repeatKeyN","")
            if not next_key or next_key == repeat_key: break
            repeat_key = next_key

    # BBS boards (GET)
    for board_name in cfg.get("bbs_boards",BBS_BOARDS):
        for page in range(1, 4):
            url = f"{BBS_API}?boardName={board_name}&curPage={page}"
            try:
                resp = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=15, verify=False)
                if resp.status_code != 200: break
                jres = resp.json()
            except Exception: break
            items = jres.get("list",[]) or []
            title_map = jres.get("title",{}) or {}
            rev_map = {v:k for k,v in title_map.items()}
            t_key = rev_map.get("제목","f1")
            d_key = rev_map.get("등록일","f0")
            u_key = rev_map.get("파일명","f3")
            w_key = rev_map.get("작성자") or rev_map.get("애널리스트") or "f5"
            for item in items:
                reg_dt = re.sub(r"[^0-9]","",str(item.get(d_key,"")))[:8]
                if not reg_dt or reg_dt < cutoff: continue
                dl = canonical_shinhan_url(item.get(u_key, ""))
                if not dl.startswith("http"): continue
                board = BOARD_MAP.get(board_name, 99)
                result.append({"firm_id":1,"board_id":board,
                    "firm_nm":"신한증권","report_date":reg_dt,"download_url":dl,"telegram_url":dl,
                    "article_title":item.get(t_key,"").strip(),"writer":item.get(w_key,"").strip(),
                    "report_unique_key":dl,
                    "save_at":datetime.now(timezone(timedelta(hours=9))).isoformat()})
    print(f"[shinhan] {len(result)} articles collected before deduplication", file=sys.stderr)
    seen = set()
    deduped_result = []
    for item in result:
        dup_key = (item.get("report_date", ""), item["article_title"])
        if dup_key not in seen:
            seen.add(dup_key)
            deduped_result.append(item)
    print(f"[shinhan] {len(deduped_result)} articles collected after deduplication", file=sys.stderr)
    return deduped_result
