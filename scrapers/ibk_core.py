"""IBK Securities — 보드별 POST URL 사용."""
import sys, re, requests
from datetime import datetime, timezone, timedelta

IBK_BASE = "https://m.ibks.com"
DEFAULT_BOARDS = [
    ("전략/시황","IKO010101","invreport"),
    ("기업분석","IKO010201","busreport"),
    ("산업분석","IKO010301","indreport"),
    ("경제/채권","IKO010401","comment"),
    ("해외기업분석","IKO010501","overseasreport","0"),
    ("글로벌ETF","IKO010501","overseasreport","1"),
]

def scrape_ibk(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json; charset=UTF-8",
        "Origin": "https://m.ibks.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    result = []
    api_urls = cfg.get("urls", [])
    for board_idx, bd_info in enumerate(DEFAULT_BOARDS):
        name, screen, path = bd_info[:3]
        menu_tp = bd_info[3] if len(bd_info) > 3 else None

        # Board-specific POST URL from config
        if isinstance(api_urls, list) and board_idx < len(api_urls):
            target_url = api_urls[board_idx]
        else:
            target_url = f"{IBK_BASE}/iko/{screen}/getInvReportList.do"

        h = dict(headers)
        h["Referer"] = f"{IBK_BASE}/iko/{screen}.do"

        for page in range(1, 6):
            row_size = 50
            payload = {
                "screen": screen,
                "data": {"start_row": (page-1)*row_size+1, "end_row": page*row_size,
                         "row_size": row_size, "pageNo": page, "search_value": "",
                         "br_cd": "", "stDt": "", "edDt": "", "sortCol": "", "sortOpt": ""}
            }
            if menu_tp:
                payload["data"]["menu_tp"] = menu_tp

            try:
                resp = requests.post(target_url, json=payload, headers=h, timeout=15)
                jres = resp.json()
            except Exception: break

            data_block = jres.get("data", {})
            items = data_block.get("list", [])
            if not items: break

            ik = cfg.get("item_keys", {})
            for item in items:
                try:
                    rdt = re.sub(r"[-./]","", str(item.get(ik.get("report_date","REG_DATE"),"")))
                    title = item.get(ik.get("title","TITLE"),"")
                    writer = item.get(ik.get("writer","REG_NAME"),"")
                    fn = item.get(ik.get("file","ATTATCH1"),"")
                    dl = ""
                    if fn:
                        # [IBKS Daily] Start with IBKS reports (GUBUN=DAIL on 전략/시황 board)
                        # use invrespect CDN path instead of invreport.
                        gubun = item.get("GUBUN", "")
                        dl_path = "invrespect" if (gubun == "DAIL" and path == "invreport") else path
                        dl = cfg.get("url_tpl",
                            "https://download.ibks.com/emsdata/tradeinfo/{path}/{file}")\
                            .replace("{path}",dl_path).replace("{file}",fn)
                    result.append(dict(
                        firm_id=cfg.get("firm_id",25),
                        board_id=board_idx,
                        firm_nm=cfg.get("firm_nm","IBK투자증권"),
                        report_date=rdt, article_title=title, writer=writer,
                        download_url=dl, telegram_url=dl, pdf_url=dl,
                        report_unique_key=dl,
                        save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
            if len(items) < row_size: break

    print(f"[ibk] {len(result)} articles collected", file=sys.stderr)
    return result
