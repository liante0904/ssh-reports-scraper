"""Sangsangin Securities — 원본 form-encoded POST + 쿠키 복원."""
import sys, re, requests
from datetime import datetime, timezone, timedelta

SANG_URL = "https://www.sangsanginib.com/main/notice/notice/getNoticeList.do"

def scrape_sangsangin(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()

    headers = {
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.sangsanginib.com",
        "Referer": "https://www.sangsanginib.com/",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15"
    }

    boards = cfg.get("boards", ["CM0078", "CM0338", "CM0079"])
    item_keys = cfg.get("item_keys", {"reg_dt": "REGDT", "title": "TITLE", "nt_no": "NT_NO"})
    url_tpl = cfg.get("url_tpl",
        "https://www.sangsanginib.com/_upload/attFile/{cms}/{cms}_{nt_no}_1.pdf")

    result = []
    for board_idx, cms_cd in enumerate(boards):
        # Get fresh JSESSIONID
        sess = requests.Session()
        try:
            sess.get("https://www.sangsanginib.com/main/main.cmd", headers={
                "User-Agent": headers["User-Agent"]}, timeout=10)
        except Exception: pass

        data = {
            "pageNum": "1", "src": "all", "cmsCd": cms_cd,
            "rowNum": "10", "startRow": "0", "sdt": "", "edt": ""
        }
        try:
            resp = sess.post(SANG_URL, headers=headers, data=data, timeout=15)
            jres = resp.json()
        except Exception: continue

        # Original: response[0].get('getNoticeList', [])
        items = []
        if isinstance(jres, list) and len(jres) > 0:
            items = jres[0].get("getNoticeList", [])
        elif isinstance(jres, dict):
            items = jres.get("0", {}).get("getNoticeList", [])

        for item in items:
            try:
                rdt = re.sub(r"[-./]", "", item.get(item_keys["reg_dt"], ""))
                title = item.get(item_keys["title"], "")
                nt_no = item.get(item_keys["nt_no"], "")
                dl = url_tpl.replace("{cms}", cms_cd).replace("{nt_no}", nt_no)
                result.append(dict(
                    sec_firm_order=cfg.get("sec_firm_order", 6),
                    article_board_order=board_idx,
                    firm_nm=cfg.get("firm_nm", "상상인증권"),
                    reg_dt=rdt, article_title=title, writer="",
                    download_url=dl, telegram_url=dl, pdf_url=dl,
                    key=dl, report_unique_key=dl,
                    save_time=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue

    print(f"[sangsangin] {len(result)} articles collected", file=sys.stderr)
    return result
