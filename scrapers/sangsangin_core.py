"""Sangsangin Securities — 원본 쿠키 + form-encoded POST."""
import sys, re, requests
from datetime import datetime, timezone, timedelta

def scrape_sangsangin(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    requests.packages.urllib3.disable_warnings()

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "ko",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.sangsanginib.com",
        "Referer": "https://www.sangsanginib.com",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
    }
    cookies = {"SSISTOCK_JSESSIONID": "F63EB7BB0166E9ECA5988FF541287E07",
               "_ga": "GA1.1.467249692.1728208332",
               "_ga_BTXL5GSB67": "GS1.1.1728208331.1.1.1728208338.53.0.0"}

    boards = cfg.get("boards", ["CM0078", "CM0338", "CM0079"])
    item_keys = cfg.get("item_keys", {"report_date": "REGDT", "title": "TITLE", "nt_no": "NT_NO"})
    url_tpl = cfg.get("url_tpl",
        "https://www.sangsanginib.com/_upload/attFile/{cms}/{cms}_{nt_no}_1.pdf")

    # Use URL from config if available, else default
    api_url = cfg.get("url", "")
    if not api_url and isinstance(cfg.get("urls"), list):
        api_url = cfg["urls"][0] if cfg["urls"] else ""
    if not api_url:
        api_url = "https://www.sangsanginib.com/notice/getNoticeList"

    result = []
    for board_idx, cms_cd in enumerate(boards):
        data = {"pageNum": "1", "src": "all", "cmsCd": cms_cd,
                "rowNum": "10", "startRow": "0", "sdt": "", "edt": ""}
        try:
            resp = requests.post(api_url, headers=headers, data=data,
                                cookies=cookies, timeout=15)
            jres = resp.json()
        except Exception: continue

        items = []
        if isinstance(jres, list) and len(jres) > 0:
            items = jres[0].get("getNoticeList", [])
        elif isinstance(jres, dict):
            items = jres.get("0", {}).get("getNoticeList", [])

        for item in items:
            try:
                rdt = re.sub(r"[-./]", "", str(item.get(item_keys["report_date"], "")))
                title = item.get(item_keys["title"], "")
                nt_no = str(item.get(item_keys["nt_no"], ""))
                dl = url_tpl.replace("{cms}", cms_cd).replace("{nt_no}", nt_no)
                result.append(dict(
                    firm_id=cfg.get("firm_id", 6),
                    board_id=board_idx,
                    firm_nm=cfg.get("firm_nm", "상상인증권"),
                    report_date=rdt, article_title=title, writer="",
                     telegram_url=dl, pdf_file_url=dl,
                    report_unique_key=dl,
                    save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
            except Exception: continue

    print(f"[sangsangin] {len(result)} articles collected", file=sys.stderr)
    return result
