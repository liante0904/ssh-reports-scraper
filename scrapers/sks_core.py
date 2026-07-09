"""SK Securities scraper using the JSON API."""
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import PurePosixPath

import requests


def _normalize_date(value):
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    if len(digits) < 8:
        return ""
    candidate = digits[:8]
    try:
        datetime.strptime(candidate, "%Y%m%d")
    except ValueError:
        return ""
    return candidate


def _extract_report_date(item, cfg, pdf_path):
    date_keys = (
        cfg.get("date_key"),
        "RDATE",
        "REGDATE",
        "REGDT",
        "WDATE",
        "WRITE_DATE",
        "CREATED_AT",
    )
    for key in date_keys:
        if key:
            report_date = _normalize_date(item.get(key))
            if report_date:
                return report_date

    filename = PurePosixPath(str(pdf_path or "")).name
    match = re.search(r"(?<![0-9])((?:19|20)[0-9]{6})", filename)
    return _normalize_date(match.group(1)) if match else ""

def scrape_sks(cfg: dict) -> list[dict]:
    requests.packages.urllib3.disable_warnings()
    result = []
    invalid_dates = 0
    urls = cfg.get("urls", [cfg.get("url","")])
    if isinstance(urls, str): urls = [urls]
    for board_order, url in enumerate(urls):
        if not url: continue
        payload = {"searchVal":"","searchType":"","page":1,"rowPerPage":cfg.get("row_per_page",2000),"_r_":"0.999"}
        try:
            resp = requests.post(url, params=payload, timeout=30, verify=False)
            resp.raise_for_status()
            items = resp.json().get(cfg.get("list_key","list"), [])
        except Exception: continue
        for item in items:
            try:
                pdfpath = item.get(cfg.get("pdf_key","PDFPATH"),"").strip()
                dl = ""
                if pdfpath:
                    dl = cfg.get("pdf_base","https://www.sks.co.kr") + cfg.get("pdf_path_prefix","/Upload/Research/") + pdfpath
                report_date = _extract_report_date(item, cfg, pdfpath or dl)
                if not report_date:
                    invalid_dates += 1
                    continue
                title = item.get(cfg.get("title_key","RSUBJECT"),"").strip()
                writer = item.get(cfg.get("writer_key","RWRITER"),"").strip()
                result.append(dict(firm_id=cfg.get("firm_id",26),
                    board_id=board_order,firm_nm=cfg.get("firm_nm","SK증권"),
                    report_date=report_date,telegram_url=dl,pdf_file_url=dl,
                    article_title=title,writer=writer,
                    save_at=datetime.now(timezone(timedelta(hours=9))).isoformat(),
                    report_unique_key=dl))
            except Exception: continue
    print(
        f"[sks] {len(result)} articles collected, "
        f"{invalid_dates} skipped due to invalid date",
        file=sys.stderr,
    )
    return result
