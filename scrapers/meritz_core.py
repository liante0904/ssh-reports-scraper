import sys
"""Meritz Securities — config 기반 HTML 파싱."""
import html
import re, requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def _resolve_meritz_pdf_url(detail_html: str, article_url: str, cfg: dict) -> str:
    soup = BeautifulSoup(detail_html, "html.parser")
    selectors = [
        cfg.get("detail_sel", ""),
        "a[href*='.pdf']",
        "a[href*='WorkFlow']",
        "a[href*='resource/research']",
    ]
    seen = set()
    for selector in selectors:
        if not selector or selector in seen:
            continue
        seen.add(selector)
        for tag in soup.select(selector):
            href = tag.get("href", "")
            if not href:
                continue
            if ".pdf" in href.lower() or "workflow" in href.lower() or "resource/research" in href.lower():
                return urljoin(article_url, href)
    decoded = html.unescape(detail_html)
    match = re.search(r"""https?://[^"'<>\s]+\.pdf(?:\?[^"'<>\s]*)?""", decoded, re.IGNORECASE)
    if match:
        return match.group(0)
    match = re.search(r"""(?<![\w:/.-])/[^\s"'<>]+\.pdf(?:\?[^"'<>\s]*)?""", decoded, re.IGNORECASE)
    if match:
        return urljoin(article_url, match.group(0))
    match = re.search(r"""title=["']([^"']+\.pdf)\s+파일\s+다운로드["']""", decoded, re.IGNORECASE)
    if match:
        filename = match.group(1).strip()
        return urljoin(article_url, f"/include/resource/research/WorkFlow/{filename}")
    return ""

def scrape_meritz(cfg: dict) -> list[dict]:
    if isinstance(cfg, list): cfg = {"urls": cfg}
    elif isinstance(cfg, str): cfg = {"url": cfg}
    # 기본값 (GA standalone에서 list만 전달되는 경우 대비, 2026.06 사이트 구조 반영)
    cfg.setdefault("head_sel", "thead th")
    cfg.setdefault("row_sel", "tbody tr")
    cfg.setdefault("base_url", "https://home.imeritz.com")
    cfg.setdefault("firm_id", 20)
    cfg.setdefault("firm_nm", "메리츠증권")
    cfg.setdefault("detail_sel", "a[href$='.pdf']")  # 2026.06: .fileArea 사라짐, a에 직접 PDF href
    requests.packages.urllib3.disable_warnings()
    hdr = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    result = []
    for board_order, base_url in enumerate(cfg.get("urls", [cfg.get("url","")])):
        if not base_url: continue
        for page in range(1, 2):  # 1페이지만 (200건, GA timeout 고려)
            url = base_url.replace("pageNum=1", f"pageNum={page}")
            try:
                resp = requests.get(url, headers=hdr, timeout=30, verify=False)
                resp.raise_for_status()
            except Exception: break
            soup = BeautifulSoup(resp.text, "html.parser")
            head_rows = soup.select(cfg["head_sel"])
            if not head_rows: break
            hmap = {th.get_text(strip=True): i for i, th in enumerate(head_rows)}
            rows = soup.select(cfg["row_sel"])
            if not rows: break
            for row in rows:
                try:
                    link = row.select_one(f'td:nth-child({hmap.get("제목",0)+1}) a')
                    if not link: continue
                    title = link.get_text(strip=True)
                    article_url = urljoin(cfg["base_url"], link["href"])
                    dc = "작성일" if "작성일" in hmap else "작성일시"
                    rd = re.sub(r"[-./]","",row.select_one(f'td:nth-child({hmap[dc]+1})').get_text(strip=True))
                    wc = "작성자" if "작성자" in hmap else "작성자명"
                    writer = row.select_one(f'td:nth-child({hmap[wc]+1})').get_text(strip=True)
                    # detail fetch for PDF (2026.06: a[href$='.pdf'] 직접)
                    try:
                        dr = requests.get(article_url, headers=hdr, timeout=15, verify=False)
                        dl = _resolve_meritz_pdf_url(dr.text, article_url, cfg)
                    except Exception:
                        dl = ""
                    if not dl:
                        dl = article_url
                    result.append(dict(firm_id=cfg["firm_id"],board_id=board_order,
                        firm_nm=cfg["firm_nm"],report_date=rd,source_url=article_url,
                        telegram_url=dl,pdf_file_url=dl,article_title=title,writer=writer,
                        report_unique_key=dl,save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
                except Exception: continue
    print(f"[meritz] {len(result)} articles collected", file=sys.stderr)
    return result
