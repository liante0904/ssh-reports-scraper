import sys
"""Heungkuk Securities — config 기반 HTML 파싱."""
import re, requests
import urllib.request
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from scrapers.config_guard import normalize_cfg

def _norm_date(text):
    if not text: return ""
    text = text.strip()
    m = re.search(r"(20\d{2})\D+(\d{1,2})\D+(\d{1,2})", text)
    if m: return f"{int(m.group(1)):04d}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    m = re.search(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\s+\d{2}:\d{2}:\d{2}\s+\w+\s+(20\d{2})\b", text, flags=re.IGNORECASE)
    if m:
        months = {m:i+1 for i,m in enumerate(["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"])}
        mon, dd, y = m.groups()
        return f"{int(y):04d}{months[mon.lower()]:02d}{int(dd):02d}"
    digits = re.sub(r"[^0-9]","",text)
    return digits[:8] if len(digits) >= 8 else ""

def _filter_duplicate_pdf_rows(rows: list[dict]) -> list[dict]:
    """같은 PDF URL이 서로 다른 article_url에 연결된 경우, formula delta + report_date
    기준으로 가장 가능성 높은 한 행에만 PDF를 할당하고 나머지는 article_url로 폴백한다."""
    if not rows:
        return rows

    # ── helper: extract numeric key from URL ──
    def _extract_key(url: str, pat: str) -> int | None:
        m = re.search(pat, url)
        return int(m.group(1)) if m else None

    # PDF URL → list of row indices
    pdf_groups: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        pdf = row.get("pdf_file_url") or ""
        if not pdf:
            continue
        pdf_groups.setdefault(pdf, []).append(i)

    # ── resolve shared PDFs ──
    reassign_indices: set[int] = set()
    kept_groups = 0

    for pdf_url, indices in pdf_groups.items():
        if len(indices) < 2:
            continue
        article_urls = {rows[i].get("source_url", "") for i in indices}
        if len(article_urls) <= 1:
            continue  # same article → keep all

        # score each candidate: lower delta = closer to correct PDF
        best_idx = -1
        best_score = (999999, -1)  # (delta, epoch_date for tiebreak)
        winner_report_date = ""

        for idx in indices:
            au = rows[idx].get("source_url", "")
            dl = rows[idx].get("pdf_file_url", "")
            vk = _extract_key(au, r"key=(\d+)")
            dk = _extract_key(dl, r"key=(\d+)")
            if vk is None or dk is None:
                continue
            formula_pk = 2 * vk - 12059
            delta = abs(dk - formula_pk)
            # tiebreak: prefer newer report_date (larger YYYYMMDD int = newer)
            reg = rows[idx].get("report_date", "")
            reg_int = int(reg) if reg.isdigit() and len(reg) == 8 else 0
            score = (delta, -reg_int)  # negate so newer (larger int) = smaller score
            if score < best_score:
                best_score = score
                best_idx = idx
                winner_report_date = reg

        if best_idx >= 0:
            kept_groups += 1
            titles = [rows[i].get("article_title", "")[:40] for i in indices]
            urls = [rows[i].get("source_url", "") for i in indices]
            winner_title = rows[best_idx].get("article_title", "")[:40]
            # mark others for reassignment (clear PDF, use article URL)
            for idx in indices:
                if idx != best_idx:
                    reassign_indices.add(idx)
            print(
                f"[heungkuk] WARN: duplicate PDF URL {pdf_url} "
                f"shared by {len(indices)} articles: titles={titles}, urls={urls}",
                file=sys.stderr,
            )
            print(
                f"[heungkuk] INFO: kept PDF for winner \"{winner_title}\" "
                f"(delta={best_score[0]}, report_date={winner_report_date}), "
                f"reassigning {len(indices)-1} other(s) to article fallback",
                file=sys.stderr,
            )
        else:
            print(
                f"[heungkuk] WARN: duplicate PDF URL {pdf_url} "
                f"shared by {len(indices)} articles but no row has extractable key "
                f"— all rows keep duplicate PDF (possible URL format change)",
                file=sys.stderr,
            )

    if not reassign_indices:
        return rows

    # ── apply reassignments ──
    for idx in reassign_indices:
        row = rows[idx]
        row["pdf_file_url"] = ""
        row["telegram_url"] = row.get("source_url") or row.get("telegram_url", "")

    print(
        f"[heungkuk] duplicate guard: kept PDF for {kept_groups} groups, "
        f"reassigned {len(reassign_indices)} row(s) to article fallback, "
        f"{len(rows)} total rows",
        file=sys.stderr,
    )
    return rows


def _content_disposition_matches(resp, analyst_key: str) -> bool:
    disp = resp.getheader("Content-Disposition", "") or ""
    if ".pdf" not in disp.lower():
        return False
    return not analyst_key or analyst_key in disp


def _head_pdf_ok(url: str, headers: dict, analyst_key: str, timeout: float) -> bool:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": headers.get("User-Agent", "Mozilla/5.0")},
        method="HEAD",
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return resp.status == 200 and _content_disposition_matches(resp, analyst_key)


def _resolve_pdf_download(base: str, view_key: int, analyst_key: str, cfg: dict) -> str | None:
    """공식 PDF key를 검증하고, 명시적으로 허용된 경우에만 짧게 주변 탐색한다.

    GA 기본값은 탐색 비활성화다. 잘못된 PDF를 전송하는 것보다 해당 row를 버리고
    validator에서 실패시키는 편이 안전하다.
    """
    pk = eval(cfg["pdf_formula"].replace("{view_key}", str(view_key)))
    dl = cfg["download_tpl"].replace("{base}", base).replace("{pdf_key}", str(pk))
    timeout = float(cfg.get("pdf_head_timeout", 0.8))
    try:
        if _head_pdf_ok(dl, cfg["headers"], analyst_key, timeout):
            return dl
    except Exception as exc:
        print(f"[heungkuk] WARN: formula PDF HEAD failed view_key={view_key}: {exc}", file=sys.stderr)

    if not cfg.get("enable_pdf_probe", False):
        print(f"[heungkuk] WARN: unresolved PDF; use article fallback view_key={view_key}", file=sys.stderr)
        return None

    max_delta = int(cfg.get("max_pdf_probe_delta", 3))
    probe_timeout = float(cfg.get("pdf_probe_timeout", 0.5))
    for delta in range(1, max_delta + 1):
        for sign in [1, -1]:
            candidate = pk + (delta * sign)
            dl_cand = cfg["download_tpl"].replace("{base}", base).replace("{pdf_key}", str(candidate))
            try:
                if _head_pdf_ok(dl_cand, cfg["headers"], analyst_key, probe_timeout):
                    return dl_cand
            except Exception:
                pass

    print(
        f"[heungkuk] WARN: unresolved PDF after bounded probe; use article fallback view_key={view_key} max_delta={max_delta}",
        file=sys.stderr,
    )
    return None


def scrape_heungkuk(cfg: dict) -> list[dict]:
    cfg = normalize_cfg(cfg, firm_key="Heungkuk")
    cfg = {
        "headers": {"User-Agent": "Mozilla/5.0"},
        "board_pattern": r"/research/([^/]+)/list\.do",
        "table_sel": "table.data_list_x tbody tr",
        "link_sel": "td.left a",
        "onclick_pattern": r"key=(\d+)",
        "pdf_formula": "2 * {view_key} - 12059",
        "download_tpl": "{base}/download.do?type=Board&key={pdf_key}",
        "view_tpl": "{base}/research/{board_path}/view.do?key={view_key}",
        "pdf_head_timeout": 0.8,
        "pdf_probe_timeout": 0.5,
        "max_pdf_probe_delta": 15,
        "enable_pdf_probe": True,
        "firm_id": 28,
        "firm_nm": "흥국증권",
        **cfg,
    }
    # 2026.06.24: PDF key 공식 시프트 (12059→12059). GA secret override 방지.
    cfg["pdf_formula"] = "2 * {view_key} - 12059"
    requests.packages.urllib3.disable_warnings()
    result = []
    for board_order, list_url in enumerate(cfg.get("urls",[cfg.get("url","")])):
        if not list_url: continue
        try:
            sess = requests.Session()
            sess.headers.update(cfg["headers"])
            resp = sess.get(list_url, timeout=20, verify=False)
            resp.raise_for_status()
            resp.encoding = cfg.get("encoding","euc-kr")
        except Exception: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        base = list_url.split("/research/")[0]
        board_pat = cfg["board_pattern"].replace("\\\\","\\")
        bm = re.search(board_pat, list_url)
        bp = bm.group(1) if bm else "company"
        for tr in soup.select(cfg["table_sel"]):
            a = tr.select_one(cfg["link_sel"])
            if not a: continue
            onclick = a.get("onclick","")
            pat = cfg["onclick_pattern"].replace("\\\\","\\")
            km = re.search(pat, onclick)
            if not km:
                km = re.search(r"key=(\d+)", onclick)
            if not km: continue
            vk = int(km.group(1))
            title = re.sub(r"\s+"," ", a.get_text(" ",strip=True))
            cells = tr.find_all("td")
            if len(cells) < 5: continue
            writer = re.sub(r"\s+"," ",cells[2].get_text(" ",strip=True))
            analyst_key = ""
            analyst_link = cells[2].find("a")
            if analyst_link:
                am = re.search(r"key=(\d+)", analyst_link.get("href", ""))
                if am:
                    analyst_key = am.group(1)
            rd = _norm_date(cells[3].get_text(" ",strip=True))
            dl = _resolve_pdf_download(base, vk, analyst_key, cfg)
            au = cfg["view_tpl"].replace("{base}",base).replace("{board_path}",bp).replace("{view_key}",str(vk))
            telegram_url = dl or au
            download_url = dl or ""
            pdf_url = dl or ""
            result.append(dict(firm_id=cfg["firm_id"],board_id=board_order,
                firm_nm=cfg["firm_nm"],report_date=rd,telegram_url=telegram_url,pdf_file_url=pdf_url,
                article_title=title,source_url=au,writer=writer,report_unique_key=au,
                save_at=datetime.now(timezone(timedelta(hours=9))).isoformat()))
    print(f"[heungkuk] {len(result)} articles collected (pre-duplicate-guard)", file=sys.stderr)
    result = _filter_duplicate_pdf_rows(result)
    return result
