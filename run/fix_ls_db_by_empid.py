"""
LS증권 upload/ fallback URL 전체 복구 스크립트

전략:
1. DB에서 msg.ls-sec.co.kr URL → writer→사번 매핑 구축
2. writer 있으면 바로 사번 조회
   writer 없거나 매핑 미스 → detail 페이지(key 컬럼) 크롤링 → '필명' 파싱
3. K_{reg_dt}_{emp_id}_{seq}.pdf 병렬 HEAD 프로빙
4. Content-Length 1차 필터 → MD5 hash 검증
5. telegram_url / pdf_url / pdf_hash 모두 static URL로 DB 업데이트
"""
import asyncio
import sys
import os
import re
import hashlib
import time
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from loguru import logger
from datetime import datetime, timedelta
from collections import defaultdict

urllib3.disable_warnings()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.db_factory import get_db

SOCKS_PROXY = os.getenv("SOCKS_PROXY_URL", "socks5h://localhost:9091")
PROXIES     = {'http': SOCKS_PROXY, 'https': SOCKS_PROXY}
MSG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://msg.ls-sec.co.kr/",
}
LS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.ls-sec.co.kr/",
}

MSG_BASE        = "https://msg.ls-sec.co.kr/eum/K_{date}_{emp_id}_{seq}.pdf"
DAILY_RATE      = {"com": 8, "jhsung": 2}
DEFAULT_RATE    = 3
PROBE_WORKERS   = 10
DETAIL_WORKERS  = 3
HEAD_TIMEOUT    = 5
GET_TIMEOUT     = 20
DETAIL_TIMEOUT  = 15


# ── HTTP 헬퍼 ──────────────────────────────────────────────────────────────

def head_size(url: str) -> int | None:
    try:
        r = requests.head(url, headers=MSG_HEADERS, proxies=PROXIES,
                          verify=False, timeout=HEAD_TIMEOUT)
        if r.status_code == 200:
            cl = r.headers.get("Content-Length")
            return int(cl) if cl else -1
    except Exception:
        pass
    return None


def fetch_hash_size(url: str, headers=None) -> tuple[str | None, int | None]:
    try:
        r = requests.get(url, headers=headers or MSG_HEADERS, proxies=PROXIES,
                         verify=False, timeout=GET_TIMEOUT)
        if r.status_code == 200:
            return hashlib.md5(r.content).hexdigest(), len(r.content)
    except Exception:
        pass
    return None, None


def parallel_head(urls: list[str]) -> dict[str, int | None]:
    with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as pool:
        futs = {pool.submit(head_size, u): u for u in urls}
        return {futs[f]: f.result() for f in as_completed(futs)}


def fetch_writer_from_detail(key_url: str) -> str | None:
    """detail 페이지에서 '필명' 파싱."""
    try:
        r = requests.get(key_url, headers=LS_HEADERS, proxies=PROXIES,
                         verify=False, timeout=DETAIL_TIMEOUT)
        soup = BeautifulSoup(r.content, "html.parser")
        for tr in soup.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td and th.get_text(strip=True) == "필명":
                writer = td.get_text(strip=True)
                return writer if writer else None
    except Exception:
        pass
    return None


# ── 보조 함수 ──────────────────────────────────────────────────────────────

def date_diff_days(dt1: str, dt2: str) -> int:
    try:
        return abs((datetime.strptime(dt1, "%Y%m%d") -
                    datetime.strptime(dt2, "%Y%m%d")).days)
    except Exception:
        return 999


def estimate_seq(reg_dt: str, history: list) -> tuple[int, int]:
    """
    history = [(reg_dt, emp_id, seq), ...] 전체 이력에서
    target reg_dt의 예상 seq와 spread 반환.
    두 참조점 사이면 선형 보간, 밖이면 속도 기반 외삽.
    """
    sorted_h = sorted(history, key=lambda x: x[0])
    before = [(dt, sq) for dt, _, sq in sorted_h if dt <= reg_dt]
    after  = [(dt, sq) for dt, _, sq in sorted_h if dt >  reg_dt]

    if before and after:
        dt1, sq1 = before[-1]
        dt2, sq2 = after[0]
        d1 = date_diff_days(reg_dt, dt1)
        d2 = date_diff_days(reg_dt, dt2)
        total = max(d1 + d2, 1)
        est = int(sq1 + (sq2 - sq1) * d1 / total)
        # 보간 구간의 일평균 변화량 기준 마진
        daily = abs(sq2 - sq1) / total
        spread = max(8, int(daily * max(d1, d2) * 0.2) + 10)
    elif before:
        # 가장 최근 두 점으로 속도 추정 후 외삽
        dt1, sq1 = before[-1]
        d = date_diff_days(reg_dt, dt1)
        if len(before) >= 2:
            dt0, sq0 = before[-2]
            daily = abs(sq1 - sq0) / max(date_diff_days(dt1, dt0), 1)
        else:
            daily = DAILY_RATE.get("", DEFAULT_RATE)
        est    = int(sq1 + daily * d)
        spread = max(8, int(daily * d * 0.2) + 10)
    elif after:
        dt2, sq2 = after[0]
        d = date_diff_days(reg_dt, dt2)
        if len(after) >= 2:
            dt3, sq3 = after[1]
            daily = abs(sq3 - sq2) / max(date_diff_days(dt3, dt2), 1)
        else:
            daily = DAILY_RATE.get("", DEFAULT_RATE)
        est    = max(1, int(sq2 - daily * d))
        spread = max(8, int(daily * d * 0.2) + 10)
    else:
        est, spread = history[0][2], 15

    return est, spread


def build_probe_urls(reg_dt: str, emp_id: str, history: list) -> tuple[list[str], int]:
    """선형 보간으로 예상 순번 계산 후 probe URL 목록 반환. est_seq도 반환."""
    est_seq, spread = estimate_seq(reg_dt, history)
    seqs = sorted(range(max(1, est_seq - spread), est_seq + spread + 1),
                  key=lambda x: abs(x - est_seq))
    urls = []
    for delta in (0, -1, 1):
        try:
            d = (datetime.strptime(reg_dt, "%Y%m%d") +
                 timedelta(days=delta)).strftime("%Y%m%d")
        except Exception:
            continue
        urls += [MSG_BASE.format(date=d, emp_id=emp_id, seq=s) for s in seqs]
    return urls, est_seq


def resolve_emp(writer: str | None, writer_history: dict) -> tuple[str, int, str] | None:
    """writer → (ref_dt, emp_id, ref_seq) 반환. 없으면 None."""
    if not writer or writer not in writer_history:
        return None
    return writer_history[writer][0]   # 이미 reg_dt 기준 정렬됨


# ── 메인 ──────────────────────────────────────────────────────────────────

async def fix_all():
    log_dir = os.getenv("LOG_DIR", "/home/ubuntu/log")
    os.makedirs(log_dir, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(os.path.join(log_dir, "fix_ls_db_empid.log"), level="DEBUG",
               rotation="10 MB", retention="14 days", encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    db = get_db()

    # 1. writer→사번 매핑 (reg_dt 가까운 순으로 미리 정렬)
    emp_rows = await db.execute_query("""
        SELECT writer,
               regexp_replace(telegram_url, '.*K_\\d{8}_([^_]+)_(\\d+)\\.pdf.*', '\\1') AS emp_id,
               regexp_replace(telegram_url, '.*K_\\d{8}_([^_]+)_(\\d+)\\.pdf.*', '\\2')::int AS seq,
               reg_dt
        FROM tbl_sec_reports
        WHERE firm_nm = 'LS증권'
          AND telegram_url ~ 'K_\\d{8}_[^_]+_\\d+\\.pdf'
          AND writer IS NOT NULL AND writer <> ''
        ORDER BY writer, reg_dt
    """)
    # writer → [(reg_dt, emp_id, seq)] — reg_dt 정렬은 나중에 per-record로
    raw_history: dict[str, list] = defaultdict(list)
    for r in emp_rows:
        raw_history[r["writer"]].append((r["reg_dt"], r["emp_id"], r["seq"]))
    logger.info(f"writer→사번 매핑: {len(raw_history)}명")

    # 2. 수리 대상 전체 (writer 여부 무관)
    targets = await db.execute_query("""
        SELECT report_id, reg_dt, writer, article_title,
               telegram_url, pdf_url, pdf_hash, key
        FROM tbl_sec_reports
        WHERE firm_nm = 'LS증권'
          AND (telegram_url LIKE 'https://www.ls-sec.co.kr/upload/%'
            OR pdf_url LIKE 'https://www.ls-sec.co.kr/upload/%')
        ORDER BY reg_dt ASC
    """)
    logger.info(f"수리 대상: {len(targets)}건")

    updated = skip_no_emp = skip_probe = skip_hash = 0
    detail_cache: dict[str, str | None] = {}   # key_url → writer

    for idx, art in enumerate(targets, 1):
        writer  = art.get("writer") or ""
        reg_dt  = art["reg_dt"]
        rid     = art["report_id"]
        old_url = art["telegram_url"] or art["pdf_url"] or ""
        db_hash = art.get("pdf_hash")
        key_url = art.get("key") or ""

        # 2a. writer로 전체 이력 조회
        history = raw_history.get(writer, [])

        # 2b. 매핑 실패 → detail 페이지에서 필명 파싱
        if not history and key_url:
            if key_url not in detail_cache:
                fetched = fetch_writer_from_detail(key_url)
                detail_cache[key_url] = fetched
                if fetched:
                    logger.debug(f"  detail 파싱 writer: {fetched}")
                time.sleep(0.5)
            fetched_writer = detail_cache.get(key_url)
            if fetched_writer:
                history = raw_history.get(fetched_writer, [])
                writer = fetched_writer

        if not history:
            logger.debug(f"[{idx}] SKIP-no-emp: {writer or '(없음)'} / "
                         f"{art['article_title'][:40]}")
            skip_no_emp += 1
            continue

        emp_id    = history[0][1]
        est_seq, spread = estimate_seq(reg_dt, history)
        probe_urls, est_seq = build_probe_urls(reg_dt, emp_id, history)

        logger.info(f"[{idx}/{len(targets)}] {writer}({emp_id}) "
                    f"{reg_dt} est=#{est_seq}±{spread} HEAD {len(probe_urls)}개")

        # 3. 병렬 HEAD
        head_res  = parallel_head(probe_urls)
        hit_urls  = {u: sz for u, sz in head_res.items() if sz is not None}

        if not hit_urls:
            logger.warning(f"  [FAIL] 200 없음: {art['article_title'][:50]}")
            skip_probe += 1
            continue

        # 4a. 원본 hash 확보
        if not db_hash and old_url and "upload" in old_url:
            db_hash, orig_size = fetch_hash_size(old_url, headers=LS_HEADERS)
            logger.debug(f"  원본 hash={db_hash} size={orig_size}")
        else:
            orig_size = None

        # 4b. size 1차 필터
        if orig_size:
            size_ok = {u: sz for u, sz in hit_urls.items()
                       if sz == -1 or sz == orig_size}
            if not size_ok:
                logger.warning(f"  [SIZE-FILTER] 원본={orig_size}: "
                               f"{art['article_title'][:50]}")
                skip_hash += 1
                continue
            hit_urls = size_ok

        # 4c. hash 검증 (est_seq에 가까운 순서로 시도)
        def seq_of(u):
            m = re.search(r'_(\d+)\.pdf$', u)
            return int(m.group(1)) if m else 0

        found_url = found_hash = None
        sorted_hits = sorted(hit_urls, key=lambda u: abs(seq_of(u) - est_seq))

        if db_hash:
            for url in sorted_hits:
                h, _ = fetch_hash_size(url)
                if h == db_hash:
                    found_url, found_hash = url, h
                    logger.success(f"  ✓ hash 일치: {url}")
                    break
                logger.debug(f"  hash 불일치: {url}")
            if not found_url:
                logger.warning(f"  [HASH-MISMATCH] {art['article_title'][:50]}")
                skip_hash += 1
                continue
        else:
            # hash 없음 → est_seq에 가장 가까운 단일 후보 신뢰
            found_url = sorted_hits[0]
            found_hash, _ = fetch_hash_size(found_url)
            if len(hit_urls) > 1:
                logger.warning(f"  [NO-HASH] 최근접 후보 선택(총{len(hit_urls)}개): {found_url}")
            else:
                logger.warning(f"  [NO-HASH] 단일 후보 신뢰: {found_url}")

        # 5. DB 업데이트
        ok = await db.update_telegram_url(
            record_id=rid,
            telegram_url=found_url,
            article_title=art["article_title"],
            pdf_url=found_url,
            pdf_hash=found_hash,
        )
        if ok:
            updated += 1
            logger.info(f"  ✓ DB 업데이트: report_id={rid}")
        else:
            logger.error(f"  DB 실패: report_id={rid}")

        await asyncio.sleep(0.1)

    logger.info("=" * 60)
    logger.info(f"완료: 복구={updated} / 사번없음={skip_no_emp} / "
                f"탐색실패={skip_probe} / hash불일치={skip_hash}")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(fix_all())
