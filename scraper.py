# -*- coding:utf-8 -*-
import os
import sys
import asyncio
import argparse
import datetime
from loguru import logger
from dotenv import load_dotenv

# 공통 로그 설정 적용
from utils.logger_util import setup_logger
setup_logger("scraper")

# --- 모듈 임포트 ---
from utils.telegram_util import sendMarkDownText
from utils.telegram_message_builder import build_telegram_message_chunks
from models.db_factory import get_db

# scraper configuration (env vars, timeouts, constants)
from scraper_config import (
    SCRAPER_STALE_DAYS, SCRAPER_SYNC_TIMEOUT_SECONDS, SCRAPER_ASYNC_TIMEOUT_SECONDS,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    BLOCKED_BY_SOURCE_IP, KNOWN_EXTERNAL_ERRORS,
    FIRM_ID_LS, FIRM_ID_DS, FIRM_ID_DBFI,
    FULL_SCRAPE_HOURS, STALE_OVERRIDES,
)

# business modules
from modules.LS_0 import LS_enrich
from modules.ShinHanInvest_1 import ShinHanInvest_checkNewArticle
from modules.NHQV_2 import NHQV_checkNewArticle               # GA 이관됨 (full-scrape fallback)
from modules.HANA_3 import HANA_checkNewArticle
from modules.KBsec_4 import KB_checkNewArticle                 # GA 이관됨 (full-scrape fallback)
from modules.Samsung_5 import Samsung_checkNewArticle          # GA 이관됨 (full-scrape fallback)
from modules.Sangsanginib_6 import Sangsanginib_checkNewArticle # GA 이관됨 (full-scrape fallback)
from modules.Shinyoung_7 import Shinyoung_checkNewArticle
from modules.Miraeasset_8 import Miraeasset_checkNewArticle
from modules.Hmsec_9 import Hmsec_checkNewArticle
from modules.Kiwoom_10 import Kiwoom_checkNewArticle           # GA 이관됨 (full-scrape fallback)
from modules.DS_11 import DS_checkNewArticle
from modules.eugenefn_12 import eugene_checkNewArticle
from modules.Koreainvestment_13 import Koreainvestment_selenium_checkNewArticle
from modules.DAOL_14 import DAOL_checkNewArticle
from modules.TOSSinvest_15 import TOSSinvest_checkNewArticle   # GA 이관됨 (full-scrape fallback)
from modules.Leading_16 import Leading_checkNewArticle          # GA 이관됨 (full-scrape fallback)
from modules.Daeshin_17 import Daeshin_checkNewArticle
from modules.iMfnsec_18 import iMfnsec_checkNewArticle
from modules.DBfi_19 import DBfi_checkNewArticle, DBfi_enrich
from modules.MERITZ_20 import MERITZ_checkNewArticle
from modules.Hanwhawm_21 import Hanwha_checkNewArticle         # GA 이관됨 (full-scrape fallback)
from modules.Hygood_22 import Hanyang_checkNewArticle           # GA 이관됨 (full-scrape fallback)
from modules.BNKfn_23 import BNK_checkNewArticle
from modules.Kyobo_24 import Kyobo_checkNewArticle
from modules.IBKs_25 import IBK_checkNewArticle
from modules.SKS_26 import Sks_checkNewArticle
from modules.Yuanta_27 import Yuanta_checkNewArticle
from modules.Heungkuk_28 import Heungkuk_checkNewArticle       # GA 이관됨 (full-scrape fallback)

load_dotenv()
token = TELEGRAM_BOT_TOKEN
chat_id = TELEGRAM_CHAT_ID
SCRAPER_HEALTH_ERRORS = []

def _is_external_error(msg: str) -> bool:
    """외부 네트워크/차단 이슈로 인한 에러인지 확인 (watchdog 알람 제외 대상)"""
    import re
    return any(re.search(pattern, msg) for pattern in KNOWN_EXTERNAL_ERRORS)

# GA 이관 증권사 — 평시(30분 간격)에는 GA standalone이 처리, 서버는 full-scrape 시간대에만 fallback 실행
# {firm_id: func} 매핑. PostgreSQL tbm_sec_firm_info.ga_enabled_yn='Y' 여부로 필터링.
_GA_FIRMS_SYNC = {
    5: Samsung_checkNewArticle,       # 삼성증권
    9: Hmsec_checkNewArticle,         # 현대차증권
    15: TOSSinvest_checkNewArticle,    # 토스증권
    28: Heungkuk_checkNewArticle,     # 흥국증권
    # 아래 firm들은 GA standalone secret은 full_config지만, 서버 ConfigManager legacy secrets는
    # URL list만 제공한다. 서버 full-scrape fallback에서 실행하면 health error로 scheduler가
    # exit 1이 되므로 server config 정규화 전까지 GA 전용으로 둔다.
    # 8: Miraeasset_checkNewArticle,  # 미래에셋증권: row_sel 필요
    # 26: Sks_checkNewArticle,        # SK증권: list input 방어 전까지 제외
}

_GA_FIRMS_ASYNC = {
    2: NHQV_checkNewArticle,          # NH투자증권
    # 하나증권(firm_id=3)은 GA 러너 IP가 www.hanaw.com에서 차단되어
    # GA로는 수집 불가. 서버 전용 regular async scraper list에서 직접 호출한다.
    # _GA_FIRMS_ASYNC에 넣으면 ga_enabled_yn='N'일 때 full-scrape에서도 제외되어
    # 완전히 누락되므로 절대 GA fallback 목록에 포함시키지 않는다.
    # 3: HANA_checkNewArticle,          # 하나증권 — 서버 전용
    4: KB_checkNewArticle,            # KB증권
    6: Sangsanginib_checkNewArticle,  # 상상인증권
    # IM증권은 현재 사이트 측 응답/수집 불가로 로컬 full-scrape fallback에서도 제외한다.
    # 수동 재검증은 run/standalone/imfn.py 또는 scrape-imfn workflow_dispatch로 수행.
    # 18: iMfnsec_checkNewArticle,     # IM증권
    19: DBfi_checkNewArticle,          # DB증권
    20: MERITZ_checkNewArticle,        # 메리츠증권
    24: Kyobo_checkNewArticle,         # 교보증권
    25: IBK_checkNewArticle,           # IBK투자증권
    27: Yuanta_checkNewArticle,        # 유안타증권
    # 서버 legacy secrets가 URL list만 제공해 full_config 기반 core에서 오류/무효 reg_dt를 낸다.
    # GA standalone은 유지하고, 서버 fallback은 config 정규화 후 재활성화한다.
    # 10: Kiwoom_checkNewArticle,      # 키움증권: payload 필요
    # 14: DAOL_checkNewArticle,        # 다올투자증권: path_tpl 필요
    # 16: Leading_checkNewArticle,     # 리딩투자증권: reg_dt 미검출
    21: Hanwha_checkNewArticle,        # 한화투자증권
    # 22: Hanyang_checkNewArticle,     # 한양증권: reg_dt 미검출
}



_ENRICHERS = {
    FIRM_ID_LS: LS_enrich,
    FIRM_ID_DBFI: DBfi_enrich,
}
_ENRICHMENT_SKIP_FIRM_IDS = frozenset({FIRM_ID_DS})


def _filter_ga_enabled(mapping: dict) -> dict:
    """PostgreSQL tbm_sec_firm_info.ga_enabled_yn='Y'인 firm만 반환.
    메타데이터 로드 실패, static fallback, 또는 예외 발생 시
    전체 mapping 반환 — GA fallback이 완전히 죽지 않도록 보호."""
    try:
        from models.firm_utils import ga_enabled_orders
        enabled = ga_enabled_orders()
        if enabled is None:
            logger.info("GA metadata unavailable (not PostgreSQL), falling back to all candidates")
            return dict(mapping)
        return {order: func for order, func in mapping.items() if order in enabled}
    except Exception:
        logger.warning("ga_enabled lookup failed, falling back to all GA candidates")
        return dict(mapping)

def _is_full_scrape_hour():
    """현재 KST 시각이 full-scrape 시간대(1,7,13,21시)인지 반환"""
    try:
        import pytz
        kst_now = datetime.datetime.now(pytz.timezone("Asia/Seoul"))
        return kst_now.hour in FULL_SCRAPE_HOURS
    except ImportError:
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        kst_hour = (utc_now.hour + 9) % 24
        return kst_hour in FULL_SCRAPE_HOURS


def _regular_sync_functions():
    return [
        Shinyoung_checkNewArticle, DS_checkNewArticle
    ]


def _regular_async_functions():
    return [
        ShinHanInvest_checkNewArticle,
        Koreainvestment_selenium_checkNewArticle,
        Daeshin_checkNewArticle,
        HANA_checkNewArticle,               # 하나증권 — 서버 전용 (GA IP 차단)
        # BNK is temporarily disabled in local scheduler (2026-06-26).
        # Server and GA IP currently blocked by source. Keep module for manual diagnostics.
        # Re-enable only after source access is confirmed.
        # BNK_checkNewArticle,
        # DAOL, iMfnsec, MERITZ → GA standalone
        # eugene_checkNewArticle # 세션 만료 및 제한 에러 (보류)
    ]


def log_scraper_health(name, rows):
    if not isinstance(rows, list):
        msg = f"{name} returned non-list result: {type(rows)}"
        SCRAPER_HEALTH_ERRORS.append(msg)
        logger.error(msg)
        return

    if not rows:
        msg = f"{name} returned 0 articles. Check source API, selector, or credentials."
        logger.warning(msg)  # 0건 수집 → 알람 노이즈 방지 (구조 변경 or IP 차단 등 외부 이슈일 가능성)
        return

    reg_dates = sorted({
        str(row.get("report_date") or row.get("reg_dt", ""))[:8]
        for row in rows
        if row.get("report_date") or row.get("reg_dt")
    })
    if not reg_dates:
        msg = f"{name} returned {len(rows)} articles but no reg_dt values."
        SCRAPER_HEALTH_ERRORS.append(msg)
        logger.error(msg)
        return

    min_reg_dt = reg_dates[0]
    max_reg_dt = reg_dates[-1]
    logger.info(f"{name} => Found {len(rows)} articles (reg_dt {min_reg_dt}~{max_reg_dt})")

    try:
        max_date = datetime.datetime.strptime(max_reg_dt, "%Y%m%d").date()
        stale_days = STALE_OVERRIDES.get(name, SCRAPER_STALE_DAYS)
        stale_cutoff = datetime.datetime.now().date() - datetime.timedelta(days=stale_days)
        if max_date < stale_cutoff:
            msg = (
                f"{name} latest reg_dt is stale: {max_reg_dt} "
                f"(older than {stale_days} days)"
            )
            SCRAPER_HEALTH_ERRORS.append(msg)
            logger.error(msg)
    except ValueError:
        msg = f"{name} returned invalid max reg_dt: {max_reg_dt}"
        SCRAPER_HEALTH_ERRORS.append(msg)
        logger.error(msg)


async def enrich_data():
    logger.info("Starting data enrichment process...")
    db = get_db()
    from models.firm_utils import all_firm_names, firm_name as _firm_name, telegram_update_required
    import pytz
    from datetime import datetime
    kst_hour = datetime.now(pytz.timezone('Asia/Seoul')).hour
    is_idle_time = kst_hour >= 20 or kst_hour < 6

    for firm_id in range(len(all_firm_names())):
        name = _firm_name(firm_id)
        if not (name and telegram_update_required(firm_id)):
            continue

        # FirmInfo 인스턴스는 db.fetch_all_empty_telegram_url_articles에 필요
        from models.FirmInfo import FirmInfo
        firm_info = FirmInfo(firm_id=firm_id, board_id=0)
        enrichment_targets = await db.fetch_all_empty_telegram_url_articles(firm_info=firm_info, days_limit=3)
        if not enrichment_targets:
            continue

        logger.info(f"[{name}] Found {len(enrichment_targets)} records for enrichment (최근 3일).")
        try:
            enricher = _ENRICHERS.get(firm_id)
            if enricher:
                await enricher(db, enrichment_targets, firm_info, is_idle_time)
            elif firm_id in _ENRICHMENT_SKIP_FIRM_IDS:
                pass
            logger.success(f"[{name}] Enrichment completed.")
        except Exception as e:
            logger.error(f"[{name}] Enrichment failed: {e}")


async def daily_send_report(date_str=None):
    db = get_db()
    rows = await db.select_reports_ready_for_telegram(date_str=date_str, type='send')
    if not rows:
        logger.info("No reports ready for Telegram send.")
        return

    report_ids = [r.get("report_id") for r in rows if r.get("report_id")]
    logger.info(f"Send candidates: {len(rows)} reports, report_ids={report_ids[:5]}...")

    chunks = build_telegram_message_chunks(rows)
    logger.info(f"Sending {len(chunks)} message chunks...")

    sent_count = 0
    failed_count = 0
    for i, chunk in enumerate(chunks, start=1):
        msg = chunk["message"]
        chunk_rows = [r for r in chunk["rows"] if r.get("report_id")]
        chunk_ids = [r["report_id"] for r in chunk_rows]
        logger.info(
            f"Telegram chunk {i}/{len(chunks)} candidates={len(chunk_rows)} "
            f"report_ids={chunk_ids[:5]}"
        )
        try:
            await sendMarkDownText(token=token, chat_id=chat_id, sendMessageText=msg)
            if chunk_rows:
                await db.daily_update_data(fetched_rows=chunk_rows, type='send')
                sent_count += len(chunk_rows)
            logger.success(f"Telegram chunk {i}/{len(chunks)} sent and marked: {len(chunk_rows)} reports")
        except Exception as e:
            failed_count += len(chunk_rows)
            logger.error(
                f"Telegram chunk {i}/{len(chunks)} failed; "
                f"not marking {len(chunk_rows)} reports sent: {e}"
            )

    logger.info(f"Daily report send complete: sent_marked={sent_count}, unmarked_failed={failed_count}")

async def run_sync_scrapers(sync_scraper_funcs, scraped_reports):
    for scraper_func in sync_scraper_funcs:
        try:
            logger.info(f"Scraping (Sync): {scraper_func.__name__}")
            scraper_result = await asyncio.wait_for(
                asyncio.to_thread(scraper_func),
                timeout=SCRAPER_SYNC_TIMEOUT_SECONDS,
            )
            if scraper_result:
                scraped_reports.extend(scraper_result)
            log_scraper_health(scraper_func.__name__, scraper_result)
            await asyncio.sleep(1)
        except asyncio.TimeoutError:
            msg = f"Sync Scraper Timeout ({scraper_func.__name__}): {SCRAPER_SYNC_TIMEOUT_SECONDS}s"
            logger.warning(msg)  # 외부 네트워크 이슈 → ERROR 아님
        except Exception as e:
            msg = f"Sync Scraper Error ({scraper_func.__name__}): {e}"
            if _is_external_error(str(e)):
                logger.warning(msg)
            else:
                SCRAPER_HEALTH_ERRORS.append(msg)
            logger.error(msg)


async def call_async_scraper(func):
    """2026-06-11 fix: iscoroutinefunction으로 호출 전 판별 → sync 함수가 이벤트루프 블로킹 방지"""
    import inspect
    name = func.__name__
    try:
        if inspect.iscoroutinefunction(func):
            scraper_result = await asyncio.wait_for(func(), timeout=SCRAPER_ASYNC_TIMEOUT_SECONDS)
        else:
            # sync 함수가 async 리스트에 잘못 들어온 경우 → to_thread로 안전하게
            scraper_result = await asyncio.to_thread(func)
        return name, scraper_result, None
    except asyncio.TimeoutError:
        return name, None, f"Async Scraper Timeout ({name}): {SCRAPER_ASYNC_TIMEOUT_SECONDS}s"
    except Exception as e:
        return name, None, f"Async Scraper Error ({name}): {e}"


async def run_async_scrapers(async_scraper_funcs, scraped_reports, max_concurrency=3):
    logger.info(f"Launching {len(async_scraper_funcs)} async scrapers (max concurrency: {max_concurrency})...")
    sem = asyncio.Semaphore(max_concurrency)
    
    async def sem_call(scraper_func):
        async with sem:
            return await call_async_scraper(scraper_func)
            
    tasks = []
    
    for scraper_func in async_scraper_funcs:
        if not callable(scraper_func):
            continue
        tasks.append(sem_call(scraper_func))

    if not tasks:
        return

    logger.debug(f"Gathering {len(tasks)} scraper tasks with Semaphore")
    results = await asyncio.gather(*tasks)
    for name, scraper_result, error in results:
        if error:
            if _is_external_error(str(error)):
                logger.warning(f"{name}: {error}")  # 외부 네트워크 이슈 → WARNING
            else:
                SCRAPER_HEALTH_ERRORS.append(error)
                logger.error(error)
        elif isinstance(scraper_result, list):
            scraped_reports.extend(scraper_result)
            log_scraper_health(name, scraper_result)
        elif scraper_result is not None:
            msg = f"{name} returned non-list result: {type(scraper_result)}"
            SCRAPER_HEALTH_ERRORS.append(msg)
            logger.error(msg)


def normalize_scraped_report_payloads(scraped_reports):
    import html as _html
    import re as _re

    for report_payload in scraped_reports:
        title = report_payload.get("article_title")
        if title:
            if any(entity in title for entity in ("&amp;", "&lt;", "&gt;", "&quot;")):
                report_payload["article_title"] = _html.unescape(title)
            market_type = report_payload.get("mkt_tp", "")
            if market_type in ("GLOBAL", "global", "US", "JP"):
                if _re.search(r"\([0-9]{5,6}\.K[QS]\)", title) or _re.search(r"코스피|코스닥|국내", title):
                    report_payload["mkt_tp"] = "KR"


def dedupe_reports_by_unique_key(scraped_reports):
    reports_by_unique_key = {}
    for report_payload in scraped_reports:
        unique_key = report_payload.get("report_unique_key") or report_payload.get("key") or report_payload.get("article_url")
        if unique_key:
            report_payload["report_unique_key"] = unique_key
            reports_by_unique_key[unique_key] = report_payload
    return reports_by_unique_key


async def insert_scraped_reports(db, reports, label="DB"):
    inserted, updated = db.insert_json_data_list(list(reports))
    logger.success(f"[{label}] DB Sync: {inserted} new, {updated} updated.")
    await asyncio.sleep(1)
    return inserted, updated


async def sync_scraped_reports_to_db(db, scraped_reports, label="DB"):
    if not scraped_reports:
        return 0, 0

    normalize_scraped_report_payloads(scraped_reports)
    reports_by_unique_key = dedupe_reports_by_unique_key(scraped_reports)
    scraped_reports.clear()

    if not reports_by_unique_key:
        logger.warning(f"[{label}] No reports with a usable unique key.")
        return 0, 0

    return await insert_scraped_reports(db, reports_by_unique_key.values(), label=label)


def build_scraper_function_lists(is_full):
    if is_full:
        logger.info("⏰ FULL-SCRAPE MODE: KST {1,7,13,21}시 — GA 이관 증권사 포함 전체 29개사 스크래핑")
    else:
        logger.info("📡 REGULAR MODE: GA 미이관 증권사만 스크래핑 (GA standalone이 처리)")

    sync_scraper_funcs = _regular_sync_functions()
    async_scraper_funcs = _regular_async_functions()

    # BNKfn_23: BLOCKED_BY_SOURCE_IP — GA & server IP 모두 차단됨.
    # Parser rewrite로 해결 불가. 재활성화는 source IP 변경 후에만 고려.
    # Previously gated by SKIP_BNK env; now permanently excluded.
    if os.getenv("SKIP_BNK", "").lower() in ("1", "true", "yes"):
        logger.info("[Local] SKIP_BNK env set (BNK already BLOCKED_BY_SOURCE_IP).")

    if is_full:
        sync_scraper_funcs.extend(_filter_ga_enabled(_GA_FIRMS_SYNC).values())
        async_scraper_funcs.extend(_filter_ga_enabled(_GA_FIRMS_ASYNC).values())
        logger.info(f"[Full-Scrape] sync={len(sync_scraper_funcs)}, async={len(async_scraper_funcs)} total={len(sync_scraper_funcs) + len(async_scraper_funcs)}")

    return sync_scraper_funcs, async_scraper_funcs


async def run_scraper_batches(db, sync_scraper_funcs, async_scraper_funcs):
    scraped_reports = []

    await run_sync_scrapers(sync_scraper_funcs, scraped_reports)
    try:
        await sync_scraped_reports_to_db(db, scraped_reports, label="Sync scrapers")
    except Exception as e:
        logger.error(f"[Sync scrapers] DB error: {e}")

    await run_async_scrapers(async_scraper_funcs, scraped_reports)

    if scraped_reports:
        try:
            await sync_scraped_reports_to_db(db, scraped_reports, label="Async scrapers")
        except Exception as e:
            logger.error(f"[Async scrapers] DB error: {e}")


async def main(date_str=None):
    logger.info("=================== SCRAPER START ===================")
    db = get_db()

    sync_scraper_funcs, async_scraper_funcs = build_scraper_function_lists(
        is_full=_is_full_scrape_hour()
    )

    await run_scraper_batches(db, sync_scraper_funcs, async_scraper_funcs)

    await enrich_data()
    
    # 발송 전에 DB 연결을 새로 하거나 세션을 확실히 분리하여 최신 데이터를 가져옴
    await daily_send_report(date_str=date_str)
    logger.info("=================== SCRAPER END =====================")
    if SCRAPER_HEALTH_ERRORS:
        joined = "; ".join(SCRAPER_HEALTH_ERRORS)
        raise RuntimeError(f"Scraper health check failed: {joined}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str, nargs='?', default=None)
    args = parser.parse_args()
    asyncio.run(main(date_str=args.date))
