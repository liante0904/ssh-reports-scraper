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
    LS_LIST_TIMEOUT_SECONDS, LS_DETAIL_TIMEOUT_SECONDS,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    BLOCKED_BY_SOURCE_IP, KNOWN_EXTERNAL_ERRORS,
    FIRM_ID_LS, FIRM_ID_DS, FIRM_ID_DBFI,
    FULL_SCRAPE_HOURS, STALE_OVERRIDES,
    invalidate_api_cache,
)

# firm registry — data-driven dispatch replaces all 29 individual module imports
from scraper_registry import (
    get_regular_sync_funcs, get_regular_async_funcs,
    get_ga_sync_mapping, get_ga_async_mapping,
    get_enricher, get_enrichment_skip_firm_ids,
    get_ls_module_func,
)

load_dotenv()
token = TELEGRAM_BOT_TOKEN
chat_id = TELEGRAM_CHAT_ID
SCRAPER_HEALTH_ERRORS = []

def _is_external_error(msg: str) -> bool:
    """외부 네트워크/차단 이슈로 인한 에러인지 확인 (watchdog 알람 제외 대상)"""
    import re
    return any(re.search(pattern, msg) for pattern in KNOWN_EXTERNAL_ERRORS)

# GA 이관 증권사 — 평시(30분 간격)에는 GA standalone이 처리, 서버는 full-scrape 시간대에만 fallback 실행.
# {firm_id: func} 매핑. PostgreSQL tbm_sec_firm_info.ga_enabled_yn='Y' 여부로 필터링.
# 포함/제외 사유는 config/firms.yaml의 ga_fallback_exclusion_reason 참조.
_GA_FIRMS_SYNC = get_ga_sync_mapping()
_GA_FIRMS_ASYNC = get_ga_async_mapping()



_ENRICHMENT_SKIP_FIRM_IDS = get_enrichment_skip_firm_ids()


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
    """Regular (non-GA) sync scraper functions — delegated to firm registry."""
    return get_regular_sync_funcs()


def _regular_async_functions():
    """Regular (non-GA) async scraper functions — delegated to firm registry."""
    return get_regular_async_funcs()


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

    report_dates = sorted({
        str(row.get("report_date", ""))[:8]
        for row in rows
        if row.get("report_date")
    })
    if not report_dates:
        msg = f"{name} returned {len(rows)} articles but no report_date values."
        SCRAPER_HEALTH_ERRORS.append(msg)
        logger.error(msg)
        return

    min_date = report_dates[0]
    max_date = report_dates[-1]
    logger.info(f"{name} => Found {len(rows)} articles (report_date {min_date}~{max_date})")

    try:
        max_date_obj = datetime.datetime.strptime(max_date, "%Y%m%d").date()
        stale_days = STALE_OVERRIDES.get(name, SCRAPER_STALE_DAYS)
        stale_cutoff = datetime.datetime.now().date() - datetime.timedelta(days=stale_days)
        if max_date_obj < stale_cutoff:
            msg = (
                f"{name} latest report_date is stale: {max_date} "
                f"(older than {stale_days} days)"
            )
            SCRAPER_HEALTH_ERRORS.append(msg)
            logger.error(msg)
    except ValueError:
        msg = f"{name} returned invalid max report_date: {max_date}"
        SCRAPER_HEALTH_ERRORS.append(msg)
        logger.error(msg)


async def enrich_data():
    logger.info("Starting data enrichment process...")
    db = get_db()
    from models.firm_utils import iter_active_firm_ids, firm_name as _firm_name, telegram_update_required
    import pytz
    from datetime import datetime
    kst_hour = datetime.now(pytz.timezone('Asia/Seoul')).hour
    is_idle_time = kst_hour >= 20 or kst_hour < 6

    for firm_id in iter_active_firm_ids():
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
            enricher = get_enricher(firm_id)
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
        logger.exception(f"Async scraper failure ({name})")
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
        unique_key = report_payload.get("report_unique_key") or report_payload.get("source_url")
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


async def run_ls_scraper(db):
    # LS flow: DB existing keys -> scrape list -> new articles only -> detail/enrich -> insert.
    if os.getenv("SKIP_LS", "").lower() in ("1", "true", "yes"):
        logger.warning("[LS] SKIP_LS enabled.")
        return

    LS_check = get_ls_module_func("LS_checkNewArticle")
    LS_detail_func = get_ls_module_func("LS_detail")
    if not LS_check or not LS_detail_func:
        logger.error("[LS] Failed to resolve LS functions from registry")
        return

    try:
        ls_articles = await asyncio.wait_for(
            asyncio.to_thread(LS_check),
            timeout=LS_LIST_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        msg = f"LS Scraper Timeout (LS_checkNewArticle): {LS_LIST_TIMEOUT_SECONDS}s"
        logger.warning(msg)
        return

    if not ls_articles:
        return

    logger.info(f"[LS] 신규 {len(ls_articles)}건 detail 추출 시작")
    try:
        enriched = await asyncio.wait_for(
            LS_detail_func(ls_articles, db=db),
            timeout=LS_DETAIL_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        enriched = ls_articles
        msg = f"LS Detail Timeout (LS_detail): {LS_DETAIL_TIMEOUT_SECONDS}s"
        logger.warning(msg)
        logger.warning("[LS] detail 타임아웃: 목록에서 확인한 신규 건은 URL 미해결 상태로 DB 저장 후 enrichment에서 재시도합니다.")

    resolved_count = sum(1 for article in enriched if article.get("telegram_url"))
    logger.success(f"[LS] {len(enriched)}건 detail 완료 (URL resolved={resolved_count})")
    try:
        ls_inserted, ls_updated = db.insert_json_data_list(enriched)
        logger.success(f"[LS] DB Sync: {ls_inserted} new, {ls_updated} updated.")
    except Exception as e:
        logger.error(f"[LS] DB error: {e}")


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

    await run_ls_scraper(db)

    sync_scraper_funcs, async_scraper_funcs = build_scraper_function_lists(
        is_full=_is_full_scrape_hour()
    )

    await run_scraper_batches(db, sync_scraper_funcs, async_scraper_funcs)

    await enrich_data()
    
    # 발송 전에 DB 연결을 새로 하거나 세션을 확실히 분리하여 최신 데이터를 가져옴
    await daily_send_report(date_str=date_str)
    invalidate_api_cache()
    logger.info("=================== SCRAPER END =====================")
    if SCRAPER_HEALTH_ERRORS:
        joined = "; ".join(SCRAPER_HEALTH_ERRORS)
        raise RuntimeError(f"Scraper health check failed: {joined}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str, nargs='?', default=None)
    args = parser.parse_args()
    asyncio.run(main(date_str=args.date))
