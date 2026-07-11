#!/usr/bin/env python3
"""
전체 증권사 Standalone Scraper — GitHub Actions 전용
======================================================
기존 scraper.py의 checkNewArticle 함수들을 그대로 재사용하여
전체 증권사 스크래핑을 GitHub Actions에서 수행.

- DB 접근 없이 순수 HTTP 크롤링 결과만 수집
- 각 증권사별 독립 실행 (한 곳 실패해도 나머지 진행)
- 결과는 JSON으로 stdout 출력 → artifact 업로드

서버 import 스크립트(import_all_artifact.py)가 JSON을 받아
DB insert (ON CONFLICT dedup) + 후처리를 담당한다.

기본 실행 대상:
  - IMPORT_MAP에 등록된 모든 standalone 대상
  - GA 운용 증권사도 포함한다. 장애/백필 시 로컬 전체 점검 경로를 유지하기 위함.

제외/제한:
  - --server-only: 예전 동작처럼 GA 운용 증권사를 제외
  - --skip-ls: LS증권 제외
  - 한국투자증권 (Koreainvestment_13): Selenium 필요

사용법:
  python3 scripts/standalone_all_scraper.py [--firms LS_0,KBsec_4] [--timeout 120]
  python3 scripts/standalone_all_scraper.py --server-only --skip-ls
"""

import argparse
import asyncio
import json
import multiprocessing
import os
import signal
import sys
import time
import traceback
from datetime import datetime

# ── 환경 설정 (DB write는 하지 않음) ──
os.environ.setdefault("DB_BACKEND", "static")
os.environ.setdefault("LOG_BASE_DIR", "/tmp")
os.environ.setdefault("SOCKS_PROXY_URL", "")  # GitHub Actions에서는 프록시 불필요
os.environ.setdefault("SCRAPER_STALE_DAYS", "30")  # stale 체크 완화

# ── 프로젝트 루트 추가 ──
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=False)

# 로깅 최소화 (stderr만)
from loguru import logger
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")

# ── 모든 checkNewArticle 함수 import ──
IMPORT_MAP: dict[str, tuple[str, str, bool]] = {
    # "키": ("모듈경로", "함수명", is_async)
    "LS_0":               ("modules.LS_0",               "LS_checkNewArticle",               False),
    "ShinHanInvest_1":    ("modules.ShinHanInvest_1",    "ShinHanInvest_checkNewArticle",     True),
    "NHQV_2":             ("modules.NHQV_2",             "NHQV_checkNewArticle",              True),
    "HANA_3":             ("modules.HANA_3",             "HANA_checkNewArticle",              True),
    "KBsec_4":            ("modules.KBsec_4",            "KB_checkNewArticle",                True),
    "Samsung_5":          ("modules.Samsung_5",          "Samsung_checkNewArticle",           False),
    "Sangsanginib_6":     ("modules.Sangsanginib_6",     "Sangsanginib_checkNewArticle",      True),
    "Shinyoung_7":        ("modules.Shinyoung_7",        "Shinyoung_checkNewArticle",         False),
    "Miraeasset_8":       ("modules.Miraeasset_8",       "Miraeasset_checkNewArticle",        False),
    "Hmsec_9":            ("modules.Hmsec_9",            "Hmsec_checkNewArticle",             False),
    "Kiwoom_10":          ("modules.Kiwoom_10",          "Kiwoom_checkNewArticle",            True),
    "DS_11":              ("modules.DS_11",              "DS_checkNewArticle",                False),
    # eugenefn_12: 보류
    "Koreainvestment_13": ("modules.Koreainvestment_13", "Koreainvestment_selenium_checkNewArticle", True),
    "DAOL_14":            ("modules.DAOL_14",            "DAOL_checkNewArticle",              True),
    "TOSSinvest_15":      ("modules.TOSSinvest_15",      "TOSSinvest_checkNewArticle",        False),
    "Leading_16":         ("modules.Leading_16",         "Leading_checkNewArticle",           True),
    "Daeshin_17":         ("modules.Daeshin_17",         "Daeshin_checkNewArticle",           True),
    # iMfnsec_18: 보류
    "DBfi_19":            ("modules.DBfi_19",            "DBfi_checkNewArticle",              True),
    "MERITZ_20":          ("modules.MERITZ_20",          "MERITZ_checkNewArticle",            True),
    "Hanwhawm_21":        ("modules.Hanwhawm_21",        "Hanwha_checkNewArticle",            True),
    "Hygood_22":          ("modules.Hygood_22",          "Hanyang_checkNewArticle",           True),
    "BNKfn_23":           ("modules.BNKfn_23",           "BNK_checkNewArticle",               True),
    "Kyobo_24":           ("modules.Kyobo_24",           "Kyobo_checkNewArticle",             True),
    "IBKs_25":            ("modules.IBKs_25",            "IBK_checkNewArticle",               True),
    "SKS_26":             ("modules.SKS_26",             "Sks_checkNewArticle",               False),
    "Yuanta_27":          ("modules.Yuanta_27",          "Yuanta_checkNewArticle",            True),
    "Heungkuk_28":        ("modules.Heungkuk_28",        "Heungkuk_checkNewArticle",           False),
}

# GA standalone으로 이관된 증권사. 기본 실행에는 포함하고, --server-only에서만 제외한다.
GA_STANDALONE_FIRMS = {
    "LS_0",            # 별도 scrape-ls.yml에서 처리
    "Samsung_5",       # GA standalone (scrape-samsung.yml)
    "Kiwoom_10",       # GA standalone (scrape-kiwoom.yml)
    "TOSSinvest_15",   # GA standalone (scrape-toss.yml)
    "Hanwhawm_21",     # GA standalone (scrape-hanwha.yml)
    "Hygood_22",       # GA standalone (scrape-hanyang.yml)
    "Heungkuk_28",     # GA standalone (scrape-heungkuk.yml)
    "KBsec_4",         # GA standalone (scrape-kb.yml)
    "NHQV_2",          # GA standalone (scrape-nhqv.yml)
    "Sangsanginib_6",  # GA standalone (scrape-sangsanginib.yml)
    "Leading_16",      # GA standalone (scrape-leading.yml)
}


def select_target_firms(firms_arg: str | None, *, server_only: bool, skip_ls: bool) -> list[str]:
    """Resolve target firm keys for local/all standalone execution."""
    if firms_arg:
        return [k.strip() for k in firms_arg.split(",") if k.strip() in IMPORT_MAP]

    excluded = set()
    if server_only:
        excluded.update(GA_STANDALONE_FIRMS)
    if skip_ls:
        excluded.add("LS_0")
    return [k for k in IMPORT_MAP if k not in excluded]


def import_function(module_path: str, func_name: str):
    """동적 import → callable 반환"""
    import importlib
    mod = importlib.import_module(module_path)
    return getattr(mod, func_name)


def _sync_worker(func, result_queue) -> None:
    """Run a sync scraper in an isolated process and report its result."""
    os.setsid()
    try:
        result_queue.put(("ok", func()))
    except BaseException as exc:
        result_queue.put(("error", (str(exc), traceback.format_exc())))


def _terminate_process_group(process: multiprocessing.Process) -> None:
    """Terminate a timed-out worker and any descendants it spawned."""
    if process.pid:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.join(timeout=0.5)
            if process.is_alive():
                os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.join(timeout=0.5)


def run_sync_scraper(name: str, func, timeout: int) -> dict:
    """Run a sync scraper with a hard timeout and typed result contract."""
    t0 = time.time()
    result_queue = multiprocessing.Queue()
    process = multiprocessing.Process(
        target=_sync_worker,
        args=(func, result_queue),
        daemon=False,
    )
    try:
        logger.info(f"[{name}] 시작...")
        process.start()
        process.join(timeout=timeout)
        if process.is_alive():
            _terminate_process_group(process)
            elapsed = time.time() - t0
            logger.error(f"[{name}] 타임아웃 ({timeout}s)")
            return {"name": name, "status": "timeout", "elapsed_sec": round(elapsed, 1), "articles": []}

        if result_queue.empty():
            raise RuntimeError(f"worker exited without a result (exitcode={process.exitcode})")
        outcome, payload = result_queue.get()
        if outcome == "error":
            error, stack = payload
            raise RuntimeError(f"{error}\n{stack}")
        result = payload
        if not isinstance(result, list):
            elapsed = time.time() - t0
            logger.error(f"[{name}] 잘못된 결과 타입: expected list, got {type(result).__name__}")
            return {
                "name": name,
                "status": "invalid_result",
                "error": f"expected list, got {type(result).__name__}",
                "elapsed_sec": round(elapsed, 1),
                "articles": [],
            }
        elapsed = time.time() - t0
        count = len(result)
        logger.success(f"[{name}] 완료: {count}건 ({elapsed:.1f}s)")
        return {
            "name": name,
            "status": "success",
            "count": count,
            "elapsed_sec": round(elapsed, 1),
            "articles": result if isinstance(result, list) else [],
        }
    except Exception as e:
        elapsed = time.time() - t0
        logger.error(f"[{name}] 실패: {e} ({elapsed:.1f}s)")
        return {
            "name": name,
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "elapsed_sec": round(elapsed, 1),
            "articles": [],
        }
    finally:
        if process.is_alive():
            _terminate_process_group(process)
        result_queue.close()
        result_queue.join_thread()


async def run_async_scraper(name: str, func, timeout: int) -> dict:
    """비동기 스크래퍼 실행 → 결과 dict"""
    t0 = time.time()
    try:
        logger.info(f"[{name}] 시작 (async)...")
        result = func()
        if asyncio.iscoroutine(result):
            result = await asyncio.wait_for(result, timeout=timeout)
        elapsed = time.time() - t0
        count = len(result) if isinstance(result, list) else 0
        logger.success(f"[{name}] 완료: {count}건 ({elapsed:.1f}s)")
        return {
            "name": name,
            "status": "success",
            "count": count,
            "elapsed_sec": round(elapsed, 1),
            "articles": result if isinstance(result, list) else [],
        }
    except asyncio.TimeoutError:
        elapsed = time.time() - t0
        logger.error(f"[{name}] 타임아웃 ({timeout}s)")
        return {"name": name, "status": "timeout", "elapsed_sec": round(elapsed, 1), "articles": []}
    except Exception as e:
        elapsed = time.time() - t0
        logger.error(f"[{name}] 실패: {e} ({elapsed:.1f}s)")
        return {
            "name": name,
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "elapsed_sec": round(elapsed, 1),
            "articles": [],
        }


async def main():
    parser = argparse.ArgumentParser(description="전체 증권사 Standalone Scraper")
    parser.add_argument("--firms", type=str, default=None,
                        help="쉼표로 구분된 증권사 키 목록 (예: 'BNKfn_23,HANA_3'). 기본: 전체")
    parser.add_argument("--timeout", type=int, default=120,
                        help="비동기 함수당 타임아웃(초) (기본: 120)")
    parser.add_argument("--server-only", action="store_true",
                        help="GA standalone 운용 증권사를 제외하고 서버 경로 대상만 실행")
    parser.add_argument("--skip-ls", action="store_true",
                        help="LS증권 제외")
    args = parser.parse_args()

    # 대상 증권사 결정
    target_firms = select_target_firms(
        args.firms,
        server_only=args.server_only,
        skip_ls=args.skip_ls,
    )

    logger.info(f"대상 증권사: {len(target_firms)}개")
    for f in target_firms:
        logger.info(f"  - {f}")

    # ── import 확인 ──
    sync_funcs: list[tuple[str, callable]] = []
    async_funcs: list[tuple[str, callable]] = []

    for key in target_firms:
        module_path, func_name, is_async = IMPORT_MAP[key]
        try:
            func = import_function(module_path, func_name)
            if is_async:
                async_funcs.append((key, func))
            else:
                sync_funcs.append((key, func))
            logger.debug(f"  {key}: import OK")
        except Exception as e:
            logger.error(f"  {key}: import 실패 - {e}")

    results: list[dict] = []

    # ── Phase 1: 동기 스크래퍼 ──
    logger.info(f"\n=== Phase 1: Sync Scrapers ({len(sync_funcs)}개) ===")
    for name, func in sync_funcs:
        result = run_sync_scraper(name, func, args.timeout)
        results.append(result)
        time.sleep(1)  # polite delay

    # ── Phase 2: 비동기 스크래퍼 ──
    logger.info(f"\n=== Phase 2: Async Scrapers ({len(async_funcs)}개) ===")
    tasks = [run_async_scraper(name, func, args.timeout) for name, func in async_funcs]
    async_results = await asyncio.gather(*tasks)
    results.extend(async_results)

    # ── 집계 ──
    total_articles = 0
    success_firms = 0
    failed_firms = 0

    for r in results:
        if r["status"] == "success":
            success_firms += 1
            total_articles += r["count"]
        else:
            failed_firms += 1

    logger.info(f"\n=== 최종 집계 ===")
    logger.info(f"성공: {success_firms}, 실패: {failed_firms}, 총 아티클: {total_articles}")

    output = {
        "scraped_at": datetime.now().isoformat(),
        "source": "github-actions",
        "total_firms": len(results),
        "success_firms": success_firms,
        "failed_firms": failed_firms,
        "total_articles": total_articles,
        "firms": results,
    }

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
