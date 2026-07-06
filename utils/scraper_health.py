# -*- coding:utf-8 -*-
"""Scraper health logging utilities.

Extracted from scraper.py to reduce module-level noise.
log_scraper_health() validates scraper results and returns any health errors
instead of mutating a global list — callers append to their own error list.
"""
import datetime
from loguru import logger


def log_scraper_health(name: str, rows, stale_days: int = 5,
                       stale_overrides: dict | None = None) -> list[str]:
    """스크래퍼 실행 결과를 검증하고 health error 메시지 목록을 반환.

    정상이면 빈 리스트, 문제가 있으면 오류 메시지가 담긴 리스트를 반환한다.
    호출자는 반환값을 자신의 error list에 extend한다.

    Args:
        name: 스크래퍼 함수명 (로깅용)
        rows: 스크래퍼 반환값
        stale_days: 최신 report_date가 이 일수보다 오래됐으면 stale 경고
        stale_overrides: 함수명 → days 매핑 (개별 stale 임계치 override)

    Returns:
        list[str]: health error 메시지 목록 (없으면 빈 리스트)
    """
    errors: list[str] = []
    overrides = stale_overrides or {}

    if not isinstance(rows, list):
        msg = f"{name} returned non-list result: {type(rows)}"
        errors.append(msg)
        logger.error(msg)
        return errors

    if not rows:
        msg = f"{name} returned 0 articles. Check source API, selector, or credentials."
        # 0건 수집 → 알람 노이즈 방지 (구조 변경 or IP 차단 등 외부 이슈일 가능성)
        logger.warning(msg)
        return errors

    report_dates = sorted({
        str(row.get("report_date", ""))[:8]
        for row in rows
        if row.get("report_date")
    })
    if not report_dates:
        msg = f"{name} returned {len(rows)} articles but no report_date values."
        errors.append(msg)
        logger.error(msg)
        return errors

    min_date = report_dates[0]
    max_date = report_dates[-1]
    logger.info(f"{name} => Found {len(rows)} articles (report_date {min_date}~{max_date})")

    try:
        max_date_obj = datetime.datetime.strptime(max_date, "%Y%m%d").date()
        threshold_days = overrides.get(name, stale_days)
        stale_cutoff = datetime.datetime.now().date() - datetime.timedelta(days=threshold_days)
        if max_date_obj < stale_cutoff:
            msg = (
                f"{name} latest report_date is stale: {max_date} "
                f"(older than {threshold_days} days)"
            )
            errors.append(msg)
            logger.error(msg)
    except ValueError:
        msg = f"{name} returned invalid max report_date: {max_date}"
        errors.append(msg)
        logger.error(msg)

    return errors
