#!/usr/bin/env python3
"""GA scrape 결과 검증기 — SCP 전에 실행.

사용법:
  python scripts/validate_scrape_result.py result.json [--require-non-empty]

Exit codes:
  0: 유효 데이터 1건 이상
  1: 0건 (빈 결과 — SCP 전송하지 않음)
  2: 스키마 오류 (잘못된 JSON, 필수 필드 누락, 날짜 형식 오류)
  3: 중복률 초과 또는 stale 데이터
"""
import json, re, sys, os
from datetime import datetime, timezone, timedelta

def validate(filepath: str, require_non_empty: bool = True) -> int:
    if not os.path.exists(filepath):
        print(f"FATAL: file not found: {filepath}", file=sys.stderr)
        return 2

    with open(filepath) as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"FATAL: invalid JSON: {e}", file=sys.stderr)
            return 2

    if not isinstance(data, list):
        print(f"FATAL: expected JSON array, got {type(data).__name__}", file=sys.stderr)
        return 2

    if len(data) == 0:
        if require_non_empty:
            print("FAIL: empty result (0 articles) — not sending to server", file=sys.stderr)
            return 1
        print("WARN: empty result (0 articles)", file=sys.stderr)
        return 1

    errors = {"bad_date": 0, "missing_key": 0, "bad_firm": 0}
    for i, item in enumerate(data):
        # Check report_unique_key
        uid = item.get("report_unique_key")
        if not uid:
            errors["missing_key"] += 1
            print(f"  item[{i}]: missing report_unique_key", file=sys.stderr)

        # Check report_date (canonical)
        report_date = str(item.get("report_date", "")).strip()
        if not re.match(r'^\d{8}$', report_date):
            errors["bad_date"] += 1
            print(f"  item[{i}]: invalid report_date='{report_date}'", file=sys.stderr)

        # Check firm_nm
        if not item.get("firm_nm"):
            errors["bad_firm"] += 1

    if errors["missing_key"] > 0:
        print(f"FAIL: {errors['missing_key']} items missing unique key", file=sys.stderr)
        return 2
    if errors["bad_date"] > 0:
        print(f"FAIL: {errors['bad_date']} items with invalid report_date", file=sys.stderr)
        return 2

    # Check report_date recency (stale check)
    dates = [str(d.get("report_date", "")) for d in data
             if re.match(r'^\d{8}$', str(d.get("report_date", "")))]
    if dates:
        latest = max(dates)
        today = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
        if latest < today:
            print(f"WARN: latest report_date={latest} < today={today} (may be stale)", file=sys.stderr)

    print(f"OK: {len(data)} articles, {errors['bad_date']} bad_date, {errors['missing_key']} missing_key", file=sys.stderr)
    return 0


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("filepath")
    p.add_argument("--require-non-empty", action="store_true", default=True)
    args = p.parse_args()
    sys.exit(validate(args.filepath, args.require_non_empty))
