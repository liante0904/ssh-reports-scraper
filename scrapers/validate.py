"""Scraper output validation — 모든 core 모듈이 리턴 전에 호출."""
import re, sys

def validate_results(results: list[dict], firm_name: str) -> list[dict]:
    """필수 필드 검증. 유효하지 않은 항목은 제외하고 stderr에 경고."""
    valid = []
    errors = 0
    for i, r in enumerate(results):
        # 1) report_unique_key must exist and be non-empty (key 컬럼 deprecated, drop 예정)
        uid = r.get("report_unique_key")
        if not uid:
            print(f"[{firm_name}] SKIP item[{i}]: missing report_unique_key", file=sys.stderr)
            errors += 1
            continue

        # 2) report_date must be exactly YYYYMMDD
        report_date = str(r.get("report_date", "")).strip()
        if not re.match(r'^\d{8}$', report_date):
            print(f"[{firm_name}] SKIP item[{i}]: invalid report_date='{report_date}'", file=sys.stderr)
            errors += 1
            continue

        r["report_date"] = report_date
        valid.append(r)

    if errors:
        print(f"[{firm_name}] validation: {errors} items filtered, {len(valid)} passed", file=sys.stderr)
    return valid
