"""Scraper output validation — 모든 core 모듈이 리턴 전에 호출."""
import re, sys

def validate_results(results: list[dict], firm_name: str) -> list[dict]:
    """필수 필드 검증. 유효하지 않은 항목은 제외하고 stderr에 경고."""
    valid = []
    errors = 0
    for i, r in enumerate(results):
        # 1) report_unique_key or key must exist and be non-empty
        uid = r.get("report_unique_key") or r.get("key")
        if not uid:
            print(f"[{firm_name}] SKIP item[{i}]: missing report_unique_key/key", file=sys.stderr)
            errors += 1
            continue

        # 2) reg_dt must be exactly YYYYMMDD
        reg_dt = str(r.get("reg_dt", "")).strip()
        if not re.match(r'^\d{8}$', reg_dt):
            print(f"[{firm_name}] SKIP item[{i}]: invalid reg_dt='{reg_dt}'", file=sys.stderr)
            errors += 1
            continue

        r["reg_dt"] = reg_dt
        valid.append(r)

    if errors:
        print(f"[{firm_name}] validation: {errors} items filtered, {len(valid)} passed", file=sys.stderr)
    return valid
