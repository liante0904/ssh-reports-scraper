"""In-process validation library to normalize and validate scraped dict lists.
Caller/Boundary: Scraper core modules (scrapers/*_core.py) processing in-memory dicts.
Counterpart: For standalone CLI validation of JSON artifact files, use scripts/validate_scrape_result.py.
"""
import sys

from models.report_payload import ReportPayload, ReportPayloadError

def validate_results(results: list[dict], firm_name: str) -> list[dict]:
    """필수 필드 검증. 유효하지 않은 항목은 제외하고 stderr에 경고."""
    valid = []
    errors = 0
    for i, r in enumerate(results):
        try:
            payload = ReportPayload.from_scraper(r, require_schema=True)
        except ReportPayloadError as exc:
            print(f"[{firm_name}] SKIP item[{i}]: {exc}", file=sys.stderr)
            errors += 1
            continue
        valid.append(payload.to_scraper_dict(r))

    if errors:
        print(f"[{firm_name}] validation: {errors} items filtered, {len(valid)} passed", file=sys.stderr)
    return valid
