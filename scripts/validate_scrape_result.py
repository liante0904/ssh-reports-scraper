#!/usr/bin/env python3
"""Validate a GA artifact against its firm manifest policy before transfer."""

import argparse
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone, timedelta

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.report_payload import ReportPayload, ReportPayloadError


def _load_firms() -> dict[str, dict]:
    with (ROOT / "config" / "firms.yaml").open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    firms = data.get("firms") if isinstance(data, dict) else None
    if not isinstance(firms, dict):
        raise ValueError("manifest must contain a firms mapping")
    return firms


def _resolve_firm(filepath: str, firm_identifier: str | None) -> tuple[str, dict]:
    firms = _load_firms()
    if firm_identifier:
        matches = [
            (key, firm) for key, firm in firms.items()
            if firm_identifier in {key, str(firm.get("firm_id")), firm.get("display_name")}
        ]
    else:
        filename = os.path.basename(filepath)
        matches = [
            (key, firm) for key, firm in firms.items()
            if os.path.basename(str(firm.get("result_file") or "")) == filename
        ]
    if len(matches) != 1:
        label = firm_identifier or os.path.basename(filepath)
        raise ValueError(f"missing or unknown firm: {label}")
    key, firm = matches[0]
    if firm.get("config_shape") not in {"url_list", "full_config"}:
        raise ValueError(f"firm '{key}' has invalid config_shape")
    if firm.get("empty_policy") not in {
        "require_non_empty", "allow_empty", "server_only"
    }:
        raise ValueError(f"firm '{key}' has invalid empty_policy")
    return key, firm


def validate(
    filepath: str,
    require_non_empty: bool | None = None,
    firm_identifier: str | None = None,
) -> int:
    try:
        firm_key, firm = _resolve_firm(filepath, firm_identifier)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2

    if not os.path.exists(filepath):
        print(f"FATAL: file not found: {filepath}", file=sys.stderr)
        return 2

    try:
        with open(filepath, encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        print(f"FATAL: invalid JSON: {exc}", file=sys.stderr)
        return 2

    if not isinstance(data, list):
        print(f"FATAL: expected JSON array, got {type(data).__name__}", file=sys.stderr)
        return 2

    policy_requires_data = firm["empty_policy"] == "require_non_empty"
    if require_non_empty is not None:
        policy_requires_data = require_non_empty
    if not data:
        if policy_requires_data:
            print("FAIL: empty result (0 articles) - not sending to server", file=sys.stderr)
            return 1
        print(f"OK: empty result allowed for firm '{firm_key}'", file=sys.stderr)
        return 0

    errors: list[str] = []
    normalized = []
    for index, item in enumerate(data):
        try:
            payload = ReportPayload.from_scraper(
                item, require_schema=True, require_firm_name=True
            )
            normalized.append(payload)
        except ReportPayloadError as exc:
            errors.append(f"item[{index}]: {exc}")
    if errors:
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        print(f"FAIL: {len(errors)} items violate report schema", file=sys.stderr)
        return 2

    dates = [payload.report_date for payload in normalized if payload.report_date]
    if dates:
        latest = max(dates)
        today = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
        if latest < today:
            print(f"WARN: latest report_date={latest} < today={today} (may be stale)", file=sys.stderr)

    print(f"OK: {len(normalized)} articles for firm '{firm_key}'", file=sys.stderr)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath")
    parser.add_argument("--firm", help="manifest key, firm_id, or display_name")
    parser.add_argument("--require-non-empty", action="store_true", default=None)
    args = parser.parse_args()
    sys.exit(validate(args.filepath, args.require_non_empty, args.firm))
