#!/usr/bin/env python3
import json
import os
import sys

from scrapers.config_guard import ScraperConfigError


def run_env_scraper(*, env_key, firm_name, scrape_func, required_keys=None, **kwargs):
    raw = os.getenv(env_key, "")
    if not raw:
        print(f"[{firm_name}] FATAL: {env_key} not set", file=sys.stderr)
        sys.exit(1)

    try:
        cfg = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"[{firm_name}] FATAL: {env_key} is not valid JSON: {exc}", file=sys.stderr)
        sys.exit(1)

    if required_keys and not isinstance(cfg, dict):
        print(
            f"[{firm_name}] FATAL: {env_key} must be full config object, got {type(cfg).__name__}",
            file=sys.stderr,
        )
        sys.exit(1)

    if required_keys:
        missing = [key for key in required_keys if key not in cfg]
        if missing:
            print(
                f"[{firm_name}] FATAL: {env_key} missing keys: {', '.join(missing)}",
                file=sys.stderr,
            )
            sys.exit(1)

    try:
        result = scrape_func(cfg=cfg, **kwargs)
    except ScraperConfigError as exc:
        print(f"[{firm_name}] FATAL: {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyError as exc:
        print(f"[{firm_name}] FATAL: config missing key: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"[{firm_name}] total {len(result)} articles collected", file=sys.stderr)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
