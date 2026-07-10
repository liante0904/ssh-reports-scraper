# -*- coding:utf-8 -*-
"""Firm scraper registry — Single Source of Truth for firm dispatch.

Reads config/firms.yaml and provides lazy-imported access to all 29 firm
scraper functions. Replaces 29 individual `from modules.X import Y` lines
in scraper.py with data-driven dispatch.

Usage:
    from scraper_registry import (
        get_regular_sync_funcs, get_regular_async_funcs,
        get_ga_sync_mapping, get_ga_async_mapping,
        get_enricher, get_enrichment_skip_firm_ids,
        get_ls_funcs, get_ls_module_func,
    )

Design:
    - YAML is the human-editable SSoT (config/firms.yaml).
    - This module reads it at import time and caches function lookups.
    - importlib.import_module is used for lazy loading — scraper functions
      are only imported when their accessor is called, not at registry import.
    - Falls back to hardcoded registry if YAML is missing or unparseable.
"""
from __future__ import annotations

import importlib
from pathlib import Path
from typing import Callable

from loguru import logger

# ── YAML manifest path ──────────────────────────────────────────────────────
_MANIFEST_PATH = Path(__file__).parent / "config" / "firms.yaml"

# ── Function cache (lazy import) ────────────────────────────────────────────
_FUNC_CACHE: dict[str, Callable] = {}

# ── Firm registry entries (loaded from YAML) ────────────────────────────────
_registry: list[dict] = []


def _load_yaml_manifest() -> list[dict]:
    """Load firm manifest from YAML. Returns list of firm dicts."""
    import yaml
    with open(_MANIFEST_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return list(data["firms"].values())


def _init_registry() -> list[dict]:
    """Initialize registry from YAML, falling back to empty list on error."""
    try:
        return _load_yaml_manifest()
    except FileNotFoundError:
        logger.warning("config/firms.yaml not found — registry will be empty")
        return []
    except Exception as e:
        logger.error(f"Failed to load config/firms.yaml: {e}")
        return []


_registry = _init_registry()

# ── Special functions, enrichers, enrichment skip (built from YAML) ────────


def _build_special_functions() -> dict[str, tuple[str, str]]:
    """Aggregate special_funcs from all firms in YAML."""
    result: dict[str, tuple[str, str]] = {}
    for firm in _registry:
        special = firm.get("special_funcs")
        if special:
            for name, path in special.items():
                module_path, func_name = path.split(":", 1)
                result[name] = (module_path, func_name)
    return result


def _build_enricher_map() -> dict[int, tuple[str, str]]:
    """Build firm_id → (module_path, func_name) from YAML enricher field."""
    result: dict[int, tuple[str, str]] = {}
    for firm in _registry:
        enricher_str = firm.get("enricher")
        if enricher_str:
            module_path, func_name = enricher_str.split(":", 1)
            result[firm["firm_id"]] = (module_path, func_name)
    return result


def _build_enrichment_skip() -> frozenset[int]:
    """Collect firm_ids whose enrichment should be skipped."""
    return frozenset(
        f["firm_id"] for f in _registry if f.get("enrichment_skip")
    )


_SPECIAL_FUNCTIONS = _build_special_functions()
_ENRICHER_MAP = _build_enricher_map()
_ENRICHMENT_SKIP = _build_enrichment_skip()


# ── Public: lazy function import ────────────────────────────────────────────

def _import_func(module_path: str, func_name: str) -> Callable:
    """Lazy-import a single function from a module, caching the result."""
    cache_key = f"{module_path}.{func_name}"
    if cache_key not in _FUNC_CACHE:
        mod = importlib.import_module(module_path)
        _FUNC_CACHE[cache_key] = getattr(mod, func_name)
    return _FUNC_CACHE[cache_key]


def get_func(module_path: str, func_name: str) -> Callable | None:
    """Import and return a scraper function. Returns None on import failure.

    Safe to call with user-provided paths — failures are logged, not raised.
    """
    try:
        return _import_func(module_path, func_name)
    except (ImportError, AttributeError) as e:
        logger.warning(f"Failed to import {func_name} from {module_path}: {e}")
        return None


# ── Public: firm data access ────────────────────────────────────────────────

def all_firms() -> list[dict]:
    """Return all 29 firm entries (raw dicts from YAML)."""
    return _registry


def get_firm_by_id(firm_id: int) -> dict | None:
    """Look up a single firm entry by firm_id. Returns None if not found."""
    for f in _registry:
        if f["firm_id"] == firm_id:
            return f
    return None


# ── Public: function list builders (match current scraper.py behavior) ──────

def get_regular_sync_funcs() -> list[Callable]:
    """Return sync scraper functions for regular (non-full-scrape) mode.

    Matches current scraper.py _regular_sync_functions():
        Shinyoung(7), DS(11)
    """
    funcs = []
    for firm in _registry:
        if firm.get("server_list") != "sync":
            continue
        fn = get_func(firm["server_module"], _func_name_from_module(firm))
        if fn:
            funcs.append(fn)
    return funcs


def get_regular_async_funcs() -> list[Callable]:
    """Return async scraper functions for regular (non-full-scrape) mode.

    Matches current scraper.py _regular_async_functions():
        ShinHanInvest(1), Koreainvestment(13), Daeshin(17), HANA(3)
    """
    funcs = []
    for firm in _registry:
        if firm.get("server_list") != "async":
            continue
        fn = get_func(firm["server_module"], _func_name_from_module(firm))
        if fn:
            funcs.append(fn)
    return funcs


def get_ga_sync_mapping() -> dict[int, Callable]:
    """Return {firm_id: func} for GA sync firms active in full-scrape fallback.

    Matches current scraper.py _GA_FIRMS_SYNC:
        Samsung(5), Hmsec(9), TOSSinvest(15), Heungkuk(28)

    Excluded firms (ga_fallback_excluded=true) are NOT included:
        Miraeasset(8), Sks(26) — server config incomplete
    """
    result: dict[int, Callable] = {}
    for firm in _registry:
        if firm.get("ga_full_scrape_list") != "sync":
            continue
        if firm.get("ga_fallback_excluded"):
            continue
        fn = get_func(firm["server_module"], _func_name_from_module(firm))
        if fn:
            result[firm["firm_id"]] = fn
    return result


def get_ga_async_mapping() -> dict[int, Callable]:
    """Return {firm_id: func} for GA async firms active in full-scrape fallback.

    Matches current scraper.py _GA_FIRMS_ASYNC:
        NHQV(2), KB(4), Sangsanginib(6), DBfi(19), MERITZ(20),
        Hanwha(21), Kyobo(24), IBK(25), Yuanta(27)

    Excluded firms (ga_fallback_excluded=true) are NOT included:
        Kiwoom(10), DAOL(14), Leading(16), iMfn(18), Hanyang(22)
    """
    result: dict[int, Callable] = {}
    for firm in _registry:
        if firm.get("ga_full_scrape_list") != "async":
            continue
        if firm.get("ga_fallback_excluded"):
            continue
        fn = get_func(firm["server_module"], _func_name_from_module(firm))
        if fn:
            result[firm["firm_id"]] = fn
    return result


def get_enricher(firm_id: int) -> Callable | None:
    """Return the enricher function for a firm, or None."""
    entry = _ENRICHER_MAP.get(firm_id)
    if entry is None:
        return None
    return get_func(*entry)


def get_enrichment_skip_firm_ids() -> frozenset[int]:
    """Return frozenset of firm_ids whose enrichment should be skipped."""
    return _ENRICHMENT_SKIP


def get_ls_module_func(func_name: str) -> Callable | None:
    """Import a function from the LS special module (modules.LS_0).

    Used for LS_checkNewArticle, LS_detail, LS_enrich.
    """
    # First check the standard function naming
    entry = _SPECIAL_FUNCTIONS.get(func_name)
    if entry:
        return get_func(*entry)
    # Fallback: try modules.LS_0 directly
    return get_func("modules.LS_0", func_name)


# ── Internal helpers ────────────────────────────────────────────────────────

def _func_name_from_module(firm: dict) -> str:
    """Resolve the checkNewArticle function name for a firm.

    Uses func_name from YAML first. Falls back to a heuristic derivation
    from server_module name for backward compatibility if func_name is missing.
    """
    # Use explicit func_name from YAML if available
    fn_name = firm.get("func_name")
    if fn_name:
        return fn_name
    # Fallback heuristic: e.g. "modules.KBsec_4" → "KBsec_checkNewArticle"
    module_name = firm.get("server_module", "")
    parts = module_name.split(".")
    if len(parts) >= 2:
        base = parts[-1]
        if "_" in base:
            name_part = base.rsplit("_", 1)[0]
        else:
            name_part = base
        return f"{name_part}_checkNewArticle"
    return "unknown"
