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
    - Missing or invalid manifests fail startup instead of silently disabling scrapers.
"""
from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from typing import Callable

from loguru import logger

# ── YAML manifest path ──────────────────────────────────────────────────────
_MANIFEST_PATH = Path(__file__).parent / "config" / "firms.yaml"

# ── Function cache (lazy import) ────────────────────────────────────────────
_FUNC_CACHE: dict[str, Callable] = {}

# ── Firm registry entries (loaded from YAML) ────────────────────────────────
_registry: list[dict] = []

_REQUIRED_FIELDS: dict[str, type] = {
    "display_name": str,
    "firm_id": int,
    "mode": str,
    "server_module": str,
    "server_list": str,
    "ga_full_scrape_list": str,
    "ga_fallback_excluded": bool,
    "func_name": str,
    "enrichment_skip": bool,
    "config_shape": str,
    "empty_policy": str,
}
_LIST_MODES = {"sync", "async", "none"}
_FIRM_MODES = {"ga", "server", "dual", "ga_disabled", "blocked"}
_CONFIG_SHAPES = {"url_list", "full_config"}
_EMPTY_POLICIES = {"require_non_empty", "allow_empty", "server_only"}


def _validate_manifest(data: object) -> list[dict]:
    """Validate the dispatch fields required by runtime registry builders."""
    if not isinstance(data, dict) or not isinstance(data.get("firms"), dict):
        raise ValueError("config/firms.yaml must contain a 'firms' mapping")

    firms = list(data["firms"].values())
    if not firms:
        raise ValueError("config/firms.yaml must contain at least one firm")

    seen_ids: set[int] = set()
    for firm in firms:
        if isinstance(firm, dict) and type(firm.get("firm_id")) is int:
            if firm["firm_id"] in seen_ids:
                raise ValueError(f"duplicate firm_id: {firm['firm_id']}")
            seen_ids.add(firm["firm_id"])

    for key, firm in data["firms"].items():
        if not isinstance(firm, dict):
            raise ValueError(f"firm '{key}' must be a mapping")
        for field, expected_type in _REQUIRED_FIELDS.items():
            value = firm.get(field)
            if type(value) is not expected_type or (
                expected_type is str and not value.strip()
            ):
                raise ValueError(
                    f"firm '{key}' field '{field}' must be a non-empty "
                    f"{expected_type.__name__}"
                )
        if firm["mode"] not in _FIRM_MODES:
            raise ValueError(
                f"firm '{key}' field 'mode' must be one of {sorted(_FIRM_MODES)}"
            )
        if firm["config_shape"] not in _CONFIG_SHAPES:
            raise ValueError(
                f"firm '{key}' field 'config_shape' must be one of {sorted(_CONFIG_SHAPES)}"
            )
        if firm["empty_policy"] not in _EMPTY_POLICIES:
            raise ValueError(
                f"firm '{key}' field 'empty_policy' must be one of {sorted(_EMPTY_POLICIES)}"
            )
        for field in ("server_list", "ga_full_scrape_list"):
            if firm[field] not in _LIST_MODES:
                raise ValueError(
                    f"firm '{key}' field '{field}' must be one of {sorted(_LIST_MODES)}"
                )
    return firms


def _load_yaml_manifest() -> list[dict]:
    """Load firm manifest from YAML. Returns list of firm dicts."""
    import yaml
    with open(_MANIFEST_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return _validate_manifest(data)


def _init_registry() -> list[dict]:
    """Initialize registry, raising when dispatch configuration is unusable."""
    return _load_yaml_manifest()


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


def _get_active_func(firm: dict, expected_mode: str) -> Callable:
    """Import an active scraper and enforce its manifest dispatch mode."""
    module_path = firm["server_module"]
    func_name = _func_name_from_module(firm)
    try:
        fn = _import_func(module_path, func_name)
    except (ImportError, AttributeError) as exc:
        raise RuntimeError(
            f"active firm_id={firm['firm_id']} callable is unavailable: "
            f"{module_path}.{func_name}"
        ) from exc
    if not callable(fn):
        raise RuntimeError(
            f"active firm_id={firm['firm_id']} target is not callable: "
            f"{module_path}.{func_name}"
        )
    actual_mode = "async" if inspect.iscoroutinefunction(fn) else "sync"
    if actual_mode != expected_mode:
        raise RuntimeError(
            f"firm_id={firm['firm_id']} callable mode mismatch: "
            f"manifest={expected_mode}, callable={actual_mode} ({module_path}.{func_name})"
        )
    return fn


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
        funcs.append(_get_active_func(firm, "sync"))
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
        funcs.append(_get_active_func(firm, "async"))
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
        result[firm["firm_id"]] = _get_active_func(firm, "sync")
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
        result[firm["firm_id"]] = _get_active_func(firm, "async")
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
