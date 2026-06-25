#!/usr/bin/env python3
"""Offline maintenance harness for firm manifest checks.

This is intentionally small: it validates manifest references and existing
shell guards without running scrapers, touching the network, or connecting to DB.
"""

from __future__ import annotations

import argparse
import os
import py_compile
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "config" / "firms.yaml"

VALID_MODES = {"ga", "server", "dual", "ga_disabled", "blocked"}
VALID_CONFIG_SHAPES = {"url_list", "full_config"}
VALID_EMPTY_POLICIES = {"require_non_empty", "allow_empty", "server_only"}


def load_manifest(path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    firms = data.get("firms")
    if not isinstance(firms, dict):
        raise ValueError("manifest must contain a 'firms' mapping")
    return firms


def module_to_path(module_name: str | None) -> str | None:
    if not module_name:
        return None
    return module_name.replace(".", os.sep) + ".py"


def _check_enum(errors: list[str], firm_key: str, label: str, value: str, allowed: set[str]) -> None:
    if value not in allowed:
        errors.append(f"{firm_key}: invalid {label} '{value}'")


def _check_path(errors: list[str], firm_key: str, label: str, rel_path: str | None) -> None:
    if rel_path is None:
        return
    if not (ROOT / rel_path).exists():
        errors.append(f"{firm_key}: {label} not found: {rel_path}")


def check_manifest(firms: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for firm_key, firm in firms.items():
        _check_enum(errors, firm_key, "mode", firm.get("mode"), VALID_MODES)
        _check_enum(
            errors,
            firm_key,
            "config_shape",
            firm.get("config_shape"),
            VALID_CONFIG_SHAPES,
        )
        _check_enum(
            errors,
            firm_key,
            "empty_policy",
            firm.get("empty_policy"),
            VALID_EMPTY_POLICIES,
        )
    return errors


def check_files(firms: dict[str, Any], selected: set[str] | None = None) -> list[str]:
    errors: list[str] = []
    for firm_key, firm in firms.items():
        if selected is not None and firm_key not in selected:
            continue

        _check_path(errors, firm_key, "core_module", module_to_path(firm.get("core_module")))
        _check_path(errors, firm_key, "standalone_path", firm.get("standalone_path"))
        _check_path(errors, firm_key, "server_module", module_to_path(firm.get("server_module")))
        _check_path(errors, firm_key, "workflow_path", firm.get("workflow_path"))

        standalone = firm.get("standalone_path")
        if standalone:
            try:
                py_compile.compile(str(ROOT / standalone), doraise=True)
            except py_compile.PyCompileError as exc:
                errors.append(f"{firm_key}: standalone py_compile failed: {exc.msg}")
    return errors


def run_shell_guards() -> list[str]:
    errors: list[str] = []
    for script in ("scripts/verify_standalones.sh", "scripts/verify_dockerfile.sh"):
        result = subprocess.run(
            ["bash", script],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if result.returncode != 0:
            errors.append(f"{Path(script).name} failed")
            if result.stdout:
                errors.append(result.stdout.strip())
    return errors


def emit_errors(errors: list[str]) -> int:
    if not errors:
        print("OK")
        return 0
    for error in errors:
        print(error, file=sys.stderr)
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline firm manifest harness")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--firm", help="Validate a single firm key")
    group.add_argument("--all", action="store_true", help="Validate all firms")
    group.add_argument("--check-manifest", action="store_true", help="Validate manifest enums only")
    group.add_argument("--list", action="store_true", help="List firm keys")
    parser.add_argument("--offline", action="store_true", help="Required for file/guard checks")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST), help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    firms = load_manifest(Path(args.manifest))

    if args.list:
        for key in sorted(firms):
            print(key)
        return 0

    errors = check_manifest(firms)
    if args.check_manifest:
        return emit_errors(errors)

    if not args.offline:
        errors.append("--offline is required for --firm/--all checks")
        return emit_errors(errors)

    selected = None
    if args.firm:
        if args.firm not in firms:
            errors.append(f"{args.firm}: firm not found in manifest")
            return emit_errors(errors)
        selected = {args.firm}

    errors.extend(check_files(firms, selected))
    errors.extend(run_shell_guards())
    return emit_errors(errors)


if __name__ == "__main__":
    raise SystemExit(main())
