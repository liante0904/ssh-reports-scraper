"""Firm manifest validation — 구조 / enum / 파일 존재 검증 (no network, no scraper logic)."""
import os
import sys

import pytest
import yaml

SCRAPER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MANIFEST_PATH = os.path.join(SCRAPER_DIR, "config", "firms.yaml")

VALID_MODES = {"ga", "server", "dual", "ga_disabled", "blocked"}
VALID_CONFIG_SHAPES = {"url_list", "full_config"}
VALID_EMPTY_POLICIES = {"require_non_empty", "allow_empty", "server_only"}
REQUIRED_FIELDS = {
    "display_name", "firm_id", "mode", "core_module",
    "standalone_path", "server_module", "workflow_path",
    "env_var", "result_file", "config_shape", "empty_policy",
}


def _load_manifest():
    with open(MANIFEST_PATH) as f:
        return yaml.safe_load(f)


# ── Structural tests ──────────────────────────────────────────────


def test_manifest_parses():
    """YAML 파싱 성공 + firms 키 존재."""
    data = _load_manifest()
    assert "firms" in data, "manifest must have top-level 'firms' key"
    assert isinstance(data["firms"], dict), "'firms' must be a dict"
    assert len(data["firms"]) > 0, "'firms' must not be empty"


def test_each_firm_has_required_fields():
    """모든 firm이 필수 필드를 빠뜨리지 않았는지 확인."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        missing = REQUIRED_FIELDS - set(firm.keys())
        assert not missing, f"{key}: missing required fields: {missing}"


def test_mode_enum():
    """mode 값이 허용된 enum인지 확인."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        assert firm["mode"] in VALID_MODES, (
            f"{key}: invalid mode='{firm['mode']}', allowed={VALID_MODES}"
        )


def test_config_shape_enum():
    """config_shape 값이 허용된 enum인지 확인."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        assert firm["config_shape"] in VALID_CONFIG_SHAPES, (
            f"{key}: invalid config_shape='{firm['config_shape']}'"
        )


def test_empty_policy_enum():
    """empty_policy 값이 허용된 enum인지 확인."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        assert firm["empty_policy"] in VALID_EMPTY_POLICIES, (
            f"{key}: invalid empty_policy='{firm['empty_policy']}'"
        )


def test_firm_id_uniqueness():
    """firm_id가 중복되지 않았는지 확인."""
    data = _load_manifest()
    orders = {}
    for key, firm in data["firms"].items():
        o = firm["firm_id"]
        assert o not in orders, (
            f"{key}: duplicate firm_id={o} (already used by {orders[o]})"
        )
        orders[o] = key


# ── File existence tests (offline, no network) ────────────────────


def _assert_path(path, label, firm_key):
    """path가 None이면 건너뛰고, 있으면 존재 확인."""
    if path is None:
        return
    full = os.path.join(SCRAPER_DIR, path)
    assert os.path.exists(full), (
        f"{firm_key}: {label} not found: {path}"
    )


def test_core_module_files_exist():
    """manifest에 선언된 core_module 파일이 존재하는지 확인 (null 허용)."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        _assert_path(
            firm["core_module"].replace(".", os.sep) + ".py" if firm["core_module"] else None,
            "core_module",
            key,
        )


def test_standalone_files_exist():
    """manifest에 선언된 standalone 파일이 존재하는지 확인 (null 허용)."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        _assert_path(firm["standalone_path"], "standalone_path", key)


def test_server_module_files_exist():
    """manifest에 선언된 server_module 파일이 존재하는지 확인."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        _assert_path(
            firm["server_module"].replace(".", os.sep) + ".py",
            "server_module",
            key,
        )


def test_workflow_files_exist():
    """manifest에 선언된 workflow 파일이 존재하는지 확인 (null 허용)."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        _assert_path(firm["workflow_path"], "workflow_path", key)


# ── Cross-reference tests ─────────────────────────────────────────


def test_env_var_consistency():
    """env_var가 있으면 standalone 또는 server_module에서 env 이름이 참조되는지 확인.

    _runner.py 기반 standalone은 env 이름이 standalone 파일에 직접 나타난다.
    _runner.py 미사용 standalone(LS, 한국투자 등)은 server_module 안에서
    ConfigManager를 통해 간접 읽으므로 server_module을 검사한다.
    env_var가 None이면 검증 건너뜀."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        env = firm["env_var"]
        if env is None:
            continue

        lookup_files = []
        sp = firm["standalone_path"]
        if sp is not None:
            lookup_files.append(sp)
        sm = firm["server_module"]
        if sm is not None:
            lookup_files.append(sm.replace(".", os.sep) + ".py")

        found_in = []
        for path in lookup_files:
            full = os.path.join(SCRAPER_DIR, path)
            if not os.path.exists(full):
                continue
            with open(full) as f:
                if env in f.read():
                    found_in.append(path)

        assert found_in, (
            f"{key}: env_var='{env}' not found in any of {lookup_files}"
        )


def test_result_file_in_workflow():
    """manifest result_file이 workflow 파일 안에서 참조되는지 확인."""
    data = _load_manifest()
    for key, firm in data["firms"].items():
        rf = firm["result_file"]
        wp = firm["workflow_path"]
        if wp is None:
            continue
        full = os.path.join(SCRAPER_DIR, wp)
        with open(full) as f:
            content = f.read()
        # basename만 비교 (e.g. /tmp/bnk_result.json → bnk_result.json)
        basename = os.path.basename(rf)
        assert basename in content, (
            f"{key}: result_file basename='{basename}' not found in {wp}"
        )
