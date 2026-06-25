import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("harness", ROOT / "scripts" / "harness.py")
harness = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(harness)


def test_check_manifest_accepts_valid_enums():
    firms = {
        "kb": {
            "mode": "dual",
            "config_shape": "full_config",
            "empty_policy": "require_non_empty",
        }
    }

    assert harness.check_manifest(firms) == []


def test_check_manifest_rejects_invalid_mode():
    firms = {
        "kb": {
            "mode": "bad",
            "config_shape": "full_config",
            "empty_policy": "require_non_empty",
        }
    }

    assert "kb: invalid mode 'bad'" in harness.check_manifest(firms)


def test_check_manifest_rejects_invalid_config_shape():
    firms = {
        "kb": {
            "mode": "dual",
            "config_shape": "bad",
            "empty_policy": "require_non_empty",
        }
    }

    assert "kb: invalid config_shape 'bad'" in harness.check_manifest(firms)


def test_check_files_reports_missing_core(monkeypatch):
    firms = {
        "kb": {
            "core_module": "scrapers.missing_core",
            "standalone_path": None,
            "server_module": "modules.KBsec_4",
            "workflow_path": None,
        }
    }
    original_exists = Path.exists

    def fake_exists(path):
        if str(path).endswith("scrapers/missing_core.py"):
            return False
        return original_exists(path)

    monkeypatch.setattr(Path, "exists", fake_exists)

    assert "kb: core_module not found: scrapers/missing_core.py" in harness.check_files(firms)


def test_main_list_prints_firm_keys(tmp_path, capsys):
    manifest = tmp_path / "firms.yaml"
    manifest.write_text(
        """
firms:
  kb:
    mode: dual
    config_shape: full_config
    empty_policy: require_non_empty
  ls:
    mode: ga
    config_shape: url_list
    empty_policy: require_non_empty
""",
        encoding="utf-8",
    )

    assert harness.main(["--list", "--manifest", str(manifest)]) == 0

    assert capsys.readouterr().out.splitlines() == ["kb", "ls"]


def test_main_check_manifest_does_not_touch_files(tmp_path):
    manifest = tmp_path / "firms.yaml"
    manifest.write_text(
        """
firms:
  kb:
    mode: dual
    config_shape: full_config
    empty_policy: require_non_empty
    core_module: scrapers.missing_core
""",
        encoding="utf-8",
    )

    assert harness.main(["--check-manifest", "--manifest", str(manifest)]) == 0
