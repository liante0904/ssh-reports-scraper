import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.validate_scrape_result import validate


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def _write(tmp_path, filename, data):
    path = tmp_path / filename
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def test_manifest_allow_empty_returns_success(tmp_path):
    path = _write(tmp_path, "bnk_result.json", [])
    assert validate(path, firm_identifier="bnk") == 0


def test_manifest_require_non_empty_rejects_empty(tmp_path):
    path = _write(tmp_path, "kb_result.json", [])
    assert validate(path, firm_identifier="kb") == 1


def test_unknown_firm_fails(tmp_path):
    path = _write(tmp_path, "unknown.json", [])
    assert validate(path, firm_identifier="unknown") == 2


def test_missing_firm_name_fails_schema_validation(tmp_path):
    path = _write(tmp_path, "kb_result.json", [{
        "report_unique_key": "key",
        "report_date": "20260710",
    }])
    assert validate(path, firm_identifier="kb") == 2


def test_existing_workflow_filename_resolves_firm(tmp_path):
    path = _write(tmp_path, "kb_result.json", [{
        "report_unique_key": "key",
        "report_date": "20260710",
        "firm_nm": "KB증권",
    }])
    assert validate(path) == 0


def test_workflow_validators_use_uv_environment():
    validator_workflows = []
    for workflow in WORKFLOWS.glob("*.yml"):
        contents = workflow.read_text(encoding="utf-8")
        if "scripts/validate_scrape_result.py" not in contents:
            continue
        validator_workflows.append(workflow.name)
        assert "uv run python scripts/validate_scrape_result.py" in contents
        assert any(
            setup in contents
            for setup in ("pip install uv", "astral-sh/setup-uv", "uv/install.sh")
        )

    assert validator_workflows, "no artifact validation workflows found"
