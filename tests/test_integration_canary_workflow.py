"""Static contract tests for the integration-canary workflow.

These tests verify YAML structure and Python skip logic without
requiring a live PostgreSQL connection or secrets.
"""
import os
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


# ──────────────────────────────────────────────────────────
# Workflow YAML contract
# ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def workflow():
    path = ROOT / ".github" / "workflows" / "integration-canary.yml"
    if not path.exists():
        pytest.skip("Workflow file not found")
    with open(path) as f:
        return yaml.safe_load(f)


def test_workflow_has_secret_validation_step(workflow):
    """Secret-presence validation step must exist before any DB access."""
    steps = workflow["jobs"]["canary"]["steps"]
    validate_step = None
    for step in steps:
        if step.get("name") == "Validate CANARY_POSTGRES secrets are configured":
            validate_step = step
            break
    assert validate_step is not None, (
        "Missing 'Validate CANARY_POSTGRES secrets are configured' step. "
        "This step must exist to fail-fast when CANARY_POSTGRES_* secrets are empty."
    )
    run_cmd = validate_step.get("run", "")
    assert "exit 1" in run_cmd, "Validation step must exit non-zero on missing secrets"


def test_workflow_rejects_production_targets(workflow):
    """Production target names must be rejected before any test runs."""
    steps = workflow["jobs"]["canary"]["steps"]
    reject_step = None
    for step in steps:
        if step.get("name") == "Reject production targets":
            reject_step = step
            break
    assert reject_step is not None, "Missing 'Reject production targets' step"
    run_cmd = reject_step.get("run", "")
    assert "prod" in run_cmd.lower() or "production" in run_cmd.lower()


def test_workflow_read_only_step_exists(workflow):
    """The read-only PostgreSQL step must be present."""
    steps = workflow["jobs"]["canary"]["steps"]
    pg_steps = [s for s in steps if "read-only" in (s.get("name") or "").lower()]
    assert len(pg_steps) >= 1, "Missing read-only PostgreSQL check step"


def test_workflow_db_import_is_opt_in(workflow):
    """DB import (write) must be gated behind allow_db_import + confirmation."""
    steps = workflow["jobs"]["canary"]["steps"]
    refuse_step = None
    import_step = None
    for step in steps:
        name = step.get("name", "")
        if "Refuse database import by default" in name:
            refuse_step = step
        if "Run explicitly approved" in name:
            import_step = step
    assert refuse_step is not None, "Missing 'Refuse database import by default' step"
    assert import_step is not None, "Missing explicitly approved import step"

    import_cond = import_step.get("if", "")
    assert "allow_db_import" in import_cond
    assert "CANARY_NON_PROD_IMPORT" in import_cond


# ──────────────────────────────────────────────────────────
# test_db_logic.py skip logic contract
# ──────────────────────────────────────────────────────────

def _get_skipif_condition():
    """정규식으로 test_db_logic.py에서 skipif 조건 문자열을 추출 (import 없이 정적 분석)."""
    import re
    path = ROOT / "tests" / "test_db_logic.py"
    text = path.read_text()
    # Capture: @pytest.mark.skipif(<condition>, reason=...)
    m = re.search(
        r'@pytest\.mark\.skipif\(([^,]+(?:\([^)]*\)[^,]*)*)\s*,',
        text,
    )
    if m:
        return m.group(1).strip()
    return None


def test_db_logic_skips_in_ci_unless_canary():
    """skipif condition must be False when ENV=canary + CI."""
    condition = _get_skipif_condition()
    assert condition is not None, "skipif decorator not found on test_db_connection_and_structure"
    should_skip = eval(condition, {"IS_CI": True, "IS_CANARY": True})
    assert not should_skip, (
        f"skipif condition '{condition}' should be False when ENV=canary and GITHUB_ACTIONS=true"
    )


def test_db_logic_skips_in_generic_ci():
    """skipif condition must be True in generic CI without canary env."""
    condition = _get_skipif_condition()
    assert condition is not None, "skipif decorator not found on test_db_connection_and_structure"
    should_skip = eval(condition, {"IS_CI": True, "IS_CANARY": False})
    assert should_skip, (
        f"skipif condition '{condition}' should be True in generic CI without ENV=canary"
    )
