"""Docker image packaging guards."""
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "Dockerfile"


def _dockerfile_text() -> str:
    return DOCKERFILE.read_text(encoding="utf-8")


def test_dockerfile_copies_runtime_manifest_config():
    """The scraper registry needs config/firms.yaml inside /app at runtime."""
    text = _dockerfile_text()

    assert (ROOT / "config" / "firms.yaml").exists()
    assert "COPY --chown=appuser:appgroup config/ ./config/" in text


def test_dockerfile_copies_all_runtime_source_directories():
    text = _dockerfile_text()

    for dirname in ("run", "models", "utils", "modules", "scrapers", "config"):
        assert (ROOT / dirname).is_dir(), f"{dirname}/ must exist in repo"
        assert f"COPY --chown=appuser:appgroup {dirname}/ ./{dirname}/" in text
