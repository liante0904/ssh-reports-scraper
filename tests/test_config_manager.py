import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))


def fresh_config(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("urls", raising=False)

    from models.ConfigManager import ConfigManager

    ConfigManager._instance = None
    return ConfigManager()


def test_get_urls_allows_missing_config_source(monkeypatch, tmp_path):
    config = fresh_config(monkeypatch, tmp_path)

    assert config.get_urls("MissingFirm") == []


def test_get_urls_raises_when_loaded_urls_missing_key(monkeypatch, tmp_path):
    config = fresh_config(monkeypatch, tmp_path)
    monkeypatch.setenv("urls", json.dumps({"LS_0": ["https://example.test/list"]}))

    from models.ConfigManager import MissingConfigError

    with pytest.raises(MissingConfigError, match="Missing urls config"):
        config.get_urls("Samsung_5")


def test_get_urls_uses_default_for_optional_lookup(monkeypatch, tmp_path):
    config = fresh_config(monkeypatch, tmp_path)
    monkeypatch.setenv("urls", json.dumps({"LS_0": ["https://example.test/list"]}))

    assert config.get_urls("OptionalFirm", default=["fallback"]) == ["fallback"]


def test_env_defaults_to_production(monkeypatch, tmp_path):
    monkeypatch.delenv("ENV", raising=False)
    config = fresh_config(monkeypatch, tmp_path)
    assert config.ENV == "production"


def test_env_resolves_dev(monkeypatch, tmp_path):
    monkeypatch.setenv("ENV", "dev")
    config = fresh_config(monkeypatch, tmp_path)
    assert config.ENV == "dev"


def test_env_resolves_production(monkeypatch, tmp_path):
    monkeypatch.setenv("ENV", "production")
    config = fresh_config(monkeypatch, tmp_path)
    assert config.ENV == "production"

    monkeypatch.setenv("ENV", "prod")
    config = fresh_config(monkeypatch, tmp_path)
    assert config.ENV == "production"


def test_fallback_to_legacy_prod_section(monkeypatch, tmp_path):
    # secrets.json에 'prod'만 있고 'production'이 없는 상태 구현
    secrets_dir = tmp_path / "secrets" / "ssh-reports-scraper"
    secrets_dir.mkdir(parents=True)
    secrets_file = secrets_dir / "secrets.json"
    
    test_secrets = {
        "common": {
            "TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET": "common_bot_token",
            "TELEGRAM_CHANNEL_ID_REPORT_ALARM": "common_channel_id"
        },
        "dev": {
            "BOT_TOKEN": "dev_bot_token",
            "CHANNEL_ID": "dev_channel_id"
        },
        "prod": {
            "BOT_TOKEN": "legacy_prod_bot_token",
            "CHANNEL_ID": "legacy_prod_channel_id"
        }
    }
    
    secrets_file.write_text(json.dumps(test_secrets), encoding="utf-8")
    
    # HOME 디렉토리를 가상 경로로 바꾸어 ~/.secrets/... 대신 임시 경로를 참조하게 조율
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("ENV", "production")
    
    # ConfigManager 인스턴스 초기화
    from models.ConfigManager import ConfigManager
    ConfigManager._instance = None
    config = ConfigManager()
    
    # production 환경에서 secrets.json의 'prod' 설정값을 올바르게 폴백하여 가져오는지 검증
    assert config.ENV == "production"
    assert config.BOT_TOKEN == "legacy_prod_bot_token"
    assert config.CHANNEL_ID == "legacy_prod_channel_id"
