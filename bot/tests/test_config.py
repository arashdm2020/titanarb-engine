"""Unit tests for bot.config_legacy_polygon loading/validation (legacy Polygon bot,
relocated to make room for the new bot/config/ Arbitrum package -- see
bot/config/settings.py for the Arbitrum-native equivalent)."""
import os

import pytest

from bot.config_legacy_polygon import ConfigError, load_settings


@pytest.fixture
def base_env(monkeypatch, tmp_path):
    monkeypatch.setenv("WSS_RPC_URL", "wss://example.invalid")
    monkeypatch.setenv("HTTP_RPC_URL", "https://example.invalid")
    monkeypatch.setenv("PRIVATE_KEY", "")
    monkeypatch.setenv("CONTRACT_ADDRESS", "")
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("MIN_LOAN_AMOUNT", "1000")
    monkeypatch.setenv("MAX_LOAN_AMOUNT", "50000")
    monkeypatch.setenv("SLIPPAGE_BPS", "50")
    yield monkeypatch


def test_load_settings_dry_run_ok(base_env):
    settings = load_settings()
    assert settings.dry_run is True
    assert settings.slippage_bps == 50
    assert "USDC" in settings.tokens


def test_load_settings_requires_private_key_when_live(base_env):
    base_env.setenv("DRY_RUN", "false")
    base_env.setenv("PRIVATE_KEY", "")
    with pytest.raises(ConfigError):
        load_settings()


def test_load_settings_requires_profit_recipient_when_live(base_env):
    base_env.setenv("DRY_RUN", "false")
    base_env.setenv("PRIVATE_KEY", "0x" + "1" * 64)
    base_env.setenv("CONTRACT_ADDRESS", "0x" + "2" * 40)
    base_env.setenv("PROFIT_RECIPIENT", "")
    with pytest.raises(ConfigError):
        load_settings()


def test_load_settings_rejects_bad_loan_range(base_env):
    base_env.setenv("MIN_LOAN_AMOUNT", "5000")
    base_env.setenv("MAX_LOAN_AMOUNT", "1000")
    with pytest.raises(ConfigError):
        load_settings()


def test_load_settings_rejects_bad_slippage(base_env):
    base_env.setenv("SLIPPAGE_BPS", "20000")
    with pytest.raises(ConfigError):
        load_settings()
