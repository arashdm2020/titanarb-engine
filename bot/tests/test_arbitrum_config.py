"""Unit tests for bot.config (Arbitrum configuration loading/validation)."""
from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from bot.config import ConfigError, load_arbitrum_config, load_settings
from bot.config.settings import ArbitrumAddresses, TokenConfig, _build_addresses, _validate_checksum


CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "arbitrum.json"


def test_load_arbitrum_config_returns_valid_json():
    cfg = load_arbitrum_config()
    assert cfg["chain"]["chainId"] == 42161
    assert "aaveV3" in cfg
    assert "uniswapV3" in cfg
    assert "camelotV3" in cfg
    assert "chainlink" in cfg
    assert "tokens" in cfg


def test_build_addresses_validates_chain_id():
    bad_cfg = {
        "chain": {"name": "wrong", "chainId": 137, "rpcUrls": {"http": "", "ws": ""}},
        "aaveV3": {"pool": "0x" + "1" * 40, "poolAddressesProvider": "0x" + "2" * 40},
        "uniswapV3": {"factory": "0x" + "3" * 40, "swapRouter02": "0x" + "4" * 40, "quoterV2": "0x" + "5" * 40},
        "camelotV3": {"algebraFactory": "0x" + "6" * 40, "swapRouter": "0x" + "7" * 40, "quoter": "0x" + "8" * 40},
        "chainlink": {"ethUsdFeed": "0x" + "9" * 40, "sequencerUptimeFeed": "0x" + "a" * 40},
        "arbGasInfo": {"precompile": "0x000000000000000000000000000000000000006C"},
        "tokens": {},
    }
    with pytest.raises(ConfigError, match="Expected Arbitrum One chain ID"):
        _build_addresses(bad_cfg)


def test_build_addresses_loads_all_components():
    cfg = load_arbitrum_config()
    addresses = _build_addresses(cfg)
    assert addresses.chain_id == 42161
    assert addresses.aave_pool == "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
    assert addresses.uniswap_factory == "0x1F98431c8aD98523631AE4a59f267346ea31F984"
    assert addresses.camelot_factory == "0x1a3c9B1d2F0529D97f2afC5136Cc23e58f1FD35B"
    assert addresses.chainlink_eth_usd_feed == "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"
    assert addresses.chainlink_sequencer_uptime_feed == "0xFdB631F5EE196F0ed6FAa767959853A9F217697D"
    assert addresses.arb_gas_info == "0x000000000000000000000000000000000000006C"
    assert "WETH" in addresses.tokens
    assert "USDC" in addresses.tokens
    assert "ARB" in addresses.tokens
    assert "USDT" in addresses.tokens


def test_build_addresses_token_decimals():
    cfg = load_arbitrum_config()
    addresses = _build_addresses(cfg)
    assert addresses.tokens["WETH"].decimals == 18
    assert addresses.tokens["USDC"].decimals == 6
    assert addresses.tokens["USDT"].decimals == 6
    assert addresses.tokens["ARB"].decimals == 18


def test_validate_checksum_rejects_invalid():
    with pytest.raises(ConfigError, match="not a valid address"):
        _validate_checksum("0xinvalid", "test")


def test_validate_checksum_rejects_empty():
    with pytest.raises(ConfigError, match="not configured"):
        _validate_checksum("", "test")


def test_load_settings_dry_run_defaults(monkeypatch):
    """load_settings should succeed in dry-run mode with minimal env vars."""
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("MIN_PROFIT_USD", "5")
    monkeypatch.setenv("SLIPPAGE_BPS", "50")
    monkeypatch.setenv("GAS_SAFETY_MARGIN_BPS", "1000")
    monkeypatch.setenv("SEQUENCER_GRACE_PERIOD_SECONDS", "3600")
    monkeypatch.delenv("PRIVATE_KEY", raising=False)
    monkeypatch.delenv("CONTRACT_ADDRESS", raising=False)
    monkeypatch.delenv("PROFIT_RECIPIENT", raising=False)
    monkeypatch.delenv("FLASH_EXECUTOR_ADDRESS", raising=False)

    settings = load_settings()
    assert settings.dry_run is True
    assert settings.chain_id == 42161
    assert settings.slippage_bps == 50
    assert settings.min_profit_usd == Decimal("5")
    assert settings.gas_safety_margin_bps == 1000
    assert settings.sequencer_grace_period_seconds == 3600


def test_load_settings_rejects_bad_loan_range(monkeypatch):
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("MIN_LOAN_AMOUNT", "50000")
    monkeypatch.setenv("MAX_LOAN_AMOUNT", "1000")
    with pytest.raises(ConfigError, match="misconfigured"):
        load_settings()


def test_load_settings_rejects_bad_slippage(monkeypatch):
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("SLIPPAGE_BPS", "99999")
    with pytest.raises(ConfigError, match="out of range"):
        load_settings()
