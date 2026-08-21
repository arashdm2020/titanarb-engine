"""Arbitrum One configuration loading and validation.

Replaces the legacy Polygon-specific ``bot/config_legacy_polygon.py`` for all
new (Arbitrum multi-DEX) development.

All financial/risk parameters are configurable via environment variables (see
``.env.example``).  Nothing here uses floating point for token amounts; monetary
figures loaded from the environment are parsed into ``Decimal`` and integer
basis-point fields.

Configuration sources
---------------------
1.  ``config/arbitrum.json`` -- immutable on-chain addresses (chain ID, RPC URLs,
    Aave Pool, Uniswap V3, Camelot V3, Chainlink feeds, ArbGasInfo precompile,
    token addresses with decimals).
2.  Environment variables / ``.env`` -- operator-specific runtime parameters
    (private key, contract address, profit recipient, slippage, loan bounds,
    gas multiplier, dry-run flag, etc.).
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from web3 import Web3

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = BASE_DIR / "config" / "arbitrum.json"

# Arbitrum One chain ID -- validated at load time to catch misconfigured RPCs.
ARBITRUM_ONE_CHAIN_ID = 42161

# ArbGasInfo precompile address on every Arbitrum chain.
ARB_GAS_INFO_PRECOMPILE = "0x000000000000000000000000000000000000006C"


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class TokenConfig:
    symbol: str
    address: str
    decimals: int


@dataclass(frozen=True)
class ArbitrumAddresses:
    """All on-chain addresses sourced from ``config/arbitrum.json``."""

    # Chain
    chain_id: int
    http_rpc_url: str
    ws_rpc_url: str

    # Aave V3
    aave_pool: str
    aave_pool_addresses_provider: str

    # Uniswap V3
    uniswap_factory: str
    uniswap_swap_router: str
    uniswap_quoter_v2: str

    # Camelot V3 (Algebra Integral)
    camelot_factory: str
    camelot_swap_router: str
    camelot_quoter: str

    # Chainlink
    chainlink_eth_usd_feed: str
    chainlink_sequencer_uptime_feed: str

    # ArbGasInfo precompile
    arb_gas_info: str

    # Tokens
    tokens: dict[str, TokenConfig] = field(default_factory=dict)


@dataclass(frozen=True)
class Settings:
    """Full runtime settings for the Arbitrum multi-DEX bot."""

    # Operator credentials / contract (from env)
    private_key: str
    contract_address: str
    profit_recipient: str
    flash_executor_address: str

    # Adapter addresses (from env -- deployed in Phase 1)
    uniswap_v3_adapter: str
    camelot_v3_adapter: str

    # Risk / financial parameters (from env)
    slippage_bps: int
    min_profit_usd: Decimal
    min_loan_amount: Decimal
    max_loan_amount: Decimal
    aave_fee_bps: int
    tx_deadline_seconds: int
    gas_limit_multiplier: Decimal
    gas_safety_margin_bps: int
    min_pool_liquidity: int

    # Operational flags (from env)
    dry_run: bool
    execution_mode: str
    market_data_mode: str
    poll_interval_seconds: int
    max_gas_cost_usd: Decimal
    loan_search_steps: int
    log_level: str

    # Sequencer grace period (seconds) -- wait this long after sequencer recovery
    # before trusting price feed data.
    sequencer_grace_period_seconds: int

    # Oracle freshness limits (seconds)
    price_max_staleness_seconds: int
    sequencer_heartbeat_seconds: int

    # On-chain addresses (from config/arbitrum.json)
    addresses: ArbitrumAddresses

    # Convenience accessors --------------------------------------------------
    @property
    def chain_id(self) -> int:
        return self.addresses.chain_id

    @property
    def http_rpc_url(self) -> str:
        return self.addresses.http_rpc_url

    @property
    def ws_rpc_url(self) -> str:
        return self.addresses.ws_rpc_url

    @property
    def tokens(self) -> dict[str, TokenConfig]:
        return self.addresses.tokens

    def __post_init__(self) -> None:
        if self.execution_mode not in {"dry_run", "live"}:
            raise ConfigError("EXECUTION_MODE must be dry_run or live")
        if self.market_data_mode not in {"polling", "wss_preferred"}:
            raise ConfigError("MARKET_DATA_MODE must be polling or wss_preferred")
        if not self.dry_run:
            if not self.private_key:
                raise ConfigError("PRIVATE_KEY is required when DRY_RUN=false")
            if not self.contract_address:
                raise ConfigError("CONTRACT_ADDRESS is required when DRY_RUN=false")
            if not self.profit_recipient:
                raise ConfigError("PROFIT_RECIPIENT is required when DRY_RUN=false")
            if not self.flash_executor_address:
                raise ConfigError(
                    "FLASH_EXECUTOR_ADDRESS is required when DRY_RUN=false"
                )


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #

def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _require_env(name: str) -> str:
    value = os.getenv(name, "")
    return value


def _validate_checksum(address: str, label: str) -> str:
    """Validate that *address* is a valid Ethereum address and return its
    checksum-normalized form.  Raises ``ConfigError`` on invalid input."""
    if not address:
        raise ConfigError(f"{label} is not configured")
    try:
        return Web3.to_checksum_address(address)
    except Exception as exc:
        raise ConfigError(f"{label} is not a valid address: {address}") from exc


def load_arbitrum_config() -> dict:
    """Load and return the raw JSON from ``config/arbitrum.json``."""
    if not CONFIG_PATH.exists():
        raise ConfigError(f"Missing chain configuration file: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _build_addresses(chain_cfg: dict) -> ArbitrumAddresses:
    """Construct ``ArbitrumAddresses`` from the raw JSON config, validating
    every address via checksum normalization."""
    chain = chain_cfg["chain"]
    chain_id = int(chain["chainId"])

    if chain_id != ARBITRUM_ONE_CHAIN_ID:
        raise ConfigError(
            f"Expected Arbitrum One chain ID {ARBITRUM_ONE_CHAIN_ID}, "
            f"got {chain_id}"
        )

    tokens: dict[str, TokenConfig] = {}
    for symbol, t in chain_cfg["tokens"].items():
        if symbol.startswith("_"):
            continue
        tokens[symbol] = TokenConfig(
            symbol=t["symbol"],
            address=_validate_checksum(t["address"], f"token {symbol}"),
            decimals=int(t["decimals"]),
        )

    return ArbitrumAddresses(
        chain_id=chain_id,
        # Public URLs live in the chain config; an operator/fork may override
        # only the endpoint, never the chain/address configuration.
        http_rpc_url=os.getenv("ARBITRUM_RPC_URL", chain["rpcUrls"]["http"]),
        ws_rpc_url=os.getenv("ARBITRUM_WSS_RPC_URL", chain["rpcUrls"]["ws"]),
        aave_pool=_validate_checksum(
            chain_cfg["aaveV3"]["pool"], "aaveV3.pool"
        ),
        aave_pool_addresses_provider=_validate_checksum(
            chain_cfg["aaveV3"]["poolAddressesProvider"],
            "aaveV3.poolAddressesProvider",
        ),
        uniswap_factory=_validate_checksum(
            chain_cfg["uniswapV3"]["factory"], "uniswapV3.factory"
        ),
        # The UniswapV3Adapter calls the ORIGINAL, deadline-bearing SwapRouter
        # (ISwapRouter.exactInputSingle includes a `deadline` field), NOT SwapRouter02.
        # They are distinct contracts with distinct selectors, so this field must resolve
        # to `uniswapV3.swapRouter` (0xE592...1564). We fall back to `swapRouter02` only for
        # backward compatibility with configs/test fixtures predating the `swapRouter` key.
        uniswap_swap_router=_validate_checksum(
            chain_cfg["uniswapV3"].get(
                "swapRouter", chain_cfg["uniswapV3"].get("swapRouter02", "")
            ),
            "uniswapV3.swapRouter",
        ),
        uniswap_quoter_v2=_validate_checksum(
            chain_cfg["uniswapV3"]["quoterV2"], "uniswapV3.quoterV2"
        ),
        camelot_factory=_validate_checksum(
            chain_cfg["camelotV3"]["algebraFactory"], "camelotV3.algebraFactory"
        ),
        camelot_swap_router=_validate_checksum(
            chain_cfg["camelotV3"]["swapRouter"], "camelotV3.swapRouter"
        ),
        camelot_quoter=_validate_checksum(
            chain_cfg["camelotV3"]["quoter"], "camelotV3.quoter"
        ),
        chainlink_eth_usd_feed=_validate_checksum(
            chain_cfg["chainlink"]["ethUsdFeed"], "chainlink.ethUsdFeed"
        ),
        chainlink_sequencer_uptime_feed=_validate_checksum(
            chain_cfg["chainlink"]["sequencerUptimeFeed"],
            "chainlink.sequencerUptimeFeed",
        ),
        arb_gas_info=_validate_checksum(
            chain_cfg["arbGasInfo"]["precompile"], "arbGasInfo.precompile"
        ),
        tokens=tokens,
    )


def load_settings(env_path: Optional[str] = None) -> Settings:
    """Load and validate the full ``Settings`` for the Arbitrum bot.

    Reads ``config/arbitrum.json`` for on-chain addresses and environment
    variables for operator-specific parameters.
    """
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()

    chain_cfg = load_arbitrum_config()
    addresses = _build_addresses(chain_cfg)

    try:
        settings = Settings(
            private_key=os.getenv("PRIVATE_KEY", ""),
            contract_address=os.getenv("CONTRACT_ADDRESS", ""),
            profit_recipient=os.getenv("PROFIT_RECIPIENT", ""),
            flash_executor_address=os.getenv("FLASH_EXECUTOR_ADDRESS", ""),
            uniswap_v3_adapter=os.getenv("UNISWAP_V3_ADAPTER", ""),
            camelot_v3_adapter=os.getenv("CAMELOT_V3_ADAPTER", ""),
            slippage_bps=int(os.getenv("SLIPPAGE_BPS", "50")),
            min_profit_usd=Decimal(os.getenv("MIN_PROFIT_USD", "5")),
            min_loan_amount=Decimal(os.getenv("MIN_LOAN_AMOUNT", "1000")),
            max_loan_amount=Decimal(os.getenv("MAX_LOAN_AMOUNT", "50000")),
            aave_fee_bps=int(os.getenv("AAVE_FEE_BPS", "5")),
            tx_deadline_seconds=int(os.getenv("TX_DEADLINE_SECONDS", "120")),
            gas_limit_multiplier=Decimal(os.getenv("GAS_LIMIT_MULTIPLIER", "1.20")),
            gas_safety_margin_bps=int(os.getenv("GAS_SAFETY_MARGIN_BPS", "1000")),
            min_pool_liquidity=int(os.getenv("MIN_POOL_LIQUIDITY", "1000000000000")),
            dry_run=_env_bool("DRY_RUN", True),
            execution_mode=os.getenv("EXECUTION_MODE", "dry_run").strip().lower(),
            market_data_mode=os.getenv("MARKET_DATA_MODE", "polling").strip().lower(),
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "15")),
            max_gas_cost_usd=Decimal(os.getenv("MAX_GAS_COST_USD", "10")),
            loan_search_steps=int(os.getenv("LOAN_SEARCH_STEPS", "8")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            sequencer_grace_period_seconds=int(
                os.getenv("SEQUENCER_GRACE_PERIOD_SECONDS", "3600")
            ),
            price_max_staleness_seconds=int(
                os.getenv("PRICE_MAX_STALENESS_SECONDS", "3600")
            ),
            sequencer_heartbeat_seconds=int(
                os.getenv("SEQUENCER_HEARTBEAT_SECONDS", "0")
            ),
            addresses=addresses,
        )
    except (ValueError, KeyError) as exc:
        raise ConfigError(f"Invalid configuration: {exc}") from exc

    # Validate operator-provided addresses (env) when not dry-run
    if not settings.dry_run:
        _validate_checksum(settings.contract_address, "CONTRACT_ADDRESS")
        _validate_checksum(settings.profit_recipient, "PROFIT_RECIPIENT")
        _validate_checksum(
            settings.flash_executor_address, "FLASH_EXECUTOR_ADDRESS"
        )
        if settings.uniswap_v3_adapter:
            _validate_checksum(settings.uniswap_v3_adapter, "UNISWAP_V3_ADAPTER")
        if settings.camelot_v3_adapter:
            _validate_checksum(settings.camelot_v3_adapter, "CAMELOT_V3_ADAPTER")

    # Sanity checks on financial parameters
    if settings.min_loan_amount <= 0 or settings.max_loan_amount <= settings.min_loan_amount:
        raise ConfigError("MIN_LOAN_AMOUNT/MAX_LOAN_AMOUNT misconfigured")
    if settings.slippage_bps < 0 or settings.slippage_bps > 10_000:
        raise ConfigError("SLIPPAGE_BPS out of range")
    if settings.gas_safety_margin_bps < 0:
        raise ConfigError("GAS_SAFETY_MARGIN_BPS must be non-negative")
    if settings.poll_interval_seconds < 5:
        raise ConfigError("POLL_INTERVAL_SECONDS must be at least 5")
    if settings.max_gas_cost_usd <= 0:
        raise ConfigError("MAX_GAS_COST_USD must be positive")
    if settings.sequencer_grace_period_seconds < 0:
        raise ConfigError("SEQUENCER_GRACE_PERIOD_SECONDS must be non-negative")

    return settings
