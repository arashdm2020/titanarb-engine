"""Configuration loading and validation for the LEGACY Polygon single-DEX bot.

NOTE (Arbitrum migration): this module is the original Polygon-only configuration
loader, relocated verbatim from `bot/config.py` to make room for the new
`bot/config/` package (Arbitrum multi-DEX configuration -- see
`bot/config/settings.py`). It is kept only so the original Polygon orchestration
(`bot/main.py`, `bot/arbitrage.py`, `bot/pools.py`, `bot/optimizer.py`) and its
existing test coverage continue to function unchanged; it is not part of the
Arbitrum migration and is superseded by `bot/config/` for all new development.

All financial/risk parameters are configurable via environment variables (see
`.env.example`). Nothing here uses floating point for token amounts; monetary
figures loaded from the environment are parsed into `Decimal` and integer
basis-point fields.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "polygon.json"


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class TokenConfig:
    symbol: str
    address: str
    decimals: int


@dataclass(frozen=True)
class RouteLeg:
    token_in: str
    token_out: str
    fee_candidates_bps: tuple[int, ...]


@dataclass(frozen=True)
class RouteConfig:
    name: str
    legs: tuple[RouteLeg, ...]


@dataclass(frozen=True)
class ChainAddresses:
    aave_pool: str
    aave_pool_addresses_provider: str
    uniswap_factory: str
    uniswap_swap_router: str
    uniswap_quoter_v2: str


@dataclass(frozen=True)
class Settings:
    wss_rpc_url: str
    http_rpc_url: str
    private_key: str
    contract_address: str
    profit_recipient: str
    chain_id: int

    slippage_bps: int
    min_profit_usd: Decimal

    min_loan_amount: Decimal
    max_loan_amount: Decimal

    aave_fee_bps: int
    tx_deadline_seconds: int
    gas_limit_multiplier: Decimal
    min_pool_liquidity: int

    dry_run: bool
    loan_search_steps: int
    log_level: str

    addresses: ChainAddresses
    tokens: dict[str, TokenConfig]
    routes: tuple[RouteConfig, ...]

    def __post_init__(self) -> None:
        if not self.dry_run:
            # Extra defensive checks only required for live execution.
            if not self.private_key:
                raise ConfigError("PRIVATE_KEY is required when DRY_RUN=false")
            if not self.contract_address:
                raise ConfigError("CONTRACT_ADDRESS is required when DRY_RUN=false")
            if not self.profit_recipient:
                raise ConfigError("PROFIT_RECIPIENT is required when DRY_RUN=false")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _require(name: str, allow_empty_if_dry_run: bool = False) -> str:
    value = os.getenv(name, "")
    return value


def load_polygon_config() -> dict:
    if not CONFIG_PATH.exists():
        raise ConfigError(f"Missing chain configuration file: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_settings(env_path: Optional[str] = None) -> Settings:
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()

    chain_cfg = load_polygon_config()

    addresses = ChainAddresses(
        aave_pool=chain_cfg["aaveV3"]["pool"],
        aave_pool_addresses_provider=chain_cfg["aaveV3"]["poolAddressesProvider"],
        uniswap_factory=chain_cfg["uniswapV3"]["factory"],
        uniswap_swap_router=chain_cfg["uniswapV3"]["swapRouter"],
        uniswap_quoter_v2=chain_cfg["uniswapV3"]["quoterV2"],
    )

    tokens: dict[str, TokenConfig] = {}
    for symbol, t in chain_cfg["tokens"].items():
        if symbol.startswith("_"):
            continue
        tokens[symbol] = TokenConfig(symbol=t["symbol"], address=t["address"], decimals=int(t["decimals"]))

    routes = tuple(
        RouteConfig(
            name=route_cfg.get("name", f"route-{i}"),
            legs=tuple(
                RouteLeg(
                    token_in=leg["tokenIn"],
                    token_out=leg["tokenOut"],
                    fee_candidates_bps=tuple(int(f) for f in leg["feeTierCandidatesBps"]),
                )
                for leg in route_cfg["legs"]
            ),
        )
        for i, route_cfg in enumerate(chain_cfg["routes"])
    )

    try:
        settings = Settings(
            wss_rpc_url=_require("WSS_RPC_URL"),
            http_rpc_url=_require("HTTP_RPC_URL"),
            private_key=os.getenv("PRIVATE_KEY", ""),
            contract_address=os.getenv("CONTRACT_ADDRESS", ""),
            profit_recipient=os.getenv("PROFIT_RECIPIENT", ""),
            chain_id=int(os.getenv("CHAIN_ID", "137")),
            slippage_bps=int(os.getenv("SLIPPAGE_BPS", "50")),
            min_profit_usd=Decimal(os.getenv("MIN_PROFIT_USD", "5")),
            min_loan_amount=Decimal(os.getenv("MIN_LOAN_AMOUNT", "1000")),
            max_loan_amount=Decimal(os.getenv("MAX_LOAN_AMOUNT", "50000")),
            aave_fee_bps=int(os.getenv("AAVE_FEE_BPS", "5")),
            tx_deadline_seconds=int(os.getenv("TX_DEADLINE_SECONDS", "60")),
            gas_limit_multiplier=Decimal(os.getenv("GAS_LIMIT_MULTIPLIER", "1.20")),
            min_pool_liquidity=int(os.getenv("MIN_POOL_LIQUIDITY", "1000000000000")),
            dry_run=_env_bool("DRY_RUN", True),
            loan_search_steps=int(os.getenv("LOAN_SEARCH_STEPS", "8")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            addresses=addresses,
            tokens=tokens,
            routes=routes,
        )
    except (ValueError, KeyError) as exc:
        raise ConfigError(f"Invalid configuration: {exc}") from exc

    if not settings.wss_rpc_url:
        raise ConfigError("WSS_RPC_URL is required")
    if not settings.http_rpc_url:
        raise ConfigError("HTTP_RPC_URL is required")
    if settings.min_loan_amount <= 0 or settings.max_loan_amount <= settings.min_loan_amount:
        raise ConfigError("MIN_LOAN_AMOUNT/MAX_LOAN_AMOUNT misconfigured")
    if settings.slippage_bps < 0 or settings.slippage_bps > 10_000:
        raise ConfigError("SLIPPAGE_BPS out of range")

    return settings
