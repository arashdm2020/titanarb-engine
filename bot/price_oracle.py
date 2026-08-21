"""Independent USD price oracle for gas-cost conversion.

CRITICAL AUDIT CONTEXT: gas is paid in the chain's native token (MATIC/POL on Polygon), but
profitability is denominated in the borrowed asset (USDC). These are different assets and must
never be subtracted directly (`profit_usdc - gas_matic` is meaningless). This module provides an
INDEPENDENT price source for the native token so that gas cost can be correctly converted into
USD (and from there into the borrowed asset's units), rather than deriving that price from the
same Uniswap V3 pool being arbitraged (which would be circular and vulnerable to exactly the kind
of thin-liquidity / price-impact / manipulation risk this system is trying to price in).

We use Chainlink's MATIC/USD Data Feed on Polygon mainnet:
    0xAB594600376Ec9fD91F8e885dADF0CE036862dE0
(verified via data.chain.link -- Chainlink's official price feed directory).

Note: for the borrowed asset (USDC) side of the conversion, this codebase currently assumes
USDC ~= 1.00 USD (a standard, widely-used approximation for a major fiat-backed stablecoin) rather
than reading a second USDC/USD Chainlink feed. This assumption is documented as a known limitation
in the README -- I could not fully verify, within this audit, an authoritative Polygon USDC/USD
Chainlink feed contract address, and the project's explicit rule is to state that instead of
guessing an address.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from web3 import Web3
from web3.contract import Contract

ABI_DIR = Path(__file__).resolve().parent / "abi"

# Verified via https://data.chain.link/feeds/polygon/mainnet/matic-usd
MATIC_USD_CHAINLINK_FEED_POLYGON = "0xAB594600376Ec9fD91F8e885dADF0CE036862dE0"

# If a feed's `updatedAt` is older than this, treat the price as stale and refuse to use it
# rather than silently using a possibly-outdated value for a financial decision.
MAX_PRICE_STALENESS_SECONDS = 3600


def _load_abi(name: str) -> list:
    with open(ABI_DIR / name, "r", encoding="utf-8") as fh:
        return json.load(fh)


CHAINLINK_FEED_ABI = _load_abi("chainlink_feed.json")


class PriceOracleError(Exception):
    """Raised when a price cannot be obtained or is stale. Callers must treat this as
    'do not proceed with a gas-cost-dependent profitability decision', never fall back to a
    hardcoded/fake price.
    """


@dataclass(frozen=True)
class OraclePrice:
    price_usd: Decimal
    updated_at: int
    age_seconds: int


def get_chainlink_price_usd(
    w3: Web3, feed_address: str, max_staleness_seconds: int = MAX_PRICE_STALENESS_SECONDS
) -> OraclePrice:
    """Reads a Chainlink `AggregatorV3Interface`-compatible feed's latest USD price.

    Raises PriceOracleError if the feed returns a non-positive answer or a stale update --
    both are treated as "no reliable price available right now".
    """
    feed: Contract = w3.eth.contract(address=Web3.to_checksum_address(feed_address), abi=CHAINLINK_FEED_ABI)

    try:
        decimals = feed.functions.decimals().call()
        round_data = feed.functions.latestRoundData().call()
    except Exception as exc:
        raise PriceOracleError(f"Failed to read Chainlink feed {feed_address}: {exc}") from exc

    _round_id, answer, _started_at, updated_at, _answered_in_round = round_data

    if answer <= 0:
        raise PriceOracleError(f"Chainlink feed {feed_address} returned non-positive answer: {answer}")

    age_seconds = int(time.time()) - int(updated_at)
    if age_seconds > max_staleness_seconds:
        raise PriceOracleError(
            f"Chainlink feed {feed_address} price is stale: last updated {age_seconds}s ago "
            f"(max allowed {max_staleness_seconds}s)"
        )

    price_usd = Decimal(answer) / (Decimal(10) ** decimals)
    return OraclePrice(price_usd=price_usd, updated_at=int(updated_at), age_seconds=age_seconds)


def get_native_token_usd_price(w3: Web3) -> OraclePrice:
    """Convenience wrapper for the Polygon native-token (MATIC/POL) USD price."""
    return get_chainlink_price_usd(w3, MATIC_USD_CHAINLINK_FEED_POLYGON)
