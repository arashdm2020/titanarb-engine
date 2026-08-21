"""Arbitrum-specific price oracle with sequencer safety checks.

Extends the legacy Chainlink price oracle (``bot/price_oracle.py``) with
Arbitrum-specific safety checks:

1. **Sequencer Uptime Feed**: Before trusting any price data, the oracle
   verifies that the Arbitrum sequencer is up (answer == 0) and that the
   grace period has elapsed since the last status change.  This prevents
   using stale prices during / after a sequencer outage.

2. **ETH/USD Chainlink Feed**: Reads the ETH/USD price for gas-cost
   conversion (gas is paid in ETH on Arbitrum).

3. **Oracle freshness / heartbeat**: Validates that the price feed's
   ``updatedAt`` is within the configured staleness window and that
   ``answeredInRound >= roundId`` (round validity).

All safety checks raise ``PriceOracleError`` on failure -- callers must
treat this as "do not proceed with a gas-cost-dependent profitability
decision".
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from web3 import Web3
from web3.contract import Contract

from .logger import get_logger, log_fields
from .price_oracle import PriceOracleError, OraclePrice, _load_abi, CHAINLINK_FEED_ABI

logger = get_logger(__name__)

# Default grace period after sequencer recovery (seconds).
# Chainlink recommends 3600s (1 hour) on Arbitrum.
DEFAULT_SEQUENCER_GRACE_PERIOD = 3600

# The uptime feed is updated when sequencer status changes, not on a fixed
# heartbeat. A mandatory freshness threshold would reject a healthy sequencer
# after a long stable period, so it is disabled by default. Tests/operators may
# opt into one explicitly if their feed policy requires it.
DEFAULT_SEQUENCER_HEARTBEAT = 0

# Default max staleness for ETH/USD price (seconds).
DEFAULT_PRICE_MAX_STALENESS = 3600


@dataclass(frozen=True)
class SequencerStatus:
    """Result of checking the Arbitrum sequencer uptime feed."""

    is_up: bool
    answer: int
    started_at: int
    updated_at: int
    time_since_status_change: int
    grace_period_elapsed: bool


def check_sequencer_status(
    w3: Web3,
    sequencer_feed_address: str,
    grace_period_seconds: int = DEFAULT_SEQUENCER_GRACE_PERIOD,
    heartbeat_seconds: int = DEFAULT_SEQUENCER_HEARTBEAT,
    current_timestamp: int | None = None,
) -> SequencerStatus:
    """Read the Chainlink L2 Sequencer Uptime Feed and validate sequencer status.

    The sequencer uptime feed returns:
      - answer == 0: sequencer is UP
      - answer == 1: sequencer is DOWN

    After the sequencer comes back up, we must wait ``grace_period_seconds``
    before trusting price data, because stale prices may persist briefly.

    Raises ``PriceOracleError`` if:
      - The feed call fails
      - The sequencer is down (answer == 1)
      - The grace period has not elapsed since the last status change
      - The feed data is stale when an explicit positive heartbeat is supplied
      - The round is invalid (answeredInRound < roundId)
    """
    feed: Contract = w3.eth.contract(
        address=Web3.to_checksum_address(sequencer_feed_address),
        abi=CHAINLINK_FEED_ABI,
    )

    try:
        round_data = feed.functions.latestRoundData().call()
    except Exception as exc:
        raise PriceOracleError(
            f"Failed to read sequencer uptime feed {sequencer_feed_address}: {exc}"
        ) from exc

    round_id, answer, started_at, updated_at, answered_in_round = round_data

    # On Arbitrum, startedAt can be 0 before initialization
    if started_at == 0:
        raise PriceOracleError(
            f"Sequencer uptime feed not initialized (startedAt=0)"
        )

    # Round validity check
    if int(answered_in_round) < int(round_id):
        raise PriceOracleError(
            f"Sequencer uptime feed round is stale: "
            f"roundId={round_id}, answeredInRound={answered_in_round}"
        )

    now = current_timestamp if current_timestamp is not None else int(time.time())
    time_since_change = now - int(started_at)

    # `updatedAt` is the last sequencer status transition, not a periodic
    # heartbeat. Only enforce this optional policy when explicitly requested.
    age = now - int(updated_at)
    if heartbeat_seconds > 0 and age > heartbeat_seconds:
        raise PriceOracleError(
            f"Sequencer uptime feed is stale: last updated {age}s ago "
            f"(heartbeat {heartbeat_seconds}s)"
        )

    is_up = int(answer) == 0

    if not is_up:
        raise PriceOracleError(
            f"Arbitrum sequencer is DOWN (answer={answer}). "
            f"Refusing to use price data during sequencer outage."
        )

    grace_elapsed = time_since_change >= grace_period_seconds
    if not grace_elapsed:
        raise PriceOracleError(
            f"Sequencer grace period not elapsed: "
            f"{time_since_change}s since status change, "
            f"need {grace_period_seconds}s. Prices may be stale."
        )

    log_fields(
        logger,
        10,
        "Sequencer status OK",
        answer=answer,
        time_since_status_change=time_since_change,
        grace_period=grace_period_seconds,
    )

    return SequencerStatus(
        is_up=True,
        answer=int(answer),
        started_at=int(started_at),
        updated_at=int(updated_at),
        time_since_status_change=time_since_change,
        grace_period_elapsed=True,
    )


def get_eth_usd_price(
    w3: Web3,
    eth_usd_feed_address: str,
    max_staleness_seconds: int = DEFAULT_PRICE_MAX_STALENESS,
) -> OraclePrice:
    """Read the ETH/USD Chainlink price feed on Arbitrum.

    This is a convenience wrapper around ``price_oracle.get_chainlink_price_usd``
    that uses the verified Arbitrum ETH/USD feed address.

    Raises ``PriceOracleError`` if the price is stale, non-positive, or the
    feed call fails.
    """
    from .price_oracle import get_chainlink_price_usd

    return get_chainlink_price_usd(
        w3, eth_usd_feed_address, max_staleness_seconds
    )


def get_safe_eth_usd_price(
    w3: Web3,
    eth_usd_feed_address: str,
    sequencer_feed_address: str,
    max_staleness_seconds: int = DEFAULT_PRICE_MAX_STALENESS,
    sequencer_grace_period: int = DEFAULT_SEQUENCER_GRACE_PERIOD,
    sequencer_heartbeat: int = DEFAULT_SEQUENCER_HEARTBEAT,
) -> OraclePrice:
    """Read ETH/USD price with full Arbitrum safety checks.

    This is the primary entry point for gas-cost conversion in the Arbitrum bot.
    It first verifies the sequencer is up and the grace period has elapsed,
    then reads the ETH/USD price feed with staleness validation.

    Raises ``PriceOracleError`` on any safety check failure.
    """
    # Step 1: Verify sequencer status
    check_sequencer_status(
        w3,
        sequencer_feed_address,
        grace_period_seconds=sequencer_grace_period,
        heartbeat_seconds=sequencer_heartbeat,
    )

    # Step 2: Read ETH/USD price
    return get_eth_usd_price(
        w3, eth_usd_feed_address, max_staleness_seconds
    )
