"""Unit tests for bot.price_oracle_arbitrum (sequencer checks + ETH/USD oracle)."""
from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import patch, MagicMock

import pytest

from bot.price_oracle import PriceOracleError, OraclePrice
from bot.price_oracle_arbitrum import (
    SequencerStatus,
    check_sequencer_status,
    get_eth_usd_price,
    get_safe_eth_usd_price,
)


class _StubCall:
    def __init__(self, return_value):
        self._return_value = return_value

    def call(self, *_a, **_k):
        return self._return_value


class _StubFunctions:
    def __init__(self, round_data=None, decimals=8, raise_on_call=False):
        self._round_data = round_data
        self._decimals = decimals
        self._raise = raise_on_call

    def latestRoundData(self):
        if self._raise:
            raise Exception("rpc error")
        return _StubCall(self._round_data)

    def decimals(self):
        return _StubCall(self._decimals)


class _StubFeedContract:
    def __init__(self, round_data=None, decimals=8, raise_on_call=False):
        self.functions = _StubFunctions(round_data, decimals, raise_on_call)


class _StubW3:
    def __init__(self, contract):
        self._contract = contract
        self.eth = self

    def contract(self, address, abi):
        return self._contract


# --- Sequencer status tests ---

def test_check_sequencer_status_up_and_grace_elapsed():
    now = int(time.time())
    # Sequencer up (answer=0), status changed 4000s ago (grace=3600)
    round_data = (1, 0, now - 4000, now - 30, 1)
    w3 = _StubW3(_StubFeedContract(round_data=round_data))

    result = check_sequencer_status(
        w3, "0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
        grace_period_seconds=3600,
        current_timestamp=now,
    )
    assert result.is_up is True
    assert result.grace_period_elapsed is True


def test_check_sequencer_status_down():
    now = int(time.time())
    round_data = (1, 1, now - 100, now - 30, 1)  # answer=1 -> DOWN
    w3 = _StubW3(_StubFeedContract(round_data=round_data))

    with pytest.raises(PriceOracleError, match="sequencer is DOWN"):
        check_sequencer_status(
            w3, "0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
            current_timestamp=now,
        )


def test_check_sequencer_status_grace_not_elapsed():
    now = int(time.time())
    # Sequencer just came up 100s ago, grace period is 3600s
    round_data = (1, 0, now - 100, now - 30, 1)
    w3 = _StubW3(_StubFeedContract(round_data=round_data))

    with pytest.raises(PriceOracleError, match="grace period not elapsed"):
        check_sequencer_status(
            w3, "0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
            grace_period_seconds=3600,
            current_timestamp=now,
        )


def test_check_sequencer_status_stale_feed():
    now = int(time.time())
    # Feed updated 5000s ago, heartbeat is 1800s
    round_data = (1, 0, now - 5000, now - 5000, 1)
    w3 = _StubW3(_StubFeedContract(round_data=round_data))

    with pytest.raises(PriceOracleError, match="stale"):
        check_sequencer_status(
            w3, "0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
            grace_period_seconds=3600,
            heartbeat_seconds=1800,
            current_timestamp=now,
        )


def test_check_sequencer_status_uninitialized():
    # startedAt = 0 (uninitialized on Arbitrum)
    round_data = (1, 0, 0, 0, 1)
    w3 = _StubW3(_StubFeedContract(round_data=round_data))

    with pytest.raises(PriceOracleError, match="not initialized"):
        check_sequencer_status(
            w3, "0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
            current_timestamp=int(time.time()),
        )


def test_check_sequencer_status_invalid_round():
    now = int(time.time())
    # answeredInRound < roundId -> invalid round
    round_data = (5, 0, now - 4000, now - 30, 3)
    w3 = _StubW3(_StubFeedContract(round_data=round_data))

    with pytest.raises(PriceOracleError, match="round is stale"):
        check_sequencer_status(
            w3, "0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
            current_timestamp=now,
        )


def test_check_sequencer_status_rpc_failure():
    w3 = _StubW3(_StubFeedContract(raise_on_call=True))

    with pytest.raises(PriceOracleError, match="Failed to read"):
        check_sequencer_status(
            w3, "0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
        )


# --- ETH/USD price tests ---

def test_get_eth_usd_price_parses_fresh_price():
    now = int(time.time())
    # answer = 3000 USD scaled to 8 decimals
    round_data = (1, 300_000_000_000, now, now, 1)
    w3 = _StubW3(_StubFeedContract(round_data=round_data, decimals=8))

    result = get_eth_usd_price(w3, "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612")
    assert result.price_usd == Decimal("3000")
    assert result.age_seconds < 5


def test_get_eth_usd_price_rejects_stale():
    old = int(time.time()) - 100_000
    round_data = (1, 300_000_000_000, old, old, 1)
    w3 = _StubW3(_StubFeedContract(round_data=round_data, decimals=8))

    with pytest.raises(PriceOracleError):
        get_eth_usd_price(
            w3, "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
            max_staleness_seconds=3600,
        )


# --- Safe ETH/USD price (with sequencer check) ---

def test_get_safe_eth_usd_price_success():
    now = int(time.time())
    # Sequencer up, grace elapsed
    seq_round = (1, 0, now - 4000, now - 30, 1)
    # ETH/USD = 3000
    eth_round = (1, 300_000_000_000, now, now, 1)

    # Address-aware stub: returns different contracts based on feed address
    seq_addr = "0xFdB631F5EE196F0ed6FAa767959853A9F217697D".lower()
    eth_addr = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612".lower()

    class _AddressAwareW3:
        def __init__(self):
            self.eth = self

        def contract(self, address, abi):
            addr = address.lower()
            if addr == seq_addr:
                return _StubFeedContract(round_data=seq_round)
            elif addr == eth_addr:
                return _StubFeedContract(round_data=eth_round, decimals=8)
            raise ValueError(f"unexpected address: {address}")

    w3 = _AddressAwareW3()

    result = get_safe_eth_usd_price(
        w3,
        eth_usd_feed_address="0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
        sequencer_feed_address="0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
    )
    assert result.price_usd == Decimal("3000")


def test_get_safe_eth_usd_price_blocks_when_sequencer_down():
    now = int(time.time())
    seq_round = (1, 1, now - 100, now - 30, 1)  # DOWN
    eth_round = (1, 300_000_000_000, now, now, 1)

    seq_addr = "0xFdB631F5EE196F0ed6FAa767959853A9F217697D".lower()
    eth_addr = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612".lower()

    class _AddressAwareW3:
        def __init__(self):
            self.eth = self

        def contract(self, address, abi):
            addr = address.lower()
            if addr == seq_addr:
                return _StubFeedContract(round_data=seq_round)
            elif addr == eth_addr:
                return _StubFeedContract(round_data=eth_round, decimals=8)
            raise ValueError(f"unexpected address: {address}")

    w3 = _AddressAwareW3()

    with pytest.raises(PriceOracleError, match="sequencer is DOWN"):
        get_safe_eth_usd_price(
            w3,
            eth_usd_feed_address="0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
            sequencer_feed_address="0xFdB631F5EE196F0ed6FAa767959853A9F217697D",
        )
