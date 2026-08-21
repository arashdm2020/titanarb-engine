"""Unit tests for bot.price_oracle -- gas-cost USD conversion must come from an independent
oracle (Chainlink), never a hardcoded constant, and must reject stale/invalid data.
"""
from __future__ import annotations

import time
from decimal import Decimal

import pytest

from bot import price_oracle as po


class _StubCall:
    def __init__(self, value):
        self._value = value

    def call(self, *_a, **_k):
        return self._value


class _StubFunctions:
    def __init__(self, decimals: int, round_data: tuple):
        self._decimals = decimals
        self._round_data = round_data

    def decimals(self):
        return _StubCall(self._decimals)

    def latestRoundData(self):
        return _StubCall(self._round_data)


class _StubFeedContract:
    def __init__(self, decimals: int, round_data: tuple):
        self.functions = _StubFunctions(decimals, round_data)


class _StubW3:
    def __init__(self, contract):
        self._contract = contract
        self.eth = self

    def contract(self, address, abi):
        return self._contract


def test_get_chainlink_price_usd_parses_fresh_price():
    now = int(time.time())
    # answer = 0.75 USD scaled to 8 decimals
    stub_contract = _StubFeedContract(decimals=8, round_data=(1, 75_000_000, now, now, 1))
    w3 = _StubW3(stub_contract)

    result = po.get_chainlink_price_usd(w3, "0x000000000000000000000000000000000000dead")
    assert result.price_usd == Decimal("0.75")
    assert result.age_seconds < 5


def test_get_chainlink_price_usd_rejects_stale_price():
    old = int(time.time()) - 100_000
    stub_contract = _StubFeedContract(decimals=8, round_data=(1, 75_000_000, old, old, 1))
    w3 = _StubW3(stub_contract)

    with pytest.raises(po.PriceOracleError):
        po.get_chainlink_price_usd(w3, "0x000000000000000000000000000000000000dead", max_staleness_seconds=3600)


def test_get_chainlink_price_usd_rejects_non_positive_answer():
    now = int(time.time())
    stub_contract = _StubFeedContract(decimals=8, round_data=(1, 0, now, now, 1))
    w3 = _StubW3(stub_contract)

    with pytest.raises(po.PriceOracleError):
        po.get_chainlink_price_usd(w3, "0x000000000000000000000000000000000000dead")


def test_get_chainlink_price_usd_wraps_rpc_failures():
    class _BrokenFunctions:
        def decimals(self):
            raise ConnectionError("rpc down")

    class _BrokenContract:
        functions = _BrokenFunctions()

    w3 = _StubW3(_BrokenContract())
    with pytest.raises(po.PriceOracleError):
        po.get_chainlink_price_usd(w3, "0x000000000000000000000000000000000000dead")
