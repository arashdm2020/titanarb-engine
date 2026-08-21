"""Unit tests for bot.gas_arbitrum (Arbitrum gas estimation)."""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from bot.gas_arbitrum import (
    ArbitrumGasBreakdown,
    GasEstimationError,
    estimate_arbitrum_gas_cost,
    estimate_calldata_size,
    get_arb_gas_prices,
    L1_CALLDATA_UNIT_SIZE,
)


class _StubCall:
    def __init__(self, return_value):
        self._return_value = return_value

    def call(self, *_a, **_k):
        return self._return_value


class _StubFunctions:
    def __init__(self, return_value):
        self._return_value = return_value

    def getPricesInWei(self):
        return _StubCall(self._return_value)


class _StubContract:
    def __init__(self, return_value):
        self.functions = _StubFunctions(return_value)


class _StubW3:
    def __init__(self, contract):
        self._contract = contract
        self.eth = self

    def contract(self, address, abi):
        return self._contract


def _make_stub_w3(prices=(13_000_000_000, 1_000_000_000, 0, 0, 0, 100_000_000)):
    """Create a stub Web3 with configurable ArbGasInfo prices.

    Default values are realistic-ish Arbitrum gas prices in wei:
    - perL2Tx: 13 gwei * 1e9 = 13,000,000,000
    - perL1CalldataUnit: 1 gwei * 1e9 = 1,000,000,000
    - perArbGasTotal: 100,000,000 (0.1 gwei)
    """
    return _StubW3(_StubContract(list(prices)))


def test_get_arb_gas_prices():
    w3 = _make_stub_w3()
    per_l2_tx, per_l1_calldata_unit, per_arb_gas_total = get_arb_gas_prices(
        w3, "0x000000000000000000000000000000000000006C"
    )
    assert per_l2_tx == 13_000_000_000
    assert per_l1_calldata_unit == 1_000_000_000
    assert per_arb_gas_total == 100_000_000


def test_get_arb_gas_prices_raises_on_failure():
    class _BrokenFunctions:
        def getPricesInWei(self):
            return _StubCall(None)  # Will fail when unpacking

    class _BrokenContract:
        functions = _BrokenFunctions()

    class _BrokenCall:
        def call(self, *_a, **_k):
            raise Exception("precompile error")

    class _BrokenFunctions2:
        def getPricesInWei(self):
            return _BrokenCall()

    class _BrokenContract2:
        functions = _BrokenFunctions2()

    w3 = _StubW3(_BrokenContract2())
    with pytest.raises(GasEstimationError, match="Failed to read ArbGasInfo"):
        get_arb_gas_prices(w3, "0x000000000000000000000000000000000000006C")


def test_estimate_calldata_size_hex_string():
    tx = {"data": "0xabcdef"}  # 3 bytes
    assert estimate_calldata_size(tx) == 3


def test_estimate_calldata_size_bytes():
    tx = {"data": b"\x01\x02\x03\x04"}  # 4 bytes
    assert estimate_calldata_size(tx) == 4


def test_estimate_calldata_size_empty():
    tx = {"data": "0x"}
    assert estimate_calldata_size(tx) == 0


def test_estimate_calldata_size_no_data():
    tx = {}
    assert estimate_calldata_size(tx) == 0


def test_estimate_arbitrum_gas_cost_basic():
    """Test that gas estimation produces correct L2 + L1 breakdown."""
    w3 = _make_stub_w3()
    tx = {"data": "0x" + "ab" * 160}  # 160 bytes of calldata

    result = estimate_arbitrum_gas_cost(
        w3,
        tx,
        gas_limit=500_000,
        precompile_address="0x000000000000000000000000000000000000006C",
        safety_margin_bps=1000,  # 10%
    )

    # L2 fee = per_l2_tx + gas_limit * per_arb_gas_total
    expected_l2 = 13_000_000_000 + (500_000 * 100_000_000)
    assert result.l2_execution_fee_wei == expected_l2

    # L1 fee: 160 bytes / 16 = 10 units
    calldata_units = (160 + L1_CALLDATA_UNIT_SIZE - 1) // L1_CALLDATA_UNIT_SIZE
    expected_l1 = calldata_units * 1_000_000_000
    assert result.l1_data_fee_wei == expected_l1

    # Raw total
    expected_raw = expected_l2 + expected_l1
    assert result.raw_total_wei == expected_raw

    # Safety margin: 10% of raw
    expected_margin = expected_raw // 10  # approximately
    assert abs(result.safety_margin_wei - expected_margin) <= 1

    # Total
    assert result.total_gas_cost_wei == result.raw_total_wei + result.safety_margin_wei

    # Gas cost in ETH
    assert result.gas_cost_eth > Decimal(0)

    # No USD price provided
    assert result.gas_cost_usd is None


def test_estimate_arbitrum_gas_cost_with_usd():
    w3 = _make_stub_w3()
    tx = {"data": "0x" + "ab" * 160}
    eth_price = Decimal("3000.00")

    result = estimate_arbitrum_gas_cost(
        w3,
        tx,
        gas_limit=500_000,
        precompile_address="0x000000000000000000000000000000000000006C",
        safety_margin_bps=0,
        eth_usd_price=eth_price,
    )

    assert result.gas_cost_usd is not None
    assert result.gas_cost_usd == result.gas_cost_eth * eth_price


def test_estimate_arbitrum_gas_cost_calldata_rounding():
    """Test that calldata is rounded up to 16-byte units."""
    w3 = _make_stub_w3()
    tx = {"data": "0x" + "ab" * 17}  # 17 bytes -> 2 units (16 + 1)

    result = estimate_arbitrum_gas_cost(
        w3,
        tx,
        gas_limit=100_000,
        precompile_address="0x000000000000000000000000000000000000006C",
        safety_margin_bps=0,
    )

    # 17 bytes -> ceil(17/16) = 2 units
    expected_l1 = 2 * 1_000_000_000
    assert result.l1_data_fee_wei == expected_l1
    assert result.calldata_size == 17


def test_estimate_arbitrum_gas_cost_zero_safety_margin():
    w3 = _make_stub_w3()
    tx = {"data": "0x" + "ab" * 32}

    result = estimate_arbitrum_gas_cost(
        w3,
        tx,
        gas_limit=100_000,
        precompile_address="0x000000000000000000000000000000000000006C",
        safety_margin_bps=0,
    )

    assert result.safety_margin_wei == 0
    assert result.total_gas_cost_wei == result.raw_total_wei
