"""Unit tests for pure/offline-computable logic in bot.quotes and bot.optimizer.

These do not require network access or a running node -- they test integer
arithmetic correctness only. Run with: pytest bot/tests
"""
import pytest
from unittest.mock import MagicMock

from bot.quotes import compute_amount_out_minimum, get_camelot_v3_executable_quote
from bot.optimizer import _candidate_amounts


def test_compute_amount_out_minimum_basic():
    # 50 bps slippage on 1,000,000 units -> 995,000
    assert compute_amount_out_minimum(1_000_000, 50) == 995_000


def test_compute_amount_out_minimum_zero_slippage():
    assert compute_amount_out_minimum(1_000_000, 0) == 1_000_000


def test_compute_amount_out_minimum_full_slippage():
    assert compute_amount_out_minimum(1_000_000, 10_000) == 0


def test_compute_amount_out_minimum_rounds_down():
    # 3 * 9999 / 10000 = 2.9997 -> floors to 2, never rounds up above tolerance
    assert compute_amount_out_minimum(3, 1) == 2


def test_compute_amount_out_minimum_rejects_out_of_range():
    with pytest.raises(ValueError):
        compute_amount_out_minimum(1000, -1)
    with pytest.raises(ValueError):
        compute_amount_out_minimum(1000, 10_001)


def test_candidate_amounts_includes_bounds():
    candidates = _candidate_amounts(1000, 5000, 5)
    assert candidates[0] == 1000
    assert candidates[-1] == 5000
    assert len(candidates) <= 5


def test_candidate_amounts_handles_small_range():
    candidates = _candidate_amounts(1000, 1001, 8)
    assert 1000 in candidates
    assert 1001 in candidates


def test_camelot_quote_uses_algebra_signature_and_dynamic_fee():
    quoter = MagicMock()
    quoter.functions.quoteExactInputSingle.return_value.call.return_value = (123456, 217)
    w3 = MagicMock()
    w3.eth.contract.return_value = quoter

    quote = get_camelot_v3_executable_quote(
        w3,
        "0x0000000000000000000000000000000000000001",
        "0x0000000000000000000000000000000000000002",
        "0x0000000000000000000000000000000000000003",
        1000,
    )

    assert quote.amount_in == 1000
    assert quote.amount_out == 123456
    assert quote.fee == 217
    quoter.functions.quoteExactInputSingle.assert_called_once()
