"""Executable quote engine.

TRADE-OFF NOTES (required reading before modifying this module):

1. `slot0()` spot price
   - Free (already fetched for pool monitoring), instantaneous.
   - WRONG for sizing a real trade: it ignores price impact entirely. Using
     `price_a * price_b * price_c > 1` as the sole profitability signal (as this
     project's requirements explicitly forbid) will produce false positives for
     any trade size large enough to move the pool.

2. Uniswap QuoterV2 (`quoteExactInputSingle` / `quoteExactInput`)
   - Simulates the actual swap against current on-chain state (including tick
     crossings), returning the amount a real `exactInputSingle` call would
     currently produce, INCLUDING price impact and the pool's fee.
   - Implemented as a non-`view` function that reverts internally and is only
     usable via `eth_call` (no state is actually changed). This is what we use
     here.
   - Still has the same staleness risk as any other read: the pool state can
     change between this call and actual execution (see README "MEV / Execution
     Risk").

3. Direct mathematical simulation (replicating Uniswap's tick-crossing math in
   Python)
   - Most accurate/fastest if implemented correctly and if you already have
     fresh tick/liquidity data cached, but requires re-implementing a
     nontrivial and easy-to-get-subtly-wrong part of Uniswap V3's math
     (word-boished bitmap of initialized ticks, in particular).
   - NOT implemented in this codebase; QuoterV2 is used instead as the "safest
     practical approach" for a production-oriented first version, since it
     defers to Uniswap's own reference math rather than a reimplementation.

This module therefore prefers QuoterV2 for all executable-quote calculations
used in profitability decisions.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from web3 import Web3
from web3.contract import Contract

ABI_DIR = Path(__file__).resolve().parent / "abi"


def _load_abi(name: str) -> list:
    with open(ABI_DIR / name, "r", encoding="utf-8") as fh:
        return json.load(fh)


QUOTER_V2_ABI = _load_abi("quoter_v2.json")
CAMELOT_QUOTER_ABI = _load_abi("camelot_quoter.json")


class QuoteError(Exception):
    pass


@dataclass(frozen=True)
class ExecutableQuote:
    amount_in: int
    amount_out: int
    sqrt_price_x96_after: int
    gas_estimate: int


@dataclass(frozen=True)
class CamelotExecutableQuote:
    """Executable single-hop quote from Camelot V3's Algebra quoter.

    Camelot's deployed quoter is not ABI-compatible with Uniswap QuoterV2:
    it takes no fee tier and returns the pool's dynamic fee rather than a gas
    estimate.  Keep this separate so callers cannot accidentally apply the
    Uniswap ABI to Camelot.
    """

    amount_in: int
    amount_out: int
    fee: int


def get_executable_quote(
    w3: Web3,
    quoter_address: str,
    token_in: str,
    token_out: str,
    fee: int,
    amount_in: int,
) -> ExecutableQuote:
    """Obtain an executable quote for a single Uniswap V3 leg via QuoterV2, using
    `eth_call` (no state changes, no gas spent on-chain). Raises `QuoteError` on
    failure so callers can treat a failed quote as "skip this candidate" rather
    than crashing the whole bot.
    """
    quoter: Contract = w3.eth.contract(address=Web3.to_checksum_address(quoter_address), abi=QUOTER_V2_ABI)
    params = (
        Web3.to_checksum_address(token_in),
        Web3.to_checksum_address(token_out),
        int(amount_in),
        int(fee),
        0,
    )
    try:
        amount_out, sqrt_price_after, _ticks_crossed, gas_estimate = quoter.functions.quoteExactInputSingle(
            params
        ).call()
    except Exception as exc:
        raise QuoteError(f"QuoterV2 call failed for {token_in}->{token_out} fee={fee} amountIn={amount_in}: {exc}") from exc

    return ExecutableQuote(
        amount_in=amount_in,
        amount_out=int(amount_out),
        sqrt_price_x96_after=int(sqrt_price_after),
        gas_estimate=int(gas_estimate),
    )


def get_camelot_v3_executable_quote(
    w3: Web3,
    quoter_address: str,
    token_in: str,
    token_out: str,
    amount_in: int,
    limit_sqrt_price: int = 0,
) -> CamelotExecutableQuote:
    """Obtain an executable Camelot V3 / Algebra single-hop quote via ``eth_call``.

    The Arbitrum Camelot quoter implements
    ``quoteExactInputSingle(address,address,uint256,uint160)`` and returns
    ``(uint256 amountOut, uint16 fee)``.  Its dynamic fee is discovered by the
    quoter; it must not be supplied by the caller as a Uniswap fee tier.
    """
    quoter: Contract = w3.eth.contract(
        address=Web3.to_checksum_address(quoter_address), abi=CAMELOT_QUOTER_ABI
    )
    try:
        amount_out, fee = quoter.functions.quoteExactInputSingle(
            Web3.to_checksum_address(token_in),
            Web3.to_checksum_address(token_out),
            int(amount_in),
            int(limit_sqrt_price),
        ).call()
    except Exception as exc:
        raise QuoteError(
            f"Camelot V3 quoter call failed for {token_in}->{token_out} "
            f"amountIn={amount_in}: {exc}"
        ) from exc

    return CamelotExecutableQuote(
        amount_in=int(amount_in), amount_out=int(amount_out), fee=int(fee)
    )


def compute_amount_out_minimum(quoted_amount_out: int, slippage_bps: int) -> int:
    """amountOutMinimum = quotedAmountOut * (10000 - slippageBps) / 10000

    Uses integer arithmetic, rounding DOWN (floor division), so the minimum
    acceptable output is never accidentally rounded UP above what slippage
    tolerance actually allows -- rounding down here is the safe direction
    because it can only make the constraint looser by a sub-unit amount, never
    stricter than intended, and never causes the contract to demand more than
    the trader intended to tolerate.
    """
    if slippage_bps < 0 or slippage_bps > 10_000:
        raise ValueError("slippage_bps out of range [0, 10000]")
    return (quoted_amount_out * (10_000 - slippage_bps)) // 10_000
