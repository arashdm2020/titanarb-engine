"""Optimal flash-loan amount search.

Profitability is NOT linear in loan size: price impact grows as trade size
grows, so `netProfit(amount)` is typically concave (rises, peaks, then falls or
turns negative). This module searches for an approximately-optimal amount
using a coarse grid search followed by a local refinement pass around the best
candidate found.

Candidates are evaluated in parallel using a thread pool to reduce wall-clock
latency from sequential RPC quoter calls.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from web3 import Web3

from .arbitrage import RouteEvaluation, evaluate_route_for_amount
from .config_legacy_polygon import Settings, TokenConfig
from .pools import PoolState
from .quotes import QuoteError


@dataclass(frozen=True)
class OptimizationResult:
    best_evaluation: Optional[RouteEvaluation]
    candidates_evaluated: int
    candidates_skipped: int


def _candidate_amounts(min_amount: int, max_amount: int, steps: int) -> list[int]:
    if steps < 2:
        steps = 2
    step_size = (max_amount - min_amount) // (steps - 1)
    if step_size <= 0:
        return [min_amount, max_amount]
    return sorted({min_amount + step_size * i for i in range(steps)} | {max_amount})


def find_optimal_loan_amount(
    w3: Web3,
    settings: Settings,
    tokens_by_symbol: dict[str, TokenConfig],
    resolved_legs: list[tuple],
    aave_fee_bps_onchain: Optional[int],
    gas_cost_native: Decimal,
    native_usd_price: Optional[Decimal],
    borrowed_asset_usd_price: Optional[Decimal],
    borrowed_asset_decimals: int,
) -> OptimizationResult:
    min_units = int(settings.min_loan_amount * (10 ** borrowed_asset_decimals))
    max_units = int(settings.max_loan_amount * (10 ** borrowed_asset_decimals))

    candidates = _candidate_amounts(min_units, max_units, settings.loan_search_steps)

    evaluated = 0
    skipped = 0

    def evaluate(amount: int) -> Optional[RouteEvaluation]:
        nonlocal evaluated, skipped
        try:
            result = evaluate_route_for_amount(
                w3,
                settings,
                tokens_by_symbol,
                resolved_legs,
                amount,
                aave_fee_bps_onchain,
                gas_cost_native,
                native_usd_price,
                borrowed_asset_usd_price,
            )
            evaluated += 1
            return result
        except QuoteError:
            skipped += 1
            return None

    # --- Coarse grid pass (parallel) ---
    with ThreadPoolExecutor(max_workers=min(8, len(candidates))) as pool:
        coarse_results = list(pool.map(evaluate, candidates))

    best: Optional[RouteEvaluation] = None
    for result in coarse_results:
        if result is None:
            continue
        if best is None or result.net_profit > best.net_profit:
            best = result

    # --- Local refinement pass around the best coarse candidate ---
    if best is not None:
        coarse_step = candidates[1] - candidates[0] if len(candidates) > 1 else 0
        if coarse_step > 0:
            refine_step = max(coarse_step // 4, 1)
            refine_candidates = [
                amt for amt in [
                    best.initial_amount - 2 * refine_step,
                    best.initial_amount - refine_step,
                    best.initial_amount + refine_step,
                    best.initial_amount + 2 * refine_step,
                ] if min_units <= amt <= max_units
            ]
            if refine_candidates:
                with ThreadPoolExecutor(max_workers=min(4, len(refine_candidates))) as pool:
                    refine_results = list(pool.map(evaluate, refine_candidates))
                for result in refine_results:
                    if result is None:
                        continue
                    if result.net_profit > best.net_profit:
                        best = result

    return OptimizationResult(best_evaluation=best, candidates_evaluated=evaluated, candidates_skipped=skipped)
