"""Triangular-route profitability calculation.

All amounts are integer base units (no floats). Human-readable figures (USD
estimates) use `Decimal`. Profitability is computed strictly from executable
quotes for the actual candidate trade size -- never from spot price products
alone (`priceA * priceB * priceC > 1` is explicitly NOT used, per project
requirements).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from web3 import Web3

from .config_legacy_polygon import RouteConfig, RouteLeg, Settings, TokenConfig
from .pools import PoolState, find_best_pool
from .quotes import ExecutableQuote, QuoteError, compute_amount_out_minimum, get_executable_quote


class NoLiquidPoolError(Exception):
    pass


@dataclass(frozen=True)
class LegResult:
    token_in: str
    token_out: str
    fee: int
    pool_address: str
    amount_in: int
    quoted_amount_out: int
    amount_out_minimum: int


@dataclass(frozen=True)
class RouteEvaluation:
    initial_amount: int
    final_amount: int
    legs: tuple[LegResult, ...]
    gross_profit: int  # base units of borrowed asset
    aave_fee: int  # base units of borrowed asset
    gas_cost_native: Decimal
    gas_cost_asset_equivalent: int  # gas cost converted into borrowed-asset base units (approx)
    net_profit: int  # base units of borrowed asset

    @property
    def is_profitable(self) -> bool:
        return self.net_profit > 0


def resolve_route_pools(
    w3: Web3, settings: Settings, tokens_by_symbol: dict[str, TokenConfig], route: RouteConfig
) -> list[tuple[RouteLeg, PoolState]]:
    """Resolve a live, liquid Uniswap V3 pool for every leg of a single route.
    Raises NoLiquidPoolError with a clear reason if any leg cannot be resolved --
    never fabricates a pool.
    """
    resolved: list[tuple[RouteLeg, PoolState]] = []
    for leg in route.legs:
        token_in = tokens_by_symbol[leg.token_in]
        token_out = tokens_by_symbol[leg.token_out]
        try:
            pool = find_best_pool(
                w3,
                settings.addresses.uniswap_factory,
                token_in.address,
                token_out.address,
                leg.fee_candidates_bps,
                min_liquidity=settings.min_pool_liquidity,
            )
        except Exception as exc:
            raise NoLiquidPoolError(f"Leg {leg.token_in}->{leg.token_out}: {exc}") from exc
        resolved.append((leg, pool))
    return resolved


def evaluate_route_for_amount(
    w3: Web3,
    settings: Settings,
    tokens_by_symbol: dict[str, TokenConfig],
    resolved_legs: list[tuple[RouteLeg, PoolState]],
    initial_amount: int,
    aave_fee_bps_onchain: Optional[int],
    gas_cost_native: Decimal,
    native_usd_price: Optional[Decimal],
    borrowed_asset_usd_price: Optional[Decimal],
) -> RouteEvaluation:
    """Evaluate the full 3-leg route for a specific candidate `initial_amount`
    (in base units of the borrowed asset), using fresh executable quotes for
    each leg in sequence (leg2's quote uses leg1's quoted output as its input,
    etc).

    Raises QuoteError if any leg's executable quote cannot be obtained -- the
    caller (optimizer) should treat that as "this candidate amount is not
    currently evaluable" and continue with other candidates rather than
    crashing.
    """
    amount = initial_amount
    legs: list[LegResult] = []

    for leg, pool in resolved_legs:
        token_in = tokens_by_symbol[leg.token_in]
        token_out = tokens_by_symbol[leg.token_out]

        quote: ExecutableQuote = get_executable_quote(
            w3, settings.addresses.uniswap_quoter_v2, token_in.address, token_out.address, pool.fee, amount
        )
        amount_out_minimum = compute_amount_out_minimum(quote.amount_out, settings.slippage_bps)

        legs.append(
            LegResult(
                token_in=token_in.address,
                token_out=token_out.address,
                fee=pool.fee,
                pool_address=pool.address,
                amount_in=amount,
                quoted_amount_out=quote.amount_out,
                amount_out_minimum=amount_out_minimum,
            )
        )
        amount = quote.amount_out

    final_amount = amount
    gross_profit = final_amount - initial_amount

    fee_bps = aave_fee_bps_onchain if aave_fee_bps_onchain is not None else settings.aave_fee_bps
    aave_fee = (initial_amount * fee_bps) // 10_000

    gas_cost_asset_equivalent = 0
    if native_usd_price is not None and borrowed_asset_usd_price is not None and borrowed_asset_usd_price > 0:
        gas_cost_usd = gas_cost_native * native_usd_price
        borrowed_asset_decimals = tokens_by_symbol[resolved_legs[0][0].token_in].decimals
        gas_cost_asset_equivalent = int(
            (gas_cost_usd / borrowed_asset_usd_price) * (Decimal(10) ** borrowed_asset_decimals)
        )

    net_profit = gross_profit - aave_fee - gas_cost_asset_equivalent

    return RouteEvaluation(
        initial_amount=initial_amount,
        final_amount=final_amount,
        legs=tuple(legs),
        gross_profit=gross_profit,
        aave_fee=aave_fee,
        gas_cost_native=gas_cost_native,
        gas_cost_asset_equivalent=gas_cost_asset_equivalent,
        net_profit=net_profit,
    )
