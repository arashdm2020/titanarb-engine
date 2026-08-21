"""Constrained Arbitrum multi-DEX discovery, quoting, and dry-run evaluation.

This is deliberately not a graph engine. It enumerates only 2--4-hop cycles
from the configured base asset through the configured intermediate token set,
using only real pools discovered from the configured Uniswap and Camelot
factories.
"""
from __future__ import annotations

import itertools
import os
import time
from dataclasses import dataclass
from dataclasses import replace
from decimal import Decimal
from typing import Iterable

from web3 import Web3

from .config import Settings
from .executor_arbitrum import SwapStepArg, encode_camelot_v3_data, encode_uniswap_v3_data
from .gas_arbitrum import estimate_arbitrum_gas_cost
from .logger import get_logger, log_fields
from .pools import POOL_ABI, read_pool_state, resolve_pool_address
from .quotes import (
    QuoteError,
    compute_amount_out_minimum,
    get_camelot_v3_executable_quote,
    get_executable_quote,
)

logger = get_logger(__name__)

ALGEBRA_FACTORY_ABI = [{
    "inputs": [{"name": "tokenA", "type": "address"}, {"name": "tokenB", "type": "address"}],
    "name": "poolByPair", "outputs": [{"name": "pool", "type": "address"}],
    "stateMutability": "view", "type": "function",
}]
ALGEBRA_POOL_LIQUIDITY_ABI = [{
    "inputs": [], "name": "liquidity", "outputs": [{"type": "uint128"}],
    "stateMutability": "view", "type": "function",
}]
AAVE_PREMIUM_ABI = [{
    "inputs": [], "name": "FLASHLOAN_PREMIUM_TOTAL", "outputs": [{"type": "uint128"}],
    "stateMutability": "view", "type": "function",
}]


@dataclass(frozen=True)
class ArbitrumPool:
    dex: str
    token_in: str
    token_out: str
    address: str
    fee: int | None
    liquidity: int


@dataclass(frozen=True)
class ArbitrumHop:
    pool: ArbitrumPool
    amount_in: int
    amount_out: int
    amount_out_minimum: int


@dataclass(frozen=True)
class ArbitrumRoute:
    hops: tuple[ArbitrumPool, ...]


@dataclass(frozen=True)
class RouteEvaluation:
    route: ArbitrumRoute
    hops: tuple[ArbitrumHop, ...]
    amount_in: int
    final_amount: int
    premium: int
    gas_cost_asset: int
    min_profit: int
    expected_net_profit: int

    @property
    def is_profitable(self) -> bool:
        return self.expected_net_profit > self.min_profit


class ArbitrumPipeline:
    def __init__(self, w3: Web3, settings: Settings):
        self.w3 = w3
        self.settings = settings
        raw = settings.addresses
        cfg = settings.tokens
        import json
        from pathlib import Path
        chain = json.loads((Path(__file__).resolve().parent.parent / "config" / "arbitrum.json").read_text())
        route_cfg = chain["routeDiscovery"]
        self.base_symbol = route_cfg["baseAsset"]
        self.intermediate_symbols = tuple(route_cfg["intermediateTokens"])
        self.fee_tiers = tuple(int(x) for x in route_cfg["uniswapFeeTiers"])
        self._addresses = raw
        self._tokens = cfg
        self._pair_cache: dict[tuple[str, str], tuple[ArbitrumPool, ...]] = {}

    def _pool_state(self, address: str):
        return read_pool_state(self.w3, address)

    def _camelot_liquidity(self, address: str) -> int:
        """Read the Algebra-compatible subset required for Camelot selection.

        Camelot V3 pools expose ``liquidity()``, but are not Uniswap V3 pools:
        their global state is not the Uniswap ``fee()/slot0()`` ABI consumed by
        :func:`read_pool_state`.  Discovery only needs liquidity because the
        router determines Algebra's dynamic fee at swap time.
        """
        pool = self.w3.eth.contract(address=address, abi=ALGEBRA_POOL_LIQUIDITY_ABI)
        return int(pool.functions.liquidity().call())

    def discover_pair(self, token_in: str, token_out: str) -> list[ArbitrumPool]:
        """Return liquid real pools for a directed pair; never fabricate a pool."""
        key = (token_in, token_out)
        cached = self._pair_cache.get(key)
        if cached is not None:
            return list(cached)
        a = self._tokens[token_in].address
        b = self._tokens[token_out].address
        found: list[ArbitrumPool] = []
        for fee in self.fee_tiers:
            try:
                pool = resolve_pool_address(self.w3, self._addresses.uniswap_factory, a, b, fee)
                if not pool:
                    continue
                state = self._pool_state(pool)
                if state.liquidity >= self.settings.min_pool_liquidity:
                    found.append(ArbitrumPool("uniswap_v3", a, b, state.address, state.fee, state.liquidity))
            except Exception as exc:
                log_fields(logger, 10, "Uniswap pool discovery failed", pair=f"{token_in}/{token_out}", fee=fee, error=str(exc))
        try:
            factory = self.w3.eth.contract(address=self._addresses.camelot_factory, abi=ALGEBRA_FACTORY_ABI)
            pool = factory.functions.poolByPair(a, b).call()
            if int(pool, 16) != 0:
                liquidity = self._camelot_liquidity(pool)
                if liquidity >= self.settings.min_pool_liquidity:
                    found.append(ArbitrumPool("camelot_v3", a, b, Web3.to_checksum_address(pool), None, liquidity))
        except Exception as exc:
            log_fields(logger, 10, "Camelot pool discovery failed", pair=f"{token_in}/{token_out}", error=str(exc))
        # Phase 2 deliberately keeps enumeration bounded: one deepest pool per
        # supported DEX for this pair. Selecting all fee tiers here would turn
        # the constrained 4-hop search into an accidental graph explosion.
        selected: list[ArbitrumPool] = []
        for dex in ("uniswap_v3", "camelot_v3"):
            candidates = [p for p in found if p.dex == dex]
            if candidates:
                selected.append(max(candidates, key=lambda p: p.liquidity))
        self._pair_cache[key] = tuple(selected)
        return selected

    def discover_routes(self) -> list[ArbitrumRoute]:
        """Constrained enumeration of 2--4-hop base-asset cycles."""
        # Validation-only narrowing for deterministic fork/runner checks.  It
        # still resolves every edge through the real factories and retains all
        # supported DEX choices for those edges; it merely avoids unrelated
        # permutations.  Production leaves this unset.
        raw_validation_route = os.getenv("ARBITRUM_VALIDATION_ROUTE", "").strip()
        if raw_validation_route:
            symbols = tuple(x.strip() for x in raw_validation_route.split(",") if x.strip())
            if len(symbols) < 3 or len(symbols) > 5:
                raise ValueError("ARBITRUM_VALIDATION_ROUTE must contain a 2-4 hop cycle")
            if symbols[0] != self.base_symbol or symbols[-1] != self.base_symbol:
                raise ValueError("ARBITRUM_VALIDATION_ROUTE must start and end with base asset")
            choices = [self.discover_pair(symbols[i], symbols[i + 1]) for i in range(len(symbols) - 1)]
            return [ArbitrumRoute(tuple(x)) for x in itertools.product(*choices)] if all(choices) else []
        routes: list[ArbitrumRoute] = []
        base = self.base_symbol
        for hop_count in range(2, 5):
            for intermediates in itertools.permutations(self.intermediate_symbols, hop_count - 1):
                symbols = (base,) + intermediates + (base,)
                choices = [self.discover_pair(symbols[i], symbols[i + 1]) for i in range(hop_count)]
                if all(choices):
                    routes.extend(ArbitrumRoute(tuple(x)) for x in itertools.product(*choices))
        return routes

    def premium_bps(self) -> int:
        pool = self.w3.eth.contract(address=self._addresses.aave_pool, abi=AAVE_PREMIUM_ABI)
        return int(pool.functions.FLASHLOAN_PREMIUM_TOTAL().call())

    def quote_route(self, route: ArbitrumRoute, amount_in: int, eth_usd: Decimal) -> RouteEvaluation:
        amount = amount_in
        hops: list[ArbitrumHop] = []
        for pool in route.hops:
            if pool.dex == "uniswap_v3":
                quote = get_executable_quote(self.w3, self._addresses.uniswap_quoter_v2, pool.token_in, pool.token_out, pool.fee or 0, amount)
                out = quote.amount_out
            else:
                quote = get_camelot_v3_executable_quote(self.w3, self._addresses.camelot_quoter, pool.token_in, pool.token_out, amount)
                out = quote.amount_out
            minimum = compute_amount_out_minimum(out, self.settings.slippage_bps)
            if minimum <= 0:
                raise QuoteError("computed amountOutMinimum is zero")
            hops.append(ArbitrumHop(pool, amount, out, minimum))
            amount = out
        premium = amount_in * self.premium_bps() // 10_000
        # Conservative pre-simulation gas estimate. Simulation replaces this with actual gas.
        provisional_data = self._calldata_for_hops(hops, amount_in, self.min_profit_units())
        gas = estimate_arbitrum_gas_cost(self.w3, {"data": provisional_data}, 500_000, self._addresses.arb_gas_info, self.settings.gas_safety_margin_bps, eth_usd)
        if gas.gas_cost_usd is None or gas.gas_cost_usd > self.settings.max_gas_cost_usd:
            raise QuoteError("gas estimate unavailable or exceeds configured maximum")
        decimals = self._tokens[self.base_symbol].decimals
        gas_asset = int(gas.gas_cost_usd * (Decimal(10) ** decimals))
        minimum_profit = self.min_profit_units()
        return RouteEvaluation(route, tuple(hops), amount_in, amount, premium, gas_asset, minimum_profit, amount - amount_in - premium - gas_asset)

    def min_profit_units(self) -> int:
        # Phase 2 only permits USDC as the borrowed base asset; avoid silently
        # applying a USD value to an unpriced non-stable asset.
        if self.base_symbol != "USDC":
            raise ValueError("Phase 2 min-profit conversion requires USDC base asset")
        return int(self.settings.min_profit_usd * (Decimal(10) ** self._tokens[self.base_symbol].decimals))

    def reprice_after_simulation(
        self,
        evaluation: RouteEvaluation,
        transaction_data: str,
        gas_limit: int,
        eth_usd: Decimal,
    ) -> RouteEvaluation:
        """Replace the provisional fee with the immediate pre-send gas estimate.

        This must run after ``eth_call``/``estimate_gas`` and before the
        executor may broadcast.  The contract's ``minProfit`` protects the
        token-side profit; this calculation additionally protects against the
        now-known ETH-denominated execution cost.
        """
        gas = estimate_arbitrum_gas_cost(
            self.w3,
            {"data": transaction_data},
            gas_limit,
            self._addresses.arb_gas_info,
            self.settings.gas_safety_margin_bps,
            eth_usd,
        )
        if gas.gas_cost_usd is None or gas.gas_cost_usd > self.settings.max_gas_cost_usd:
            raise QuoteError("post-simulation gas estimate unavailable or exceeds configured maximum")
        decimals = self._tokens[self.base_symbol].decimals
        gas_asset = int(gas.gas_cost_usd * (Decimal(10) ** decimals))
        net = evaluation.final_amount - evaluation.amount_in - evaluation.premium - gas_asset
        return replace(evaluation, gas_cost_asset=gas_asset, expected_net_profit=net)

    def to_steps(self, evaluation: RouteEvaluation) -> list[SwapStepArg]:
        steps = []
        for hop in evaluation.hops:
            data = encode_uniswap_v3_data(hop.pool.fee or 0) if hop.pool.dex == "uniswap_v3" else encode_camelot_v3_data()
            adapter = self.settings.uniswap_v3_adapter if hop.pool.dex == "uniswap_v3" else self.settings.camelot_v3_adapter
            if not adapter:
                raise ValueError(f"{hop.pool.dex} adapter address is not configured")
            steps.append(SwapStepArg(adapter, hop.pool.token_in, hop.pool.token_out, hop.amount_out_minimum, data))
        return steps

    def _calldata_for_hops(self, hops: Iterable[ArbitrumHop], amount: int, min_profit: int) -> str:
        # ABI-independent conservative payload sizing for the pre-simulation fee guard.
        # Full ABI encoding and eth_call occur through executor_arbitrum immediately before execution.
        return "0x" + "00" * (4 + 160 + 256 * len(tuple(hops)))
