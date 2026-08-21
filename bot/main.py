"""LEGACY Polygon single-DEX entry point: WSS pool monitoring + triangular-arbitrage
evaluation loop.

ARBITRUM MIGRATION NOTE: this module implements the original Polygon/Uniswap-V3-only
opportunity pipeline and is left functionally UNCHANGED in Phase 2 (per the migration
spec: "do not modify the opportunity detection algorithm in this phase"). It is
superseded by the new cross-DEX pipeline introduced in Phase 3 (bot/dex/, bot/graph/,
bot/quotes/engine.py). Because bot/executor.py's SwapStepArg was updated in Phase 2 to
match the new FlashArbitrageExecutor contract's adapter-based SwapStep[] (replacing the
old fixed router/fee shape), this legacy loop's `SwapStepArg(router=..., fee=...)`
construction below is no longer executable end-to-end -- it will be replaced by
Phase 3's new opportunity pipeline, which builds `SwapStepArg` from the liquidity graph
using approved DEX adapters instead of raw routers.

MONITORING STRATEGY
  The bot subscribes to Uniswap V3 `Swap` events (via `eth_subscribe("logs", ...)`)
  on the three resolved route pools, rather than polling `slot0()`/quotes on a
  fixed timer. This means RPC calls for quotes/optimization are only made when a
  relevant pool's state has actually changed, which is both cheaper and more
  responsive than blind polling (see README "RPC Efficiency").

RECONNECTION
  The raw WSS connection is wrapped in a reconnect loop with exponential
  backoff. A dropped connection does not crash the process; it resubscribes to
  the same pools once reconnected.

IMPORTANT
  Detecting a state change does NOT guarantee a profitable, executable
  opportunity. Every detected change triggers a fresh optimizer pass that
  re-quotes from current state; the executor performs one more simulation
  immediately before any transaction is sent. See README "MEV / Execution
  Risk" for why even this is not a guarantee of profitable execution.
"""
from __future__ import annotations

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Optional

import websockets
from web3 import Web3

from .arbitrage import NoLiquidPoolError, resolve_route_pools
from .config_legacy_polygon import RouteConfig, Settings, load_settings
from .executor import ArbitrageExecutor, SwapStepArg
from .gas import estimate_gas_cost
from .logger import get_logger, log_fields
from .optimizer import find_optimal_loan_amount
from .pools import PoolState, read_pool_state
from .price_oracle import PriceOracleError, get_native_token_usd_price

logger = get_logger(__name__)

SWAP_EVENT_TOPIC = Web3.keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()

RECONNECT_INITIAL_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0


class ArbitrageBot:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.w3 = Web3(Web3.HTTPProvider(settings.http_rpc_url))
        self.executor = ArbitrageExecutor(self.w3, settings)
        self.tokens_by_symbol = settings.tokens
        self._cycle_lock = asyncio.Lock()
        self.resolved_routes: list[tuple[RouteConfig, list]] = []
        self._pool_to_routes: dict[str, list[int]] = {}
        self._executor_pool = ThreadPoolExecutor(max_workers=8)

    def bootstrap(self) -> None:
        # Fail-fast safety check: if a contract is configured, its on-chain profitRecipient()
        # MUST match the configured PROFIT_RECIPIENT before we do anything else. A mismatch means
        # either misconfiguration or a wrong/unexpected deployment -- either way, arbitrage must
        # not be attempted. This intentionally propagates (crashes startup) rather than being
        # caught, since there is no safe way to continue after this check fails.
        if self.settings.contract_address:
            self.executor.verify_profit_recipient()
        else:
            log_fields(
                logger,
                30,
                "CONTRACT_ADDRESS not configured; skipping on-chain profitRecipient verification "
                "(no deployed contract to check against)",
            )

        log_fields(
            logger,
            20,
            "Resolving live Uniswap V3 pools for all configured routes",
            chain_id=self.settings.chain_id,
            route_count=len(self.settings.routes),
        )
        for route in self.settings.routes:
            try:
                resolved = resolve_route_pools(self.w3, self.settings, self.tokens_by_symbol, route)
                self.resolved_routes.append((route, resolved))
                for leg, pool in resolved:
                    log_fields(
                        logger,
                        20,
                        "Resolved pool",
                        route=route.name,
                        pair=f"{leg.token_in}/{leg.token_out}",
                        pool=pool.address,
                        fee_bps=pool.fee,
                        liquidity=str(pool.liquidity),
                    )
            except NoLiquidPoolError as exc:
                log_fields(
                    logger,
                    30,
                    "Skipping route: could not resolve all pools",
                    route=route.name,
                    error=str(exc),
                )

        # Build pool address -> route indices for affected-route-only evaluation
        for idx, (_route, resolved) in enumerate(self.resolved_routes):
            for _leg, pool in resolved:
                self._pool_to_routes.setdefault(pool.address.lower(), []).append(idx)

    def _pool_addresses(self) -> list[str]:
        addrs: list[str] = []
        seen: set[str] = set()
        for _route, resolved in self.resolved_routes:
            for _leg, pool in resolved:
                if pool.address not in seen:
                    seen.add(pool.address)
                    addrs.append(pool.address)
        return addrs

    def _borrowed_asset_symbol(self) -> str:
        return self.settings.routes[0].legs[0].token_in

    def _spot_price_product(self, resolved: list[tuple]) -> float:
        """Compute the product of spot prices across all legs of a route.
        If product > 1.0 (after fees), the route MAY be profitable at zero size.
        If product < 1.0, the route CANNOT be profitable at any size (price impact only makes it worse).
        This is a fast pre-filter using cached sqrtPriceX96 -- no RPC calls needed.
        """
        product = 1.0
        for leg, pool in resolved:
            token_in_cfg = self.tokens_by_symbol[leg.token_in]
            token_out_cfg = self.tokens_by_symbol[leg.token_out]
            # human_price_of expects token0_decimals, token1_decimals (pool's token0/token1)
            if pool.token0.lower() == token_in_cfg.address.lower():
                t0_dec, t1_dec = token_in_cfg.decimals, token_out_cfg.decimals
            else:
                t0_dec, t1_dec = token_out_cfg.decimals, token_in_cfg.decimals
            # Price of token_in expressed in token_out (human units)
            price = pool.human_price_of(token_in_cfg.address, t0_dec, t1_dec)
            fee_mult = 1.0 - (pool.fee / 1_000_000)
            product *= price * fee_mult
        return product

    def _estimate_native_usd_price(self) -> Optional[Decimal]:
        """Native-token (MATIC/POL) USD price used ONLY to convert estimated gas cost into the
        borrowed asset's units for the net-profit calculation -- never used to size trades.

        CRITICAL: this MUST come from a source independent of the pools being arbitraged.
        Deriving the native-token price from the same USDC/WMATIC pool this bot trades against
        would be circular and inherits that specific pool's liquidity/manipulation risk (exactly
        the kind of risk this system is supposed to be pricing IN, not depending ON). We
        therefore use Chainlink's MATIC/USD Data Feed on Polygon (see bot/price_oracle.py) as an
        independent oracle. If the oracle call fails or returns a stale price, we fall back to
        the pool-spot-price approximation as a last resort, but this is logged loudly as
        DEGRADED so it is never mistaken for a reliable price.
        """
        try:
            oracle_price = get_native_token_usd_price(self.w3)
            return oracle_price.price_usd
        except PriceOracleError as exc:
            log_fields(
                logger,
                30,
                "Chainlink MATIC/USD oracle unavailable; falling back to DEGRADED pool-spot-price "
                "approximation for gas-cost conversion only (never used to size trades)",
                error=str(exc),
            )

        for _route, resolved in self.resolved_routes:
            for leg, pool in resolved:
                symbols = {leg.token_in, leg.token_out}
                if "USDC" in symbols and "WMATIC" in symbols:
                    usdc_cfg = self.tokens_by_symbol["USDC"]
                    wmatic_cfg = self.tokens_by_symbol["WMATIC"]
                    token0_decimals = usdc_cfg.decimals if pool.token0.lower() == usdc_cfg.address.lower() else wmatic_cfg.decimals
                    token1_decimals = wmatic_cfg.decimals if pool.token1.lower() == wmatic_cfg.address.lower() else usdc_cfg.decimals
                    price_wmatic_in_usdc = pool.human_price_of(wmatic_cfg.address, token0_decimals, token1_decimals)
                    return Decimal(str(price_wmatic_in_usdc))
        return None

    async def run_arbitrage_cycle(self, trigger: str, route_indices: Optional[list[int]] = None) -> None:
        if self._cycle_lock.locked():
            log_fields(logger, 10, "Cycle already running, skipping overlapping trigger", trigger=trigger)
            return
        async with self._cycle_lock:
            await self._run_arbitrage_cycle_locked(trigger, route_indices)

    async def _run_arbitrage_cycle_locked(self, trigger: str, route_indices: Optional[list[int]] = None) -> None:
        started = time.time()
        native_usd_price = self._estimate_native_usd_price()
        borrowed_asset_usd_price = Decimal("1")

        approx_gas_units = 550_000
        try:
            gas_price = self.w3.eth.gas_price
        except Exception:
            gas_price = 0
        gas_cost_native = Decimal(approx_gas_units) * Decimal(gas_price) / Decimal(10 ** 18)

        # Determine which routes to evaluate
        if route_indices is not None:
            routes_to_check = [(i, self.resolved_routes[i]) for i in route_indices if i < len(self.resolved_routes)]
        else:
            routes_to_check = list(enumerate(self.resolved_routes))

        best_overall: Optional[tuple[RouteConfig, object]] = None
        best_net_profit: int = 0
        total_evaluated = 0
        total_skipped = 0
        routes_prefiltered = 0

        for idx, (route, resolved) in routes_to_check:
            # --- Spot price pre-filter (zero RPC calls) ---
            spot_product = self._spot_price_product(resolved)
            if spot_product < 0.999:
                routes_prefiltered += 1
                log_fields(
                    logger,
                    10,
                    "Route pre-filtered (spot price product < 1.0)",
                    route=route.name,
                    spot_product=round(spot_product, 6),
                )
                continue

            borrowed_symbol = route.legs[0].token_in
            borrowed_token = self.tokens_by_symbol[borrowed_symbol]

            try:
                result = find_optimal_loan_amount(
                    self.w3,
                    self.settings,
                    self.tokens_by_symbol,
                    resolved,
                    aave_fee_bps_onchain=None,
                    gas_cost_native=gas_cost_native,
                    native_usd_price=native_usd_price,
                    borrowed_asset_usd_price=borrowed_asset_usd_price,
                    borrowed_asset_decimals=borrowed_token.decimals,
                )
            except Exception as exc:
                log_fields(logger, 40, "Optimizer failed for route", trigger=trigger, route=route.name, error=str(exc))
                continue

            total_evaluated += result.candidates_evaluated
            total_skipped += result.candidates_skipped

            best = result.best_evaluation
            if best is None:
                log_fields(
                    logger,
                    10,
                    "No evaluable candidate for route",
                    trigger=trigger,
                    route=route.name,
                    evaluated=result.candidates_evaluated,
                    skipped=result.candidates_skipped,
                )
                continue

            log_fields(
                logger,
                20,
                "Route candidate evaluated",
                trigger=trigger,
                route=route.name,
                initial_amount=best.initial_amount,
                final_amount=best.final_amount,
                gross_profit=best.gross_profit,
                aave_fee=best.aave_fee,
                gas_cost_asset_equivalent=best.gas_cost_asset_equivalent,
                net_profit=best.net_profit,
                candidates_evaluated=result.candidates_evaluated,
                candidates_skipped=result.candidates_skipped,
                spot_product=round(spot_product, 6),
            )

            if best.net_profit > best_net_profit:
                best_net_profit = best.net_profit
                best_overall = (route, best)

        elapsed_ms = int((time.time() - started) * 1000)

        if best_overall is None:
            log_fields(
                logger,
                10,
                "No profitable candidate this cycle",
                trigger=trigger,
                routes_checked=len(routes_to_check),
                routes_prefiltered=routes_prefiltered,
                evaluated=total_evaluated,
                skipped=total_skipped,
                elapsed_ms=elapsed_ms,
            )
            return

        route, best = best_overall
        borrowed_symbol = route.legs[0].token_in
        borrowed_token = self.tokens_by_symbol[borrowed_symbol]
        min_profit_units = int(self.settings.min_profit_usd * (10 ** borrowed_token.decimals))

        log_fields(
            logger,
            20,
            "Best candidate across evaluated routes",
            trigger=trigger,
            route=route.name,
            initial_amount=best.initial_amount,
            final_amount=best.final_amount,
            gross_profit=best.gross_profit,
            aave_fee=best.aave_fee,
            gas_cost_asset_equivalent=best.gas_cost_asset_equivalent,
            net_profit=best.net_profit,
            min_profit_threshold_units=min_profit_units,
            total_evaluated=total_evaluated,
            total_skipped=total_skipped,
            elapsed_ms=elapsed_ms,
        )

        if best.net_profit <= min_profit_units:
            log_fields(logger, 10, "Below minimum profit threshold; not executing", trigger=trigger, route=route.name)
            return

        deadline = int(time.time()) + self.settings.tx_deadline_seconds
        steps = [
            SwapStepArg(
                router=self.settings.addresses.uniswap_swap_router,
                token_in=leg.token_in,
                token_out=leg.token_out,
                fee=leg.fee,
                amount_out_minimum=leg.amount_out_minimum,
            )
            for leg in best.legs
        ]

        tx_hash = await self.executor.execute(borrowed_token.address, best.initial_amount, steps, deadline)
        if tx_hash:
            log_fields(logger, 20, "Arbitrage transaction sent", tx_hash=tx_hash, route=route.name)

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        params = msg.get("params")
        if not params:
            return
        result = params.get("result")
        if not result:
            return
        pool_addr = result.get("address", "").lower()
        affected = self._pool_to_routes.get(pool_addr)
        if not affected:
            return
        await self.run_arbitrage_cycle(trigger="pool_swap_event", route_indices=affected)

    async def watch_pools_forever(self) -> None:
        delay = RECONNECT_INITIAL_DELAY
        pool_addresses = self._pool_addresses()

        while True:
            try:
                async with websockets.connect(self.settings.wss_rpc_url, ping_interval=20, ping_timeout=20) as ws:
                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": [
                            "logs",
                            {"address": pool_addresses, "topics": [SWAP_EVENT_TOPIC]},
                        ],
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    await ws.recv()  # subscription confirmation
                    log_fields(logger, 20, "WSS subscribed to pool Swap events", pools=pool_addresses)
                    delay = RECONNECT_INITIAL_DELAY  # reset backoff after a successful (re)connect

                    async for raw in ws:
                        await self._handle_message(raw)

            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                log_fields(logger, 30, "WSS connection lost; reconnecting", error=str(exc), retry_in_s=delay)
            except Exception as exc:
                log_fields(logger, 40, "Unexpected WSS error; reconnecting", error=str(exc), retry_in_s=delay)

            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)

    async def periodic_fallback_loop(self, interval_seconds: int = 30) -> None:
        """Defense-in-depth: even if WSS misses an event (e.g. brief gap during
        reconnect), periodically run a cycle anyway. Interval is intentionally
        much coarser than event-driven triggers to avoid RPC-call explosion.
        """
        while True:
            await asyncio.sleep(interval_seconds)
            await self.run_arbitrage_cycle(trigger="periodic_fallback")


async def main() -> None:
    settings = load_settings()
    log_fields(
        get_logger("bot.startup"),
        20,
        "Starting Polygon triangular arbitrage bot",
        dry_run=settings.dry_run,
        chain_id=settings.chain_id,
    )

    bot = ArbitrageBot(settings)
    bot.bootstrap()

    await asyncio.gather(
        bot.watch_pools_forever(),
        bot.periodic_fallback_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
