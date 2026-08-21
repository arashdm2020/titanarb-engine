"""Arbitrum Phase-2 runner: polling-first, dry-run-safe orchestration."""
from __future__ import annotations

import asyncio
import ctypes
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

from .arbitrum_pipeline import ArbitrumPipeline
from .config import load_settings
from .executor_arbitrum import ArbitrageExecutor, SimulationFailedError
from .logger import get_logger, log_fields, redact_secrets
from .price_oracle_arbitrum import get_safe_eth_usd_price
from .provider import make_http_provider, make_ws_provider

logger = get_logger(__name__)


class RunnerAlreadyActiveError(RuntimeError):
    """Raised when another Arbitrum runner already owns the local lock."""


class RunnerLock:
    """A small, process-safe local lock preventing duplicate live runners."""

    def __init__(self, path: Path):
        self.path = path
        self.acquired = False

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        if os.name == "nt":
            # ``os.kill(pid, 0)`` maps to a console control event on Windows,
            # rather than a harmless POSIX liveness probe.  Query the process
            # handle instead so a duplicate-runner check cannot interrupt it.
            process_query_limited_information = 0x1000
            still_active = 259
            handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
                process_query_limited_information, False, pid
            )
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                if not ctypes.windll.kernel32.GetExitCodeProcess(  # type: ignore[attr-defined]
                    handle, ctypes.byref(exit_code)
                ):
                    return False
                return exit_code.value == still_active
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                pid = int(self.path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                pid = 0
            if pid and self._pid_is_alive(pid):
                raise RunnerAlreadyActiveError(
                    f"Arbitrum runner is already active with pid {pid}"
                )
            self.path.unlink(missing_ok=True)
            return self.acquire()
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(str(os.getpid()))
        self.acquired = True

    def release(self) -> None:
        if self.acquired:
            self.path.unlink(missing_ok=True)
            self.acquired = False


class JsonlMetrics:
    """Durable local operational events; intentionally excludes credentials."""

    SECRET_FIELD_FRAGMENTS = ("key", "secret", "password", "url", "rpc")

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def _safe_fields(cls, fields: dict) -> dict:
        return {
            key: redact_secrets(value)
            for key, value in fields.items()
            if not any(fragment in key.lower() for fragment in cls.SECRET_FIELD_FRAGMENTS)
        }

    def record(self, event: str, **fields) -> None:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **self._safe_fields(fields),
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), default=str))
            handle.write("\n")
            handle.flush()


def _route_label(route) -> str:
    return " -> ".join(
        f"{pool.token_in}:{pool.dex}:{pool.token_out}"
        for hop in route.hops
        for pool in (getattr(hop, "pool", hop),)
    )


class ArbitrumBot:
    def __init__(self, settings):
        self.settings = settings
        self.w3 = make_http_provider(settings.http_rpc_url)
        if self.w3.eth.chain_id != 42161:
            raise RuntimeError("Refusing to run: RPC is not Arbitrum One")
        self.pipeline = ArbitrumPipeline(self.w3, settings)
        self.executor = ArbitrageExecutor(self.w3, settings)
        self.metrics = {key: 0 for key in (
            "blocks", "routes", "routes_evaluated", "quote_failures", "unprofitable",
            "profitable", "simulations", "simulation_failures", "final_gas_rejections",
            "submitted", "successful_trades", "reverted_trades", "realized_profit",
        )}
        self._cycle_lock = asyncio.Lock()
        self._wss_healthy = False
        self._wss_ready = asyncio.Event()
        self._event_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        runtime_dir = Path(os.getenv("ARBITRUM_RUNTIME_DIR", "runtime"))
        self.metrics_writer = JsonlMetrics(
            runtime_dir / os.getenv("ARBITRUM_METRICS_FILE", "arbitrum_metrics.jsonl")
        )
        self._runner_lock = RunnerLock(runtime_dir / "arbitrum_runner.lock")

    def _emit_metrics(self, event: str, **fields) -> None:
        self.metrics_writer.record(event, **fields, metrics=dict(self.metrics))

    async def _wss_event_loop(self) -> None:
        """Subscribe to real newHeads events and signal the single cycle worker."""
        delay = 1.0
        reconnecting = False
        while True:
            try:
                if reconnecting:
                    self._emit_metrics("wss_reconnect_attempt", retry_delay_seconds=delay)
                async with websockets.connect(
                    self.settings.ws_rpc_url,
                    open_timeout=20,
                    ping_interval=20,
                    ping_timeout=20,
                ) as socket:
                    await socket.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
                    reply = json.loads(await asyncio.wait_for(socket.recv(), timeout=20))
                    if "error" in reply or not reply.get("result"):
                        raise ConnectionError("WSS newHeads subscription rejected")
                    self._wss_healthy = True
                    self._wss_ready.set()
                    delay = 1.0
                    self._emit_metrics("wss_connected", subscription="newHeads")
                    if reconnecting:
                        self._emit_metrics("wss_restored", subscription="newHeads")
                    log_fields(logger, 20, "WSS primary connected", subscription="newHeads")
                    reconnecting = False
                    while True:
                        message = json.loads(await socket.recv())
                        if message.get("method") != "eth_subscription":
                            continue
                        result = message.get("params", {}).get("result", {})
                        block = result.get("number")
                        if self._event_queue.empty():
                            self._event_queue.put_nowait("wss_new_head")
                            # Persist only the event that can actually schedule
                            # work.  Subsequent heads while a cycle is active are
                            # intentionally coalesced by the one-slot queue.
                            self._emit_metrics("wss_new_head", block=block)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                was_healthy = self._wss_healthy
                self._wss_healthy = False
                self._wss_ready.clear()
                reconnecting = True
                self._emit_metrics(
                    "wss_disconnected",
                    error_type=type(exc).__name__,
                    retry_in_seconds=delay,
                )
                log_fields(
                    logger,
                    30,
                    "WSS unavailable; HTTP polling fallback active",
                    error_type=type(exc).__name__,
                    retry_in_seconds=delay,
                    was_healthy=was_healthy,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def _record_execution_result(self, result, route, evaluation) -> None:
        realized_profit = None
        if result.tx_hash and result.receipt_status == 1:
            try:
                receipt = await asyncio.to_thread(
                    self.w3.eth.get_transaction_receipt, result.tx_hash
                )
                events = self.executor.contract.events.ArbitrageExecuted().process_receipt(receipt)
                if events:
                    realized_profit = int(events[-1]["args"]["profit"])
                    self.metrics["realized_profit"] += realized_profit
            except Exception as exc:
                self._emit_metrics("profit_decode_failed", tx_hash=result.tx_hash, error_type=type(exc).__name__)
        if result.tx_hash:
            if result.receipt_status == 1:
                self.metrics["successful_trades"] += 1
            elif result.receipt_status == 0:
                self.metrics["reverted_trades"] += 1
            self._emit_metrics(
                "execution_result",
                tx_hash=result.tx_hash,
                receipt_status=result.receipt_status,
                gas_used=result.receipt_gas_used,
                route=_route_label(route),
                loan_amount=evaluation.amount_in,
                premium=evaluation.premium,
                expected_net_profit=evaluation.expected_net_profit,
                min_profit=evaluation.min_profit,
                realized_profit=realized_profit,
            )
            if result.receipt_status == 1:
                log_fields(
                    logger,
                    20,
                    "LIVE TRADE CONFIRMED",
                    tx_hash=result.tx_hash,
                    receipt_status=result.receipt_status,
                    gas_used=result.receipt_gas_used,
                    realized_profit=realized_profit,
                )
            elif result.receipt_status == 0:
                log_fields(
                    logger,
                    40,
                    "LIVE TRADE REVERTED",
                    tx_hash=result.tx_hash,
                    receipt_status=result.receipt_status,
                    gas_used=result.receipt_gas_used,
                )

    async def run_once(self, trigger: str = "polling") -> None:
        if self._cycle_lock.locked():
            self._emit_metrics("cycle_skipped_overlap", trigger=trigger)
            return
        async with self._cycle_lock:
            await self._run_once(trigger)

    async def _run_once(self, trigger: str) -> None:
        block = await asyncio.to_thread(self.w3.eth.get_block, "latest")
        self.metrics["blocks"] += 1
        self._emit_metrics("cycle_started", block=block.number, trigger=trigger)
        try:
            oracle = await asyncio.to_thread(
                get_safe_eth_usd_price,
                self.w3,
                self.settings.addresses.chainlink_eth_usd_feed,
                self.settings.addresses.chainlink_sequencer_uptime_feed,
                self.settings.price_max_staleness_seconds,
                self.settings.sequencer_grace_period_seconds,
                self.settings.sequencer_heartbeat_seconds,
            )
            eth = oracle.price_usd
        except Exception as exc:
            log_fields(logger, 30, "Oracle/sequencer gate rejected cycle", block=block.number, error=str(exc))
            self._emit_metrics("cycle_rejected_oracle", block=block.number, error_type=type(exc).__name__)
            return
        routes = await asyncio.to_thread(self.pipeline.discover_routes)
        self.metrics["routes"] += len(routes)
        self._emit_metrics("routes_discovered", block=block.number, routes_discovered=len(routes))
        amount = int(self.settings.min_loan_amount * (10 ** self.settings.tokens[self.pipeline.base_symbol].decimals))
        for route in routes:
            self.metrics["routes_evaluated"] += 1
            try:
                evaluation = await asyncio.to_thread(
                    self.pipeline.quote_route, route, amount, eth
                )
            except Exception as exc:
                self.metrics["quote_failures"] += 1
                log_fields(logger, 10, "Route rejected during quote", block=block.number, error=str(exc))
                self._emit_metrics("quote_failed", block=block.number, route=_route_label(route), error_type=type(exc).__name__)
                continue
            if not evaluation.is_profitable:
                self.metrics["unprofitable"] += 1
                log_fields(logger, 10, "Route unprofitable", block=block.number, amount_in=evaluation.amount_in, final_amount=evaluation.final_amount, premium=evaluation.premium, gas_asset=evaluation.gas_cost_asset, min_profit=evaluation.min_profit, net_profit=evaluation.expected_net_profit)
                self._emit_metrics("route_rejected_unprofitable", block=block.number, route=_route_label(route), final_amount=evaluation.final_amount, premium=evaluation.premium, expected_net_profit=evaluation.expected_net_profit, min_profit=evaluation.min_profit)
                continue
            self.metrics["profitable"] += 1
            self._emit_metrics("profitable_candidate", block=block.number, route=_route_label(route), expected_net_profit=evaluation.expected_net_profit, min_profit=evaluation.min_profit)
            try:
                steps = self.pipeline.to_steps(evaluation)
                deadline = int(block.timestamp) + self.settings.tx_deadline_seconds
                self.metrics["simulations"] += 1
                self._emit_metrics("simulation_attempt", block=block.number, route=_route_label(route), loan_amount=evaluation.amount_in, premium=evaluation.premium, min_profit=evaluation.min_profit, expected_net_profit=evaluation.expected_net_profit, min_outs=[step.amount_out_minimum for step in steps])
                # simulate immediately before executor.execute; execute repeats the
                # check and cannot send unless both explicit live guards are set.
                gas_limit = await asyncio.to_thread(
                    self.executor.simulate,
                    self.settings.tokens[self.pipeline.base_symbol].address,
                    evaluation.amount_in,
                    steps,
                    deadline,
                    evaluation.min_profit,
                )
                call = self.executor._build_call(self.settings.tokens[self.pipeline.base_symbol].address, evaluation.amount_in, steps, deadline, evaluation.min_profit)
                final = await asyncio.to_thread(
                    self.pipeline.reprice_after_simulation,
                    evaluation,
                    call._encode_transaction_data(),
                    gas_limit,
                    eth,
                )
                if not final.is_profitable:
                    self.metrics["final_gas_rejections"] += 1
                    log_fields(logger, 20, "Route rejected after final gas recheck", block=block.number, gas_limit=gas_limit, net_profit=final.expected_net_profit, min_profit=final.min_profit)
                    self._emit_metrics("route_rejected_post_gas", block=block.number, route=_route_label(route), gas_limit=gas_limit, expected_net_profit=final.expected_net_profit, min_profit=final.min_profit)
                    continue
                result = await self.executor.execute(self.settings.tokens[self.pipeline.base_symbol].address, final.amount_in, steps, deadline, final.min_profit)
                if result.tx_hash:
                    self.metrics["submitted"] += 1
                    self._emit_metrics("transaction_broadcast", block=block.number, tx_hash=result.tx_hash, route=_route_label(route), loan_amount=final.amount_in, premium=final.premium, min_profit=final.min_profit, expected_net_profit=final.expected_net_profit, gas_limit=result.gas_limit, min_outs=[step.amount_out_minimum for step in steps])
                    log_fields(
                        logger,
                        20,
                        "TX SENT",
                        tx_hash=result.tx_hash,
                        block=block.number,
                        expected_net_profit=final.expected_net_profit,
                    )
                await self._record_execution_result(result, route, final)
                log_fields(logger, 20, "Arbitrum route processed", block=block.number, hops=len(steps), dry_run=self.settings.dry_run, execution_mode=self.settings.execution_mode, net_profit=final.expected_net_profit, simulation_error=result.error, tx_hash=result.tx_hash)
            except SimulationFailedError as exc:
                self.metrics["simulation_failures"] += 1
                log_fields(logger, 20, "Simulation rejected route", block=block.number, error=str(exc))
                self._emit_metrics("simulation_failed", block=block.number, route=_route_label(route), error_type=type(exc).__name__)
        self._emit_metrics("cycle_complete", block=block.number, trigger=trigger)
        log_fields(
            logger,
            20,
            "CYCLE SUMMARY",
            block=block.number,
            routes_evaluated=self.metrics["routes_evaluated"],
            quote_failures=self.metrics["quote_failures"],
            profitable_candidates=self.metrics["profitable"],
            simulations=self.metrics["simulations"],
            post_gas_rejections=self.metrics["final_gas_rejections"],
            broadcasts=self.metrics["submitted"],
        )

    async def run_forever(self) -> None:
        self._runner_lock.acquire()
        wss_task = None
        try:
            if self.settings.market_data_mode == "wss_preferred":
                wss_task = asyncio.create_task(self._wss_event_loop())
                mode = "wss_primary_polling_fallback"
                try:
                    await asyncio.wait_for(self._wss_ready.wait(), timeout=20)
                except asyncio.TimeoutError:
                    self._emit_metrics("wss_startup_polling_fallback")
                    log_fields(logger, 30, "WSS startup timed out; HTTP polling fallback active")
            else:
                mode = "polling"
            self._emit_metrics("runner_started", mode=mode, dry_run=self.settings.dry_run, execution_mode=self.settings.execution_mode)
            log_fields(logger, 20, "Starting Arbitrum bot", mode=mode, dry_run=self.settings.dry_run, execution_mode=self.settings.execution_mode)
            while True:
                try:
                    if self.settings.market_data_mode == "wss_preferred" and self._wss_healthy:
                        try:
                            trigger = await asyncio.wait_for(self._event_queue.get(), timeout=self.settings.poll_interval_seconds * 2)
                        except asyncio.TimeoutError:
                            self._wss_healthy = False
                            self._emit_metrics("wss_stale_polling_fallback")
                            continue
                    else:
                        trigger = "polling_fallback" if self.settings.market_data_mode == "wss_preferred" else "polling"
                    await self.run_once(trigger)
                except Exception as exc:
                    log_fields(logger, 40, "Non-fatal Arbitrum cycle failure", error_type=type(exc).__name__)
                    self._emit_metrics("cycle_error", error_type=type(exc).__name__)
                if not self._wss_healthy:
                    await asyncio.sleep(self.settings.poll_interval_seconds)
        finally:
            if wss_task:
                wss_task.cancel()
                try:
                    await wss_task
                except asyncio.CancelledError:
                    pass
            self._emit_metrics("runner_stopped")
            self._runner_lock.release()


async def main() -> None:
    await ArbitrumBot(load_settings()).run_forever()


if __name__ == "__main__":
    asyncio.run(main())
