"""Operational safeguards for the Arbitrum runner.

These checks cover transport and observability only.  They do not affect
route selection, transaction construction, or safety gates.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from bot.main_arbitrum import (
    ArbitrumBot,
    JsonlMetrics,
    RunnerAlreadyActiveError,
    RunnerLock,
    _route_label,
)


def test_jsonl_metrics_are_durable_and_exclude_credentials(tmp_path: Path):
    path = tmp_path / "metrics.jsonl"
    JsonlMetrics(path).record(
        "cycle_complete",
        block=42161,
        routes_evaluated=3,
        rpc_url="https://credential-bearing.example",
        private_key="must-not-persist",
        error="connection to wss://credential-bearing.example/path failed",
    )

    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["event"] == "cycle_complete"
    assert record["block"] == 42161
    assert "rpc_url" not in record
    assert "private_key" not in record
    assert "credential-bearing.example" not in record["error"]


def test_runner_lock_prevents_duplicate_processes(tmp_path: Path):
    path = tmp_path / "arbitrum_runner.lock"
    first = RunnerLock(path)
    second = RunnerLock(path)

    first.acquire()
    try:
        with pytest.raises(RunnerAlreadyActiveError):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    second.release()


def test_route_label_supports_discovery_pools_and_evaluated_hops():
    pool = SimpleNamespace(token_in="USDC", dex="uniswap_v3", token_out="WETH")
    discovery_route = SimpleNamespace(hops=(pool,))
    evaluated_route = SimpleNamespace(hops=(SimpleNamespace(pool=pool),))

    assert _route_label(discovery_route) == "USDC:uniswap_v3:WETH"
    assert _route_label(evaluated_route) == "USDC:uniswap_v3:WETH"


class _DisconnectingSocket:
    def __init__(self):
        self.messages = [
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": "sub-1"}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {"result": {"number": "0x1"}},
                }
            ),
        ]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def send(self, _payload):
        return None

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        raise ConnectionError("test disconnect")


def test_wss_disconnect_records_fallback_and_reconnect_attempt(tmp_path: Path):
    """Use asyncio.run so the suite does not require pytest-asyncio."""

    async def scenario() -> None:
        bot = ArbitrumBot.__new__(ArbitrumBot)
        bot.settings = SimpleNamespace(ws_rpc_url="wss://test.invalid")
        bot.metrics = {}
        bot.metrics_writer = JsonlMetrics(tmp_path / "metrics.jsonl")
        bot._wss_healthy = False
        bot._wss_ready = asyncio.Event()
        bot._event_queue = asyncio.Queue(maxsize=1)

        attempts = 0

        def connect(*_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            return _DisconnectingSocket()

        async def fast_retry_sleep(_delay):
            # Avoid a real one-second backoff in a unit test.  The second
            # connection proves the reconnect path was reached.
            if attempts >= 2:
                raise asyncio.CancelledError

        with (
            patch("bot.main_arbitrum.websockets.connect", side_effect=connect),
            patch("bot.main_arbitrum.log_fields"),
            patch("bot.main_arbitrum.asyncio.sleep", side_effect=fast_retry_sleep),
        ):
            task = asyncio.create_task(bot._wss_event_loop())
            with pytest.raises(asyncio.CancelledError):
                await task

        records = [
            json.loads(line)
            for line in (tmp_path / "metrics.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert attempts >= 2
        assert any(record["event"] == "wss_connected" for record in records)
        assert any(record["event"] == "wss_disconnected" for record in records)
        assert any(record["event"] == "wss_reconnect_attempt" for record in records)
        assert any(record["event"] == "wss_restored" for record in records)
        assert bot._event_queue.get_nowait() == "wss_new_head"

    asyncio.run(scenario())
