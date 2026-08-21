"""Web3 provider layer for Arbitrum One.

Provides:
  - HTTP and WebSocket provider construction with configurable timeouts.
  - Automatic reconnect with exponential backoff for WebSocket connections.
  - Health-check helper (``is_healthy``) that verifies RPC connectivity
    via ``eth_blockNumber``.
  - RPC error classification (revert, nonce too low, insufficient funds,
    timeout, rate-limited, network) so callers can react appropriately.

This module is designed to be used by the bot's async orchestration loop
(``bot/main.py``) and the executor (``bot/executor.py``).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional

from web3 import Web3, HTTPProvider
from web3.providers import WebsocketProvider
from web3.exceptions import (
    ContractLogicError,
    TransactionNotFound,
)

from .logger import get_logger, log_fields

logger = get_logger(__name__)

# Default timeouts (seconds)
DEFAULT_HTTP_TIMEOUT = 30
DEFAULT_WS_TIMEOUT = 60

# Reconnect parameters
RECONNECT_INITIAL_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0
RECONNECT_BACKOFF_FACTOR = 2.0


class RpcErrorType(Enum):
    """Classification of RPC / transaction errors for programmatic handling."""

    CONTRACT_REVERT = auto()
    NONCE_TOO_LOW = auto()
    NONCE_TOO_HIGH = auto()
    INSUFFICIENT_FUNDS = auto()
    TX_ALREADY_KNOWN = auto()
    TIMEOUT = auto()
    RATE_LIMITED = auto()
    NETWORK_ERROR = auto()
    SEQUENCER_DOWN = auto()
    UNKNOWN = auto()


@dataclass
class ClassifiedRpcError:
    """Result of classifying an RPC exception."""

    error_type: RpcErrorType
    message: str
    original: Exception

    @property
    def is_retryable(self) -> bool:
        """Whether the caller should retry the operation after a brief wait."""
        return self.error_type in (
            RpcErrorType.TIMEOUT,
            RpcErrorType.RATE_LIMITED,
            RpcErrorType.NETWORK_ERROR,
            RpcErrorType.SEQUENCER_DOWN,
        )

    @property
    def is_simulation_revert(self) -> bool:
        """Whether this error represents a contract logic revert (simulation failure)."""
        return self.error_type == RpcErrorType.CONTRACT_REVERT


def classify_rpc_error(exc: Exception) -> ClassifiedRpcError:
    """Classify an RPC/web3 exception into a ``ClassifiedRpcError``.

    This examines the exception type and message string to determine the
    error category, enabling callers to make programmatic decisions
    (retry, skip, abort) rather than relying on string matching at
    every call site.
    """
    msg = str(exc).lower()

    if isinstance(exc, ContractLogicError):
        return ClassifiedRpcError(RpcErrorType.CONTRACT_REVERT, str(exc), exc)

    # Nonce errors
    if "nonce too low" in msg or "replacement transaction underpriced" in msg:
        return ClassifiedRpcError(RpcErrorType.NONCE_TOO_LOW, str(exc), exc)
    if "nonce too high" in msg:
        return ClassifiedRpcError(RpcErrorType.NONCE_TOO_HIGH, str(exc), exc)

    # Insufficient funds
    if "insufficient funds" in msg or "intrinsic gas too low" in msg:
        return ClassifiedRpcError(RpcErrorType.INSUFFICIENT_FUNDS, str(exc), exc)

    # Already known
    if "already known" in msg or "already imported" in msg:
        return ClassifiedRpcError(RpcErrorType.TX_ALREADY_KNOWN, str(exc), exc)

    # Rate limiting
    if "rate limit" in msg or "429" in msg or "too many requests" in msg:
        return ClassifiedRpcError(RpcErrorType.RATE_LIMITED, str(exc), exc)

    # Timeout
    if "timeout" in msg or "timed out" in msg or "timeexhausted" in msg:
        return ClassifiedRpcError(RpcErrorType.TIMEOUT, str(exc), exc)

    # Sequencer down (Arbitrum-specific)
    if "sequencer is down" in msg or "seq-down" in msg:
        return ClassifiedRpcError(RpcErrorType.SEQUENCER_DOWN, str(exc), exc)

    # Network / connection errors
    if isinstance(exc, (ConnectionError, OSError, asyncio.TimeoutError)):
        return ClassifiedRpcError(RpcErrorType.NETWORK_ERROR, str(exc), exc)
    if "connection" in msg and ("refused" in msg or "reset" in msg or "closed" in msg):
        return ClassifiedRpcError(RpcErrorType.NETWORK_ERROR, str(exc), exc)

    return ClassifiedRpcError(RpcErrorType.UNKNOWN, str(exc), exc)


# --------------------------------------------------------------------------- #
#  Provider construction
# --------------------------------------------------------------------------- #

def make_http_provider(
    rpc_url: str,
    timeout: int = DEFAULT_HTTP_TIMEOUT,
) -> Web3:
    """Construct a ``Web3`` instance backed by an HTTP provider."""
    w3 = Web3(HTTPProvider(rpc_url, request_kwargs={"timeout": timeout}))
    _verify_connected(w3, label="HTTP")
    return w3


def make_ws_provider(
    rpc_url: str,
    timeout: int = DEFAULT_WS_TIMEOUT,
) -> Web3:
    """Construct a ``Web3`` instance backed by a WebSocket provider.

    Note: the caller is responsible for managing the WebSocket lifecycle
    (connecting / disconnecting).  Use ``ReconnectingWsProvider`` for
    automatic reconnection.
    """
    w3 = Web3(WebsocketProvider(rpc_url, request_timeout=timeout))
    _verify_connected(w3, label="WebSocket")
    return w3


def _verify_connected(w3: Web3, label: str) -> None:
    """Raise ``ConnectionError`` if the provider is not reachable."""
    if not w3.is_connected():
        raise ConnectionError(f"{label} provider is not connected")
    try:
        w3.eth.block_number
    except Exception as exc:
        raise ConnectionError(
            f"{label} provider connected but eth_blockNumber failed: {exc}"
        ) from exc


# --------------------------------------------------------------------------- #
#  Health check
# --------------------------------------------------------------------------- #

def is_healthy(w3: Web3) -> bool:
    """Return ``True`` if the provider responds to ``eth_blockNumber``."""
    try:
        w3.eth.block_number
        return True
    except Exception:
        return False


def health_check_detail(w3: Web3) -> dict:
    """Return a detailed health-check dict for logging / monitoring."""
    try:
        block = w3.eth.block_number
        chain_id = w3.eth.chain_id
        return {
            "healthy": True,
            "block_number": block,
            "chain_id": chain_id,
        }
    except Exception as exc:
        classified = classify_rpc_error(exc)
        return {
            "healthy": False,
            "error_type": classified.error_type.name,
            "error": classified.message,
        }


# --------------------------------------------------------------------------- #
#  Reconnecting WebSocket provider
# --------------------------------------------------------------------------- #

class ReconnectingWsProvider:
    """WebSocket provider wrapper with automatic reconnection.

    On disconnect or RPC error, this class attempts to reconnect with
    exponential backoff (1s -> 60s cap).  Between reconnects, ``is_connected``
    returns ``False`` and callers should avoid submitting transactions.

    Usage::

        provider = ReconnectingWsProvider(ws_url)
        await provider.connect()
        w3 = provider.w3
        # ... use w3 ...
        await provider.disconnect()
    """

    def __init__(
        self,
        ws_url: str,
        timeout: int = DEFAULT_WS_TIMEOUT,
        reconnect_initial_delay: float = RECONNECT_INITIAL_DELAY,
        reconnect_max_delay: float = RECONNECT_MAX_DELAY,
        backoff_factor: float = RECONNECT_BACKOFF_FACTOR,
    ):
        self._ws_url = ws_url
        self._timeout = timeout
        self._initial_delay = reconnect_initial_delay
        self._max_delay = reconnect_max_delay
        self._backoff_factor = backoff_factor

        self._w3: Optional[Web3] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._should_run = False
        self._current_delay = reconnect_initial_delay

    @property
    def w3(self) -> Web3:
        if self._w3 is None:
            raise ConnectionError("WebSocket provider not connected")
        return self._w3

    @property
    def is_connected(self) -> bool:
        if self._w3 is None:
            return False
        try:
            return self._w3.is_connected()
        except Exception:
            return False

    async def connect(self) -> None:
        """Establish the initial WebSocket connection."""
        self._should_run = True
        await self._do_connect()
        # Start background reconnection watcher
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _do_connect(self) -> None:
        """Single connection attempt."""
        try:
            self._w3 = Web3(
                WebsocketProvider(self._ws_url, request_timeout=self._timeout)
            )
            _verify_connected(self._w3, label="WebSocket (reconnect)")
            self._current_delay = self._initial_delay
            log_fields(
                logger,
                20,
                "WebSocket provider connected",
            )
        except Exception as exc:
            log_fields(
                logger,
                30,
                "WebSocket connection attempt failed",
                error_type=type(exc).__name__,
                next_retry_in=f"{self._current_delay:.1f}s",
            )
            raise

    async def _reconnect_loop(self) -> None:
        """Background loop that reconnects on disconnect."""
        while self._should_run:
            await asyncio.sleep(1.0)
            if self.is_connected:
                continue

            if not self._should_run:
                break

            log_fields(
                logger,
                30,
                "WebSocket disconnected, attempting reconnect",
                next_retry_in=f"{self._current_delay:.1f}s",
            )

            try:
                await self._do_connect()
            except Exception:
                # Exponential backoff
                await asyncio.sleep(self._current_delay)
                self._current_delay = min(
                    self._current_delay * self._backoff_factor,
                    self._max_delay,
                )

    async def disconnect(self) -> None:
        """Shut down the provider and cancel the reconnection loop."""
        self._should_run = False
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        if self._w3 is not None:
            try:
                # Close the underlying WebSocket if possible
                provider = self._w3.provider
                if hasattr(provider, "disconnect"):
                    await provider.disconnect()
            except Exception:
                pass
            self._w3 = None

        log_fields(logger, 20, "WebSocket provider disconnected")
