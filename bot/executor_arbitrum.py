"""Arbitrum transaction simulation, signing, and submission.

This module is the Arbitrum-native replacement for ``bot/executor.py``, updated
for the new ``FlashArbitrageExecutor`` contract's adapter-based ``SwapStep[]``
architecture.

Key changes from the legacy executor:
  - ``SwapStepArg`` now uses ``adapter`` / ``data`` instead of ``router`` / ``fee``,
    matching the on-chain ``FlashArbitrageExecutor.SwapStep`` struct.
  - The ABI is loaded from ``flash_arbitrage_executor.json`` (not ``flash_arb.json``).
  - ``executeArbitrage`` now takes a ``min_profit`` parameter.
  - Simulation failures are classified via ``provider.classify_rpc_error`` so
    callers can distinguish reverts from network errors, nonce issues, etc.
  - Stale opportunity detection: the caller-supplied ``deadline`` is checked
    against the current block timestamp before simulation.
  - Transaction expiration: if the transaction is not mined within
    ``tx_timeout_seconds``, it is considered expired.
  - Execution latency tracking: measures wall-clock time from simulation
    to broadcast for monitoring / optimization.

Security / safety properties preserved from the legacy executor:
  - Private key loaded once via ``Account.from_key`` from env var.
  - ``_execution_lock`` prevents concurrent arbitrage transactions.
  - ``NonceManager`` uses pending transaction count to avoid nonce collisions.
  - Every transaction is simulated via ``eth_call`` before sending.
  - DRY_RUN=true guarantees no transaction is ever sent.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from eth_account import Account
from eth_account.signers.local import LocalAccount
from web3 import Web3
from web3.exceptions import ContractLogicError, TimeExhausted

from .config import Settings
from .logger import get_logger, log_fields
from .provider import classify_rpc_error, ClassifiedRpcError, RpcErrorType

ABI_DIR = Path(__file__).resolve().parent / "abi"
logger = get_logger(__name__)


def _load_abi(name: str) -> list:
    with open(ABI_DIR / name, "r", encoding="utf-8") as fh:
        return json.load(fh)


FLASH_ARBITRAGE_EXECUTOR_ABI = _load_abi("flash_arbitrage_executor.json")


# --------------------------------------------------------------------------- #
#  Errors
# --------------------------------------------------------------------------- #

class SimulationFailedError(Exception):
    """Raised when eth_call / estimateGas simulation reverts."""

    def __init__(self, message: str, classified: ClassifiedRpcError | None = None):
        super().__init__(message)
        self.classified = classified


class ExecutionLockedError(Exception):
    """Raised when an arbitrage transaction is already in flight."""


class ProfitRecipientMismatchError(Exception):
    """Raised when configured PROFIT_RECIPIENT doesn't match on-chain state."""


class StaleOpportunityError(Exception):
    """Raised when the deadline has already passed before simulation."""


class TransactionExpiredError(Exception):
    """Raised when a submitted transaction is not mined within the timeout."""


# --------------------------------------------------------------------------- #
#  SwapStepArg -- matches FlashArbitrageExecutor.SwapStep
# --------------------------------------------------------------------------- #

@dataclass
class SwapStepArg:
    """Python-side representation of ``FlashArbitrageExecutor.SwapStep``.

    Fields map 1:1 to the on-chain struct:
      - ``adapter``: address of the DEX adapter contract (e.g. UniswapV3Adapter)
      - ``token_in``: input token address
      - ``token_out``: output token address
      - ``amount_out_minimum``: minimum acceptable output (slippage protection)
      - ``data``: adapter-specific encoded parameters (e.g. ``uint24 fee`` for
        Uniswap V3, empty bytes for Camelot V3)
    """
    adapter: str
    token_in: str
    token_out: str
    amount_out_minimum: int
    data: bytes

    def as_tuple(self) -> tuple:
        """Return the tuple representation expected by web3.py ABI encoding."""
        return (
            Web3.to_checksum_address(self.adapter),
            Web3.to_checksum_address(self.token_in),
            Web3.to_checksum_address(self.token_out),
            int(self.amount_out_minimum),
            self.data if isinstance(self.data, bytes) else bytes(self.data),
        )


# --------------------------------------------------------------------------- #
#  Helpers for encoding adapter-specific data
# --------------------------------------------------------------------------- #

def encode_uniswap_v3_data(fee: int) -> bytes:
    """Encode the Uniswap V3 fee tier as the ``data`` field for a SwapStep.

    The UniswapV3Adapter decodes ``data`` as a ``uint24`` fee tier.
    We ABI-encode a single ``uint24`` value.
    """
    from eth_abi import encode as abi_encode
    return abi_encode(["uint24"], [int(fee)])


def encode_camelot_v3_data() -> bytes:
    """Encode empty data for Camelot V3 (no adapter-specific parameters needed).

    The CamelotV3Adapter does not use the ``data`` field.
    """
    return b""


# --------------------------------------------------------------------------- #
#  NonceManager (unchanged from legacy, but self-contained)
# --------------------------------------------------------------------------- #

class NonceManager:
    """Tracks the next nonce to use, reconciling against the chain's pending
    transaction count so that back-to-back submissions do not collide.
    """

    def __init__(self, w3: Web3, address: str):
        self._w3 = w3
        self._address = Web3.to_checksum_address(address)
        self._next_nonce: Optional[int] = None
        self._lock = asyncio.Lock()

    async def next_nonce(self) -> int:
        async with self._lock:
            chain_pending = self._w3.eth.get_transaction_count(self._address, "pending")
            if self._next_nonce is None or chain_pending > self._next_nonce:
                self._next_nonce = chain_pending
            nonce = self._next_nonce
            self._next_nonce += 1
            return nonce

    async def release_on_failure(self, nonce: int) -> None:
        """Call if a transaction using `nonce` failed to broadcast."""
        async with self._lock:
            if self._next_nonce is not None and nonce == self._next_nonce - 1:
                self._next_nonce = nonce


# --------------------------------------------------------------------------- #
#  Execution result
# --------------------------------------------------------------------------- #

@dataclass
class ExecutionResult:
    """Result of an execution attempt."""

    tx_hash: str | None
    gas_limit: int | None
    simulation_latency_ms: float | None
    broadcast_latency_ms: float | None
    total_latency_ms: float | None
    receipt_status: int | None
    receipt_block: int | None
    receipt_gas_used: int | None
    error: str | None = None


# --------------------------------------------------------------------------- #
#  ArbitrumExecutor
# --------------------------------------------------------------------------- #

class ArbitrageExecutor:
    """Transaction executor for the Arbitrum ``FlashArbitrageExecutor`` contract."""

    def __init__(self, w3: Web3, settings: Settings):
        self.w3 = w3
        self.settings = settings
        self._execution_lock = asyncio.Lock()
        self.account: Optional[LocalAccount] = None
        if settings.private_key:
            self.account = Account.from_key(settings.private_key)
        self.nonce_manager: Optional[NonceManager] = (
            NonceManager(w3, self.account.address) if self.account else None
        )
        self.contract = None
        # Use the FlashArbitrageExecutor address (from env)
        executor_addr = settings.flash_executor_address or settings.contract_address
        if executor_addr:
            self.contract = w3.eth.contract(
                address=Web3.to_checksum_address(executor_addr),
                abi=FLASH_ARBITRAGE_EXECUTOR_ABI,
            )

    def wallet_address(self) -> Optional[str]:
        return self.account.address if self.account else None

    def verify_profit_recipient(self) -> None:
        """Fail-fast startup check: read the deployed contract's on-chain
        ``profitRecipient()`` and compare against the configured value.
        """
        if self.contract is None:
            raise ProfitRecipientMismatchError(
                "FLASH_EXECUTOR_ADDRESS is not configured; cannot verify profitRecipient"
            )
        if not self.settings.profit_recipient:
            raise ProfitRecipientMismatchError(
                "PROFIT_RECIPIENT is not set in the environment"
            )
        try:
            on_chain_recipient = self.contract.functions.profitRecipient().call()
        except Exception as exc:
            raise ProfitRecipientMismatchError(
                f"Failed to read on-chain profitRecipient(): {exc}"
            ) from exc

        configured = Web3.to_checksum_address(self.settings.profit_recipient)
        on_chain = Web3.to_checksum_address(on_chain_recipient)

        if configured != on_chain:
            raise ProfitRecipientMismatchError(
                f"PROFIT_RECIPIENT mismatch: .env={configured} but "
                f"on-chain={on_chain}. Refusing to proceed."
            )

        log_fields(
            logger,
            20,
            "profitRecipient verified: .env matches on-chain contract state",
            profit_recipient=configured,
        )

    def _check_deadline(self, deadline: int) -> None:
        """Raise ``StaleOpportunityError`` if the deadline has already passed."""
        try:
            block = self.w3.eth.get_block("latest")
            current_time = int(block.get("timestamp", 0))
        except Exception:
            # If we can't read the block, use system time as fallback
            current_time = int(time.time())

        if current_time > deadline:
            raise StaleOpportunityError(
                f"Opportunity is stale: current block time {current_time} "
                f"exceeds deadline {deadline}"
            )

    def _build_call(
        self,
        asset: str,
        amount: int,
        steps: list[SwapStepArg],
        deadline: int,
        min_profit: int = 0,
    ):
        """Build the ``executeArbitrage`` contract function call."""
        if self.contract is None:
            raise SimulationFailedError("FlashArbitrageExecutor address is not configured")
        steps_tuple = tuple(s.as_tuple() for s in steps)
        return self.contract.functions.executeArbitrage(
            Web3.to_checksum_address(asset),
            int(amount),
            steps_tuple,
            int(deadline),
            int(min_profit),
        )

    def simulate(
        self,
        asset: str,
        amount: int,
        steps: list[SwapStepArg],
        deadline: int,
        min_profit: int = 0,
    ) -> int:
        """Simulate via ``eth_call`` + ``estimateGas``.

        Returns the estimated gas limit on success.
        Raises ``SimulationFailedError`` on any failure, with the classified
        error attached for programmatic handling.
        """
        # Stale opportunity check
        self._check_deadline(deadline)

        call = self._build_call(asset, amount, steps, deadline, min_profit)
        from_addr = self.wallet_address()

        try:
            call.call({"from": from_addr} if from_addr else {})
        except ContractLogicError as exc:
            classified = classify_rpc_error(exc)
            raise SimulationFailedError(
                f"eth_call simulation reverted: {exc}", classified=classified
            ) from exc
        except Exception as exc:
            classified = classify_rpc_error(exc)
            raise SimulationFailedError(
                f"eth_call simulation failed: {exc}", classified=classified
            ) from exc

        try:
            gas_limit = call.estimate_gas({"from": from_addr} if from_addr else {})
        except Exception as exc:
            classified = classify_rpc_error(exc)
            raise SimulationFailedError(
                f"estimate_gas failed: {exc}", classified=classified
            ) from exc

        return gas_limit

    async def execute(
        self,
        asset: str,
        amount: int,
        steps: list[SwapStepArg],
        deadline: int,
        min_profit: int = 0,
        tx_timeout_seconds: int = 120,
    ) -> ExecutionResult:
        """Simulate then (unless DRY_RUN) sign and send the arbitrage transaction.

        Returns an ``ExecutionResult`` with full latency tracking.

        Guarded by an execution lock so concurrent opportunities never race
        against the same wallet/nonce.
        """
        start_time = time.monotonic()

        if self._execution_lock.locked():
            raise ExecutionLockedError("An arbitrage transaction is already in flight")

        async with self._execution_lock:
            # --- Simulation phase ---
            sim_start = time.monotonic()
            try:
                gas_limit = self.simulate(asset, amount, steps, deadline, min_profit)
            except StaleOpportunityError as exc:
                log_fields(logger, 30, "Stale opportunity detected; skipping", error=str(exc))
                return ExecutionResult(
                    tx_hash=None,
                    gas_limit=None,
                    simulation_latency_ms=None,
                    broadcast_latency_ms=None,
                    total_latency_ms=(time.monotonic() - start_time) * 1000,
                    receipt_status=None,
                    receipt_block=None,
                    receipt_gas_used=None,
                    error=str(exc),
                )
            except SimulationFailedError as exc:
                log_fields(
                    logger,
                    30,
                    "Simulation failed; transaction NOT sent",
                    error=str(exc),
                    error_type=exc.classified.error_type.name if exc.classified else "UNKNOWN",
                )
                return ExecutionResult(
                    tx_hash=None,
                    gas_limit=None,
                    simulation_latency_ms=(time.monotonic() - sim_start) * 1000,
                    broadcast_latency_ms=None,
                    total_latency_ms=(time.monotonic() - start_time) * 1000,
                    receipt_status=None,
                    receipt_block=None,
                    receipt_gas_used=None,
                    error=str(exc),
                )

            sim_latency_ms = (time.monotonic() - sim_start) * 1000
            gas_limit = int(gas_limit * float(self.settings.gas_limit_multiplier))

            # A key and DRY_RUN=false are insufficient to authorize a real
            # broadcast. Operators must make the independent, explicit choice
            # EXECUTION_MODE=live.
            if self.settings.dry_run or getattr(self.settings, "execution_mode", "dry_run") != "live":
                log_fields(
                    logger,
                    20,
                    "DRY_RUN active: simulation succeeded, transaction NOT sent",
                    asset=asset,
                    amount=amount,
                    gas_limit=gas_limit,
                    deadline=deadline,
                    min_profit=min_profit,
                    simulation_latency_ms=round(sim_latency_ms, 2),
                )
                return ExecutionResult(
                    tx_hash=None,
                    gas_limit=gas_limit,
                    simulation_latency_ms=sim_latency_ms,
                    broadcast_latency_ms=None,
                    total_latency_ms=(time.monotonic() - start_time) * 1000,
                    receipt_status=None,
                    receipt_block=None,
                    receipt_gas_used=None,
                )

            if self.account is None or self.nonce_manager is None:
                log_fields(
                    logger,
                    40,
                    "Cannot execute live transaction: PRIVATE_KEY not configured",
                )
                return ExecutionResult(
                    tx_hash=None,
                    gas_limit=gas_limit,
                    simulation_latency_ms=sim_latency_ms,
                    broadcast_latency_ms=None,
                    total_latency_ms=(time.monotonic() - start_time) * 1000,
                    receipt_status=None,
                    receipt_block=None,
                    receipt_gas_used=None,
                    error="PRIVATE_KEY not configured",
                )

            # --- Broadcast phase ---
            broadcast_start = time.monotonic()
            nonce = await self.nonce_manager.next_nonce()

            # Arbitrum gas price: use gas_price (Arbitrum handles fee market internally)
            try:
                max_fee = self.w3.eth.gas_price
                max_priority_fee = self.w3.eth.max_priority_fee
            except Exception:
                max_fee = self.w3.eth.gas_price
                max_priority_fee = 0

            call = self._build_call(asset, amount, steps, deadline, min_profit)
            tx = call.build_transaction(
                {
                    "from": self.account.address,
                    "nonce": nonce,
                    "chainId": self.settings.chain_id,
                    "gas": gas_limit,
                    "maxFeePerGas": max_fee,
                    "maxPriorityFeePerGas": max_priority_fee,
                }
            )

            signed = self.account.sign_transaction(tx)

            try:
                tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
            except Exception as exc:
                classified = classify_rpc_error(exc)
                await self.nonce_manager.release_on_failure(nonce)
                log_fields(
                    logger,
                    40,
                    "Transaction broadcast failed",
                    error=str(exc),
                    error_type=classified.error_type.name,
                    nonce=nonce,
                )
                return ExecutionResult(
                    tx_hash=None,
                    gas_limit=gas_limit,
                    simulation_latency_ms=sim_latency_ms,
                    broadcast_latency_ms=(time.monotonic() - broadcast_start) * 1000,
                    total_latency_ms=(time.monotonic() - start_time) * 1000,
                    receipt_status=None,
                    receipt_block=None,
                    receipt_gas_used=None,
                    error=f"Broadcast failed: {exc}",
                )

            broadcast_latency_ms = (time.monotonic() - broadcast_start) * 1000

            log_fields(
                logger,
                20,
                "Transaction submitted",
                wallet=self.account.address,
                chain_id=self.settings.chain_id,
                nonce=nonce,
                tx_hash=tx_hash.hex(),
                simulation_latency_ms=round(sim_latency_ms, 2),
                broadcast_latency_ms=round(broadcast_latency_ms, 2),
            )

            # --- Receipt phase ---
            try:
                receipt = self.w3.eth.wait_for_transaction_receipt(
                    tx_hash, timeout=tx_timeout_seconds
                )
                log_fields(
                    logger,
                    20,
                    "Transaction mined",
                    tx_hash=tx_hash.hex(),
                    status=receipt.status,
                    block_number=receipt.blockNumber,
                    gas_used=receipt.gasUsed,
                )
                return ExecutionResult(
                    tx_hash=tx_hash.hex(),
                    gas_limit=gas_limit,
                    simulation_latency_ms=sim_latency_ms,
                    broadcast_latency_ms=broadcast_latency_ms,
                    total_latency_ms=(time.monotonic() - start_time) * 1000,
                    receipt_status=receipt.status,
                    receipt_block=receipt.blockNumber,
                    receipt_gas_used=receipt.gasUsed,
                )
            except TimeExhausted:
                log_fields(
                    logger,
                    30,
                    "Transaction timed out waiting for receipt (may still mine later)",
                    tx_hash=tx_hash.hex(),
                    timeout_seconds=tx_timeout_seconds,
                )
                return ExecutionResult(
                    tx_hash=tx_hash.hex(),
                    gas_limit=gas_limit,
                    simulation_latency_ms=sim_latency_ms,
                    broadcast_latency_ms=broadcast_latency_ms,
                    total_latency_ms=(time.monotonic() - start_time) * 1000,
                    receipt_status=None,
                    receipt_block=None,
                    receipt_gas_used=None,
                    error=f"Transaction not mined within {tx_timeout_seconds}s",
                )
