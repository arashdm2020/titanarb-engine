"""Transaction simulation, signing, and submission.

Security / safety properties enforced here:
  - Private key is loaded once via `Account.from_key`, only from an in-memory
    string sourced from the environment (never logged, never written to disk).
  - An asyncio lock (`_execution_lock`) prevents two arbitrage transactions from
    being built/sent concurrently against the same wallet.
  - Nonce is read using the PENDING transaction count (`pending` block
    parameter), not just the latest mined count, to avoid nonce collisions with
    a transaction this process already submitted but that has not yet mined.
  - Every transaction is simulated via `eth_call` (using the exact same call
    data) immediately before sending; if the simulation reverts, the
    transaction is NOT sent.
  - DRY_RUN=true (the default) guarantees no transaction is ever sent: the
    "send" step is skipped entirely and only logged.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from eth_account import Account
from eth_account.signers.local import LocalAccount
from web3 import Web3
from web3.exceptions import ContractLogicError, TimeExhausted

from .config_legacy_polygon import Settings
from .logger import get_logger, log_fields

ABI_DIR = Path(__file__).resolve().parent / "abi"
logger = get_logger(__name__)


def _load_abi(name: str) -> list:
    with open(ABI_DIR / name, "r", encoding="utf-8") as fh:
        return json.load(fh)


FLASH_ARB_ABI = _load_abi("flash_arb.json")


class SimulationFailedError(Exception):
    pass


class ExecutionLockedError(Exception):
    pass


class ProfitRecipientMismatchError(Exception):
    """Raised at startup when the configured PROFIT_RECIPIENT does not match the deployed
    contract's on-chain `profitRecipient()`. This is a fail-fast safety check: if these
    disagree, either the .env is misconfigured, the contract was reconfigured out-of-band, or
    CONTRACT_ADDRESS points at the wrong deployment -- in every case, the bot must refuse to
    proceed rather than silently execute against unexpected on-chain state.
    """


@dataclass
class SwapStepArg:
    router: str
    token_in: str
    token_out: str
    fee: int
    amount_out_minimum: int

    def as_tuple(self) -> tuple:
        return (
            Web3.to_checksum_address(self.router),
            Web3.to_checksum_address(self.token_in),
            Web3.to_checksum_address(self.token_out),
            int(self.fee),
            int(self.amount_out_minimum),
        )


class NonceManager:
    """Tracks the next nonce to use, reconciling against the chain's pending
    transaction count so that back-to-back submissions (or a restart after a
    still-pending tx) do not collide.
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
        """Call if a transaction using `nonce` failed to broadcast at all (e.g. RPC
        rejected it before it entered the mempool), so the nonce can be reused instead
        of being permanently skipped.
        """
        async with self._lock:
            if self._next_nonce is not None and nonce == self._next_nonce - 1:
                self._next_nonce = nonce


class ArbitrageExecutor:
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
        if settings.contract_address:
            self.contract = w3.eth.contract(
                address=Web3.to_checksum_address(settings.contract_address), abi=FLASH_ARB_ABI
            )

    def wallet_address(self) -> Optional[str]:
        return self.account.address if self.account else None

    def verify_profit_recipient(self) -> None:
        """Fail-fast startup check: read the deployed contract's on-chain
        `profitRecipient()` and compare it (checksum-address-normalized) against the
        configured `PROFIT_RECIPIENT` env var. Raises `ProfitRecipientMismatchError`
        on any mismatch or missing configuration -- this method must be called
        before any arbitrage execution is attempted.
        """
        if self.contract is None:
            raise ProfitRecipientMismatchError(
                "CONTRACT_ADDRESS is not configured; cannot verify profitRecipient on-chain"
            )
        if not self.settings.profit_recipient:
            raise ProfitRecipientMismatchError(
                "PROFIT_RECIPIENT is not set in the environment; refusing to proceed without an "
                "explicit, verified profit destination"
            )
        try:
            on_chain_recipient = self.contract.functions.profitRecipient().call()
        except Exception as exc:
            raise ProfitRecipientMismatchError(f"Failed to read on-chain profitRecipient(): {exc}") from exc

        configured = Web3.to_checksum_address(self.settings.profit_recipient)
        on_chain = Web3.to_checksum_address(on_chain_recipient)

        if configured != on_chain:
            raise ProfitRecipientMismatchError(
                f"PROFIT_RECIPIENT mismatch: .env configured={configured} but on-chain "
                f"contract.profitRecipient()={on_chain}. Refusing to proceed."
            )

        log_fields(
            logger,
            20,
            "profitRecipient verified: .env matches on-chain contract state",
            profit_recipient=configured,
        )

    def _build_call(self, asset: str, amount: int, steps: list[SwapStepArg], deadline: int):
        if self.contract is None:
            raise SimulationFailedError("CONTRACT_ADDRESS is not configured")
        steps_tuple = tuple(s.as_tuple() for s in steps)
        return self.contract.functions.executeArbitrage(
            Web3.to_checksum_address(asset), int(amount), steps_tuple, int(deadline)
        )

    def simulate(self, asset: str, amount: int, steps: list[SwapStepArg], deadline: int) -> int:
        """Simulate via eth_call + estimate_gas. Raises SimulationFailedError (never
        lets a failing simulation silently pass) so the caller can safely skip
        sending. Returns the estimated gas limit on success.
        """
        call = self._build_call(asset, amount, steps, deadline)
        from_addr = self.wallet_address()
        try:
            call.call({"from": from_addr} if from_addr else {})
        except ContractLogicError as exc:
            raise SimulationFailedError(f"eth_call simulation reverted: {exc}") from exc
        except Exception as exc:
            raise SimulationFailedError(f"eth_call simulation failed: {exc}") from exc

        try:
            gas_limit = call.estimate_gas({"from": from_addr} if from_addr else {})
        except Exception as exc:
            raise SimulationFailedError(f"estimate_gas failed: {exc}") from exc

        return gas_limit

    async def execute(
        self,
        asset: str,
        amount: int,
        steps: list[SwapStepArg],
        deadline: int,
    ) -> Optional[str]:
        """Simulate then (unless DRY_RUN) sign and send the arbitrage transaction.
        Returns the transaction hash if sent, or None in dry-run / on simulation
        failure. Guarded by an execution lock so concurrent opportunities never
        race against the same wallet/nonce.
        """
        if self._execution_lock.locked():
            raise ExecutionLockedError("An arbitrage transaction is already in flight")

        async with self._execution_lock:
            try:
                gas_limit = self.simulate(asset, amount, steps, deadline)
            except SimulationFailedError as exc:
                log_fields(logger, 30, "Simulation failed; transaction NOT sent", error=str(exc))
                return None

            gas_limit = int(gas_limit * float(self.settings.gas_limit_multiplier))

            if self.settings.dry_run:
                log_fields(
                    logger,
                    20,
                    "DRY_RUN active: simulation succeeded, transaction NOT sent",
                    asset=asset,
                    amount=amount,
                    gas_limit=gas_limit,
                    deadline=deadline,
                )
                return None

            if self.account is None or self.nonce_manager is None:
                log_fields(logger, 40, "Cannot execute live transaction: PRIVATE_KEY not configured")
                return None

            nonce = await self.nonce_manager.next_nonce()
            max_fee, max_priority_fee = self.w3.eth.max_priority_fee, self.w3.eth.max_priority_fee
            try:
                latest = self.w3.eth.get_block("latest")
                base_fee = latest.get("baseFeePerGas", self.w3.eth.gas_price)
                max_fee = int(base_fee * 2) + int(max_priority_fee)
            except Exception:
                max_fee = self.w3.eth.gas_price

            call = self._build_call(asset, amount, steps, deadline)
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
                await self.nonce_manager.release_on_failure(nonce)
                log_fields(logger, 40, "Transaction broadcast failed", error=str(exc))
                return None

            log_fields(
                logger,
                20,
                "Transaction submitted",
                wallet=self.account.address,
                chain_id=self.settings.chain_id,
                nonce=nonce,
                tx_hash=tx_hash.hex(),
            )

            try:
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
                log_fields(
                    logger,
                    20,
                    "Transaction mined",
                    tx_hash=tx_hash.hex(),
                    status=receipt.status,
                    block_number=receipt.blockNumber,
                    gas_used=receipt.gasUsed,
                )
            except TimeExhausted:
                log_fields(logger, 30, "Timed out waiting for receipt (may still mine later)", tx_hash=tx_hash.hex())

            return tx_hash.hex()
