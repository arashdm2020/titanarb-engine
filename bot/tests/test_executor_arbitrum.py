"""Unit tests for bot.executor_arbitrum (Arbitrum executor with FlashArbitrageExecutor)."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest
from web3.exceptions import ContractLogicError

from bot.executor_arbitrum import (
    ArbitrageExecutor,
    ExecutionResult,
    ExecutionLockedError,
    ProfitRecipientMismatchError,
    SimulationFailedError,
    StaleOpportunityError,
    SwapStepArg,
    encode_uniswap_v3_data,
    encode_camelot_v3_data,
)


# --- SwapStepArg tests ---

def test_swap_step_arg_as_tuple():
    step = SwapStepArg(
        adapter="0x" + "1" * 40,
        token_in="0x" + "2" * 40,
        token_out="0x" + "3" * 40,
        amount_out_minimum=1000,
        data=b"\x00\x01\x02",
    )
    t = step.as_tuple()
    assert t[0] == "0x" + "1" * 40  # adapter (checksummed)
    assert t[1] == "0x" + "2" * 40  # token_in
    assert t[2] == "0x" + "3" * 40  # token_out
    assert t[3] == 1000  # amount_out_minimum
    assert t[4] == b"\x00\x01\x02"  # data


def test_encode_uniswap_v3_data():
    data = encode_uniswap_v3_data(500)  # 0.05% fee
    # Should be 32 bytes (ABI-encoded uint24 is padded to 32 bytes)
    assert len(data) == 32
    # The last 3 bytes should represent 500 = 0x0001F4
    assert data[-3:] == (500).to_bytes(3, "big")


def test_encode_camelot_v3_data():
    data = encode_camelot_v3_data()
    assert data == b""


# --- Executor tests (using stubs) ---

class _StubFunctionCall:
    def __init__(self, return_value):
        self._return_value = return_value

    def call(self, *_args, **_kwargs):
        return self._return_value


class _StubFunctions:
    def __init__(self, profit_recipient_value: str):
        self._profit_recipient_value = profit_recipient_value

    def profitRecipient(self):
        return _StubFunctionCall(self._profit_recipient_value)


class _StubContract:
    def __init__(self, profit_recipient_value: str):
        self.functions = _StubFunctions(profit_recipient_value)


@dataclass
class _StubSettings:
    contract_address: str
    profit_recipient: str
    flash_executor_address: str
    private_key: str = ""
    chain_id: int = 42161
    gas_limit_multiplier: float = 1.2
    dry_run: bool = True
    uniswap_v3_adapter: str = ""
    camelot_v3_adapter: str = ""
    slippage_bps: int = 50
    min_profit_usd: object = 5
    min_loan_amount: object = 1000
    max_loan_amount: object = 50000
    aave_fee_bps: int = 5
    tx_deadline_seconds: int = 120
    gas_safety_margin_bps: int = 1000
    min_pool_liquidity: int = 1000000000000
    loan_search_steps: int = 8
    log_level: str = "INFO"
    sequencer_grace_period_seconds: int = 3600
    price_max_staleness_seconds: int = 3600
    sequencer_heartbeat_seconds: int = 1800


def _make_executor(
    configured_recipient: str = "0x" + "2" * 40,
    on_chain_recipient: str = "0x" + "2" * 40,
    flash_executor: str = "0x" + "1" * 40,
) -> ArbitrageExecutor:
    settings = _StubSettings(
        contract_address=flash_executor,
        profit_recipient=configured_recipient,
        flash_executor_address=flash_executor,
    )
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = settings
    executor.contract = _StubContract(on_chain_recipient)
    return executor


def test_verify_profit_recipient_passes_when_matching():
    executor = _make_executor(
        configured_recipient="0x000000000000000000000000000000000000bEEF".lower(),
        on_chain_recipient="0x000000000000000000000000000000000000BEEF",
    )
    executor.verify_profit_recipient()  # must not raise


def test_verify_profit_recipient_fails_on_mismatch():
    executor = _make_executor(
        configured_recipient="0x" + "1" * 40,
        on_chain_recipient="0x" + "2" * 40,
    )
    with pytest.raises(ProfitRecipientMismatchError):
        executor.verify_profit_recipient()


def test_verify_profit_recipient_fails_when_not_configured():
    executor = _make_executor(
        configured_recipient="",
        on_chain_recipient="0x" + "2" * 40,
    )
    with pytest.raises(ProfitRecipientMismatchError):
        executor.verify_profit_recipient()


def test_verify_profit_recipient_fails_when_contract_not_configured():
    settings = _StubSettings(
        contract_address="",
        profit_recipient="0xBEEF",
        flash_executor_address="",
    )
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = settings
    executor.contract = None
    with pytest.raises(ProfitRecipientMismatchError):
        executor.verify_profit_recipient()


# --- DRY_RUN test ---

class _SentinelSendCalled(Exception):
    pass


def test_execute_never_sends_when_dry_run_is_true():
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = _StubSettings(
        contract_address="0x" + "1" * 40,
        profit_recipient="0x" + "2" * 40,
        flash_executor_address="0x" + "1" * 40,
        dry_run=True,
    )
    executor.contract = _StubContract("0x" + "2" * 40)
    executor._execution_lock = asyncio.Lock()
    executor.simulate = lambda *a, **k: 500_000

    class _PoisonedAccount:
        @property
        def address(self):
            raise _SentinelSendCalled("account accessed during DRY_RUN")

    class _PoisonedNonceManager:
        async def next_nonce(self):
            raise _SentinelSendCalled("nonce manager accessed during DRY_RUN")

    executor.account = _PoisonedAccount()
    executor.nonce_manager = _PoisonedNonceManager()

    result = asyncio.run(
        executor.execute("0x" + "3" * 40, 1_000_000, [], 9_999_999_999)
    )
    assert result.tx_hash is None
    assert result.gas_limit is not None


# --- Stale opportunity test ---

def test_execute_returns_error_on_stale_opportunity():
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = _StubSettings(
        contract_address="0x" + "1" * 40,
        profit_recipient="0x" + "2" * 40,
        flash_executor_address="0x" + "1" * 40,
        dry_run=True,
    )
    executor.contract = _StubContract("0x" + "2" * 40)
    executor._execution_lock = asyncio.Lock()

    # Mock _check_deadline to raise StaleOpportunityError
    def _raise_stale(deadline):
        raise StaleOpportunityError("stale")
    executor._check_deadline = _raise_stale

    result = asyncio.run(
        executor.execute("0x" + "3" * 40, 1_000_000, [], 100)
    )
    assert result.tx_hash is None
    assert result.error is not None
    assert "stale" in result.error.lower()


# --- Simulation failure test ---

def test_execute_returns_error_on_simulation_failure():
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = _StubSettings(
        contract_address="0x" + "1" * 40,
        profit_recipient="0x" + "2" * 40,
        flash_executor_address="0x" + "1" * 40,
        dry_run=True,
    )
    executor.contract = _StubContract("0x" + "2" * 40)
    executor._execution_lock = asyncio.Lock()

    def _raise_sim_fail(*a, **k):
        raise SimulationFailedError("reverted")
    executor.simulate = _raise_sim_fail

    result = asyncio.run(
        executor.execute("0x" + "3" * 40, 1_000_000, [], 9_999_999_999)
    )
    assert result.tx_hash is None
    assert result.error is not None
    assert "reverted" in result.error.lower()


# --- Execution lock test ---

def test_execute_raises_when_lock_held():
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = _StubSettings(
        contract_address="0x" + "1" * 40,
        profit_recipient="0x" + "2" * 40,
        flash_executor_address="0x" + "1" * 40,
        dry_run=True,
    )
    executor.contract = _StubContract("0x" + "2" * 40)
    executor._execution_lock = asyncio.Lock()

    # Hold the lock manually
    loop = asyncio.new_event_loop()
    loop.run_until_complete(executor._execution_lock.acquire())

    with pytest.raises(ExecutionLockedError):
        loop.run_until_complete(
            executor.execute("0x" + "3" * 40, 1_000_000, [], 9_999_999_999)
        )
    loop.close()
