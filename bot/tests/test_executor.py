"""Unit tests for bot.executor.ArbitrageExecutor.

Uses lightweight stub objects instead of a real web3/RPC connection so this test can run
fully offline, matching the other pure-logic tests in this directory.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from bot.executor import ArbitrageExecutor, ProfitRecipientMismatchError


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
    private_key: str = ""
    chain_id: int = 137
    gas_limit_multiplier: object = 1.2


def _make_executor(configured_recipient: str, on_chain_recipient: str) -> ArbitrageExecutor:
    settings = _StubSettings(
        contract_address="0x0000000000000000000000000000000000dEaD",
        profit_recipient=configured_recipient,
    )
    # Bypass __init__'s real Web3 wiring by constructing a bare instance and setting attrs
    # directly -- avoids needing a real RPC connection for this pure logic test.
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = settings
    executor.contract = _StubContract(on_chain_recipient)
    return executor


def test_verify_profit_recipient_passes_when_addresses_match_case_insensitively():
    executor = _make_executor(
        configured_recipient="0x000000000000000000000000000000000000bEEF".lower(),
        on_chain_recipient="0x000000000000000000000000000000000000BEEF",
    )
    executor.verify_profit_recipient()  # must not raise


def test_verify_profit_recipient_fails_fast_on_mismatch():
    executor = _make_executor(
        configured_recipient="0x000000000000000000000000000000000000BEEF",
        on_chain_recipient="0x000000000000000000000000000000000000dEaD",
    )
    with pytest.raises(ProfitRecipientMismatchError):
        executor.verify_profit_recipient()


def test_verify_profit_recipient_fails_fast_when_not_configured():
    executor = _make_executor(
        configured_recipient="",
        on_chain_recipient="0x000000000000000000000000000000000000BEEF",
    )
    with pytest.raises(ProfitRecipientMismatchError):
        executor.verify_profit_recipient()


def test_verify_profit_recipient_fails_fast_when_contract_not_configured():
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = _StubSettings(contract_address="", profit_recipient="0xBEEF")
    executor.contract = None
    with pytest.raises(ProfitRecipientMismatchError):
        executor.verify_profit_recipient()


class _SentinelSendCalled(Exception):
    """Raised by the stubbed send path below if it is ever reached -- proves DRY_RUN=true
    structurally prevents execution from reaching transaction signing/sending.
    """


def test_execute_never_sends_a_transaction_when_dry_run_is_true(monkeypatch):
    """Audit requirement: DRY_RUN=true must make it impossible to submit a live transaction
    through ANY code path in `execute()`, not merely via a single `if` check deep inside. This
    test stubs out `simulate` (so we don't need a real RPC) and then monkeypatches the
    account/nonce manager to raise if ever touched -- if `execute()` returns None without ever
    accessing `self.account` or `self.nonce_manager`, DRY_RUN is proven to short-circuit before
    any signing/sending logic runs.
    """
    executor = ArbitrageExecutor.__new__(ArbitrageExecutor)
    executor.settings = _StubSettings(contract_address="0x" + "1" * 40, profit_recipient="0x" + "2" * 40)
    executor.settings.dry_run = True  # type: ignore[attr-defined]
    executor.settings.gas_limit_multiplier = 1.2  # type: ignore[attr-defined]
    executor.contract = _StubContract("0x" + "2" * 40)
    executor._execution_lock = asyncio.Lock()
    executor.simulate = lambda *a, **k: 500_000  # pretend simulation succeeded

    class _PoisonedAccount:
        @property
        def address(self):
            raise _SentinelSendCalled("account accessed during DRY_RUN")

    class _PoisonedNonceManager:
        async def next_nonce(self):
            raise _SentinelSendCalled("nonce manager accessed during DRY_RUN")

    executor.account = _PoisonedAccount()
    executor.nonce_manager = _PoisonedNonceManager()

    result = asyncio.run(executor.execute("0x" + "3" * 40, 1_000_000, [], 9_999_999_999))
    assert result is None
