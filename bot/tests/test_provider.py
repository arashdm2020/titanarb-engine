"""Unit tests for bot.provider (RPC error classification, health checks)."""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from web3.exceptions import ContractLogicError

from bot.provider import (
    ClassifiedRpcError,
    RpcErrorType,
    classify_rpc_error,
    is_healthy,
    health_check_detail,
)


def test_classify_contract_revert():
    exc = ContractLogicError("execution reverted: InsufficientRepayment")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.CONTRACT_REVERT
    assert result.is_simulation_revert is True
    assert result.is_retryable is False


def test_classify_nonce_too_low():
    exc = Exception("nonce too low")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.NONCE_TOO_LOW
    assert result.is_retryable is False


def test_classify_insufficient_funds():
    exc = Exception("insufficient funds for gas")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.INSUFFICIENT_FUNDS


def test_classify_rate_limited():
    exc = Exception("429 Too Many Requests")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.RATE_LIMITED
    assert result.is_retryable is True


def test_classify_timeout():
    exc = Exception("request timeout")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.TIMEOUT
    assert result.is_retryable is True


def test_classify_network_error():
    exc = ConnectionError("connection refused")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.NETWORK_ERROR
    assert result.is_retryable is True


def test_classify_sequencer_down():
    exc = Exception("sequencer is down")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.SEQUENCER_DOWN
    assert result.is_retryable is True


def test_classify_unknown():
    exc = Exception("something weird happened")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.UNKNOWN
    assert result.is_retryable is False


def test_classify_tx_already_known():
    exc = Exception("already known")
    result = classify_rpc_error(exc)
    assert result.error_type == RpcErrorType.TX_ALREADY_KNOWN


def test_is_healthy_true():
    w3 = MagicMock()
    w3.eth.block_number = 12345
    assert is_healthy(w3) is True


def test_is_healthy_false():
    w3 = MagicMock()
    type(w3.eth).block_number = PropertyMock(side_effect=Exception("rpc down"))
    assert is_healthy(w3) is False


def test_health_check_detail_healthy():
    w3 = MagicMock()
    type(w3.eth).block_number = PropertyMock(return_value=12345)
    type(w3.eth).chain_id = PropertyMock(return_value=42161)
    result = health_check_detail(w3)
    assert result["healthy"] is True
    assert result["block_number"] == 12345
    assert result["chain_id"] == 42161


def test_health_check_detail_unhealthy():
    w3 = MagicMock()
    type(w3.eth).block_number = PropertyMock(side_effect=ConnectionError("refused"))
    result = health_check_detail(w3)
    assert result["healthy"] is False
    assert "error_type" in result
