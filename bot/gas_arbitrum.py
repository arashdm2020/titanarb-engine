"""Arbitrum-specific gas estimation.

Arbitrum gas costs differ fundamentally from L1 EIP-1559 chains:
  - **L2 Execution Fee**: covers L2 computation, analogous to normal gas.
  - **L1 Data Posting Fee**: covers the cost of posting transaction data to L1
    (Ethereum mainnet).  This is proportional to the calldata size of the
    transaction and is often the dominant cost component on Arbitrum.

This module reads gas price components from the **ArbGasInfo** precompile
(``0x006C``) and combines them with the transaction's calldata size to produce
a total gas cost estimate in wei, which can then be converted to USD using
the ETH/USD Chainlink oracle.

Formula:

    l2_fee  = per_l2_tx + (gas_limit * per_arb_gas_total)
    l1_fee  = NodeInterface.gasEstimateComponents(...).gasEstimateForL1 * baseFee
    total   = (l2_fee + l1_fee) * (1 + safety_margin_bps / 10_000)

All monetary results are returned as ``Decimal``.  Native-token cost is exact
(computed from integer wei); USD conversion depends on the freshness of the
supplied ETH/USD price.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from web3 import Web3
from web3.contract import Contract

from .logger import get_logger, log_fields

logger = get_logger(__name__)

ABI_DIR = Path(__file__).resolve().parent / "abi"
WEI_PER_ETHER = Decimal(10) ** 18

# Kept only for the legacy fallback used with minimal/mock RPCs that do not
# implement Arbitrum's NodeInterface. Real Arbitrum estimates use the node's
# compression-aware L1 component below.
L1_CALLDATA_UNIT_SIZE = 16
NODE_INTERFACE = "0x00000000000000000000000000000000000000C8"
_NODE_INTERFACE_GAS_ABI = [{
    "inputs": [
        {"name": "to", "type": "address"},
        {"name": "contractCreation", "type": "bool"},
        {"name": "data", "type": "bytes"},
    ],
    "name": "gasEstimateComponents",
    "outputs": [
        {"name": "gasEstimate", "type": "uint64"},
        {"name": "gasEstimateForL1", "type": "uint64"},
        {"name": "baseFee", "type": "uint256"},
        {"name": "l1BaseFeeEstimate", "type": "uint256"},
    ],
    "stateMutability": "view",
    "type": "function",
}]


def _load_abi(name: str) -> list:
    with open(ABI_DIR / name, "r", encoding="utf-8") as fh:
        return json.load(fh)


ARB_GAS_INFO_ABI = _load_abi("arb_gas_info.json")


class GasEstimationError(Exception):
    """Raised when gas parameters cannot be read from the ArbGasInfo precompile."""


@dataclass(frozen=True)
class ArbitrumGasBreakdown:
    """Detailed breakdown of an Arbitrum gas cost estimate."""

    # L2 execution fee (wei)
    l2_execution_fee_wei: int
    # L1 data posting fee (wei)
    l1_data_fee_wei: int
    # Raw total before safety margin (wei)
    raw_total_wei: int
    # Safety margin applied (wei)
    safety_margin_wei: int
    # Total gas cost including safety margin (wei)
    total_gas_cost_wei: int

    # Gas limit used for the estimate
    gas_limit: int
    # Calldata size in bytes
    calldata_size: int

    # Derived costs
    gas_cost_eth: Decimal
    gas_cost_usd: Decimal | None

    # Raw price components from ArbGasInfo (for logging / debugging)
    per_l2_tx: int
    per_l1_calldata_unit: int
    per_arb_gas_total: int


def _get_arb_gas_info_contract(w3: Web3, precompile_address: str) -> Contract:
    """Return an ``ArbGasInfo`` contract instance."""
    return w3.eth.contract(
        address=Web3.to_checksum_address(precompile_address),
        abi=ARB_GAS_INFO_ABI,
    )


def get_arb_gas_prices(w3: Web3, precompile_address: str) -> tuple[int, int, int]:
    """Read current gas price components from the ArbGasInfo precompile.

    Returns ``(per_l2_tx, per_l1_calldata_unit, per_arb_gas_total)`` in wei.

    - ``per_l2_tx``: fixed fee per L2 transaction (wei)
    - ``per_l1_calldata_unit``: price per 16-byte unit of L1 calldata (wei)
    - ``per_arb_gas_total``: price per unit of L2 computational gas (wei)
    """
    contract = _get_arb_gas_info_contract(w3, precompile_address)
    try:
        result = contract.functions.getPricesInWei().call()
    except Exception as exc:
        raise GasEstimationError(
            f"Failed to read ArbGasInfo.getPricesInWei: {exc}"
        ) from exc

    # getPricesInWei returns 6 values:
    # (perL2Tx, perL1CalldataUnit, perStorageAllocation,
    #  perArbGasBase, perArbGasCongestion, perArbGasTotal)
    per_l2_tx = int(result[0])
    per_l1_calldata_unit = int(result[1])
    per_arb_gas_total = int(result[5])

    return per_l2_tx, per_l1_calldata_unit, per_arb_gas_total


def estimate_calldata_size(tx: dict) -> int:
    """Estimate the calldata size (in bytes) for a transaction dict.

    The calldata is the ``data`` field of the transaction.  If the transaction
    is a contract call built by web3.py, this will be the ABI-encoded function
    call.  Each hex character represents 4 bits, so bytes = len(hex_data) // 2,
    minus the ``0x`` prefix.
    """
    data = tx.get("data", "0x")
    if isinstance(data, bytes):
        return len(data)
    if isinstance(data, str):
        if data.startswith("0x"):
            data = data[2:]
        return len(data) // 2
    return 0


def _node_l1_data_fee_wei(w3: Web3, tx: dict) -> int | None:
    """Return the node's compression-aware L1 posting fee, if available.

    ``perL1CalldataUnit`` is not a simple 16-byte-chunk price: Nitro charges
    compressed calldata units.  The NodeInterface exposes the authoritative
    L1 gas component for a payload.  A zero-code destination is sufficient
    here because this query is only measuring the posted bytes, not execution.
    """
    data = tx.get("data", "0x")
    if isinstance(data, str):
        data = bytes.fromhex(data.removeprefix("0x"))
    if not isinstance(data, bytes):
        return None
    try:
        node = w3.eth.contract(address=NODE_INTERFACE, abi=_NODE_INTERFACE_GAS_ABI)
        _total, l1_gas, base_fee, _l1_base_fee = node.functions.gasEstimateComponents(
            "0x000000000000000000000000000000000000dEaD", False, data
        ).call()
        return int(l1_gas) * int(base_fee)
    except Exception:
        return None


def estimate_arbitrum_gas_cost(
    w3: Web3,
    tx: dict,
    gas_limit: int,
    precompile_address: str,
    safety_margin_bps: int = 1000,
    eth_usd_price: Decimal | None = None,
) -> ArbitrumGasBreakdown:
    """Estimate the total gas cost for a transaction on Arbitrum.

    Parameters
    ----------
    w3 :
        Web3 instance connected to an Arbitrum One RPC.
    tx :
        Transaction dict (must contain ``data`` for calldata size estimation).
    gas_limit :
        Estimated gas limit for the L2 execution.
    precompile_address :
        Address of the ArbGasInfo precompile (``0x006C``).
    safety_margin_bps :
        Safety margin in basis points (default 1000 = 10%).
    eth_usd_price :
        Current ETH/USD price from Chainlink, for USD conversion.

    Returns
    -------
    ArbitrumGasBreakdown
        Detailed breakdown of L2, L1, and total gas costs.

    Raises
    ------
    GasEstimationError
        If the ArbGasInfo precompile cannot be read.
    """
    per_l2_tx, per_l1_calldata_unit, per_arb_gas_total = get_arb_gas_prices(
        w3, precompile_address
    )

    calldata_size = estimate_calldata_size(tx)
    calldata_units = (calldata_size + L1_CALLDATA_UNIT_SIZE - 1) // L1_CALLDATA_UNIT_SIZE

    # L2 execution fee: fixed per-tx fee + gas * per-arb-gas price
    l2_execution_fee_wei = per_l2_tx + (gas_limit * per_arb_gas_total)

    # Prefer Nitro's compression-aware node result. The old chunk calculation
    # is retained only for non-Arbitrum/mock providers, where it is explicitly
    # an approximation rather than a validated L1 fee.
    l1_data_fee_wei = _node_l1_data_fee_wei(w3, tx)
    if l1_data_fee_wei is None:
        l1_data_fee_wei = calldata_units * per_l1_calldata_unit

    raw_total_wei = l2_execution_fee_wei + l1_data_fee_wei

    # Apply safety margin
    margin_numerator = Decimal(raw_total_wei) * Decimal(safety_margin_bps)
    safety_margin_wei = int(margin_numerator / Decimal(10_000))
    total_gas_cost_wei = raw_total_wei + safety_margin_wei

    gas_cost_eth = Decimal(total_gas_cost_wei) / WEI_PER_ETHER

    gas_cost_usd: Decimal | None = None
    if eth_usd_price is not None:
        gas_cost_usd = gas_cost_eth * eth_usd_price

    log_fields(
        logger,
        10,  # DEBUG
        "Arbitrum gas estimate",
        gas_limit=gas_limit,
        calldata_size=calldata_size,
        calldata_units=calldata_units,
        l2_fee_wei=l2_execution_fee_wei,
        l1_fee_wei=l1_data_fee_wei,
        raw_total_wei=raw_total_wei,
        safety_margin_wei=safety_margin_wei,
        total_wei=total_gas_cost_wei,
        gas_cost_eth=str(gas_cost_eth),
        gas_cost_usd=str(gas_cost_usd) if gas_cost_usd is not None else None,
    )

    return ArbitrumGasBreakdown(
        l2_execution_fee_wei=l2_execution_fee_wei,
        l1_data_fee_wei=l1_data_fee_wei,
        raw_total_wei=raw_total_wei,
        safety_margin_wei=safety_margin_wei,
        total_gas_cost_wei=total_gas_cost_wei,
        gas_limit=gas_limit,
        calldata_size=calldata_size,
        gas_cost_eth=gas_cost_eth,
        gas_cost_usd=gas_cost_usd,
        per_l2_tx=per_l2_tx,
        per_l1_calldata_unit=per_l1_calldata_unit,
        per_arb_gas_total=per_arb_gas_total,
    )
