"""Gas estimation and cost calculation.

All monetary results are returned as `Decimal`. Native-token cost is exact
(computed from integer wei); USD conversion is approximate and depends on the
freshness of the supplied MATIC/POL-USD price.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from web3 import Web3

WEI_PER_ETHER = Decimal(10) ** 18


@dataclass(frozen=True)
class GasEstimate:
    gas_limit: int
    max_fee_per_gas_wei: int
    max_priority_fee_per_gas_wei: int
    gas_cost_native: Decimal  # in MATIC/POL
    gas_cost_usd: Decimal | None


def get_fee_estimate(w3: Web3) -> tuple[int, int]:
    """Return (max_fee_per_gas, max_priority_fee_per_gas) in wei using Polygon's
    EIP-1559 fee market. Falls back to `eth_gasPrice` if fee history is unavailable
    (some Polygon RPC providers throttle `eth_feeHistory`).
    """
    try:
        latest = w3.eth.get_block("latest")
        base_fee = latest.get("baseFeePerGas")
        if base_fee is None:
            raise ValueError("no baseFeePerGas in latest block")
        priority_fee = w3.eth.max_priority_fee
        max_fee = int(base_fee * 2) + int(priority_fee)
        return max_fee, int(priority_fee)
    except Exception:
        gas_price = w3.eth.gas_price
        return int(gas_price), int(gas_price)


def estimate_gas_cost(
    w3: Web3,
    tx: dict,
    gas_limit_multiplier: Decimal,
    native_usd_price: Decimal | None = None,
) -> GasEstimate:
    """Estimate gas for `tx` via `eth_estimateGas`, apply a safety multiplier, and
    compute native + USD cost using current fee-market pricing.

    Raises whatever exception `eth_estimateGas` raises (e.g. execution reverted)
    so the caller can treat a failed estimate as "do not submit this transaction".
    """
    raw_gas_limit = w3.eth.estimate_gas(tx)
    gas_limit = int(Decimal(raw_gas_limit) * gas_limit_multiplier)

    max_fee, max_priority_fee = get_fee_estimate(w3)

    gas_cost_wei = Decimal(gas_limit) * Decimal(max_fee)
    gas_cost_native = gas_cost_wei / WEI_PER_ETHER

    gas_cost_usd = None
    if native_usd_price is not None:
        gas_cost_usd = gas_cost_native * native_usd_price

    return GasEstimate(
        gas_limit=gas_limit,
        max_fee_per_gas_wei=max_fee,
        max_priority_fee_per_gas_wei=max_priority_fee,
        gas_cost_native=gas_cost_native,
        gas_cost_usd=gas_cost_usd,
    )
