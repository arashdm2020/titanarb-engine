"""Uniswap V3 pool discovery and state reading.

Pool addresses are never hardcoded or assumed: they are resolved on-chain via
`UniswapV3Factory.getPool(tokenA, tokenB, fee)`. token0/token1 ordering is
never assumed either -- it is always read directly from the pool and used to
correctly orient spot-price math regardless of whether the token of interest
is token0 or token1.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from web3 import Web3
from web3.contract import Contract

ABI_DIR = Path(__file__).resolve().parent / "abi"


class PoolNotFoundError(Exception):
    pass


def _load_abi(name: str) -> list:
    with open(ABI_DIR / name, "r", encoding="utf-8") as fh:
        return json.load(fh)


FACTORY_ABI = _load_abi("uniswap_v3_factory.json")
POOL_ABI = _load_abi("uniswap_v3_pool.json")
ERC20_ABI = _load_abi("erc20.json")


@dataclass(frozen=True)
class PoolState:
    address: str
    token0: str
    token1: str
    fee: int
    liquidity: int
    sqrt_price_x96: int
    tick: int

    def _raw_price_token1_per_token0(self) -> float:
        """Raw (integer-unit) spot price: token1 raw units per token0 raw unit,
        derived from slot0().sqrtPriceX96. NOT decimal-adjusted, NOT executable-size
        aware -- see module docstring and bot/quotes.py.
        """
        return (self.sqrt_price_x96 / (2 ** 96)) ** 2

    def human_price(self, token0_decimals: int, token1_decimals: int) -> float:
        """Human-readable spot price of 1 token0 expressed in token1, adjusted for
        each token's decimals. WARNING: spot price only (zero trade size) -- see
        module docstring. Never use this alone to size or validate a real trade.
        """
        raw = self._raw_price_token1_per_token0()
        return raw * (10 ** (token0_decimals - token1_decimals))

    def human_price_of(self, token_address: str, token0_decimals: int, token1_decimals: int) -> float:
        """Human-readable spot price of `token_address` expressed in the OTHER pool
        token, correctly handling whichever of token0/token1 `token_address` is.
        Case-insensitive address comparison (EVM addresses are case-insensitive).
        """
        addr_lower = token_address.lower()
        price_token0_in_token1 = self.human_price(token0_decimals, token1_decimals)
        if addr_lower == self.token0.lower():
            return price_token0_in_token1
        if addr_lower == self.token1.lower():
            return 1.0 / price_token0_in_token1 if price_token0_in_token1 != 0 else 0.0
        raise ValueError(f"{token_address} is neither token0 ({self.token0}) nor token1 ({self.token1})")


def resolve_pool_address(w3: Web3, factory_address: str, token_a: str, token_b: str, fee: int) -> Optional[str]:
    factory: Contract = w3.eth.contract(address=Web3.to_checksum_address(factory_address), abi=FACTORY_ABI)
    pool_address = factory.functions.getPool(
        Web3.to_checksum_address(token_a), Web3.to_checksum_address(token_b), fee
    ).call()
    if int(pool_address, 16) == 0:
        return None
    return pool_address


def read_pool_state(w3: Web3, pool_address: str) -> PoolState:
    pool: Contract = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=POOL_ABI)
    token0 = pool.functions.token0().call()
    token1 = pool.functions.token1().call()
    fee = pool.functions.fee().call()
    liquidity = pool.functions.liquidity().call()
    slot0 = pool.functions.slot0().call()
    sqrt_price_x96, tick = slot0[0], slot0[1]
    return PoolState(
        address=Web3.to_checksum_address(pool_address),
        token0=token0,
        token1=token1,
        fee=fee,
        liquidity=liquidity,
        sqrt_price_x96=sqrt_price_x96,
        tick=tick,
    )


def find_best_pool(
    w3: Web3,
    factory_address: str,
    token_a: str,
    token_b: str,
    fee_candidates: tuple[int, ...],
    min_liquidity: int = 1,
) -> PoolState:
    """Try each configured fee tier candidate (in order) and return the first pool that
    exists AND reports liquidity strictly greater than `min_liquidity`. Raises
    PoolNotFoundError if none qualify -- this function deliberately never guesses/returns a
    fabricated address.

    `min_liquidity` defaults to 1 (i.e. "non-zero") for backward compatibility, but callers
    performing real trade-size evaluation should pass a meaningfully higher, configured
    threshold (raw on-chain liquidity units, NOT a dollar amount) -- a pool that merely exists
    and has *some* nonzero liquidity can still have far too little depth to execute a given
    trade size without catastrophic price impact. `getPool()` returning a non-zero address is
    NEVER, by itself, sufficient justification for selecting a pool.
    """
    last_error = None
    for fee in fee_candidates:
        try:
            address = resolve_pool_address(w3, factory_address, token_a, token_b, fee)
        except Exception as exc:  # RPC failure for this candidate; try next
            last_error = exc
            continue
        if address is None:
            continue
        state = read_pool_state(w3, address)
        if state.liquidity > min_liquidity:
            return state
    raise PoolNotFoundError(
        f"No pool with liquidity > {min_liquidity} found for {token_a}/{token_b} among fee tiers "
        f"{fee_candidates}" + (f" (last error: {last_error})" if last_error else "")
    )


def get_token_decimals(w3: Web3, token_address: str) -> int:
    token: Contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return int(token.functions.decimals().call())


def get_token_symbol(w3: Web3, token_address: str) -> str:
    token: Contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return token.functions.symbol().call()
