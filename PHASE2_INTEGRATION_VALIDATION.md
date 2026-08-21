# Phase 2 Integration Validation — Arbitrum One

**Repo:** `D:\TitanArb\polygon-triangular-arbitrage`
**Date:** 2026-08-20
**Scope:** Prove the complete Phase 2 chain is genuinely ABI-compatible and integration-sound:
Python bot → `SwapStep[]` → ABI encoding → `FlashArbitrageExecutor` → arbitrage adapters →
Uniswap V3 / Camelot V3 → Aave V3 flash loan → repayment → profit validation. Fix only concrete
integration bugs. No Phase 3, no new DEXes, no architecture changes, no real funds, no mainnet
transactions.

---

## Verdict at a glance

The **core integration contract is sound**: the Python encoding layer, the JSON ABI, the Solidity
`SwapStep`/`executeArbitrage` entry point, and the two adapters all agree, field-for-field and
selector-for-selector. This was verified by reading the actual Solidity, the actual ABI JSON, and
the actual Python — not by trusting unit tests (which use mocks that would mask exactly the kind of
router-ABI bug found below).

Three concrete issues were found and fixed:

| # | Type | Severity | Status |
|---|------|----------|--------|
| BUG #1 | Uniswap router ABI mismatch (config carried SwapRouter02, adapter needs the original SwapRouter) | Latent (would break every Uniswap hop the moment the adapter is wired from config) | **Fixed** |
| BUG #2 | No Phase 2 Arbitrum deploy path (only a legacy Polygon deploy script existed) | Blocking (cannot stand up Phase 2 on a fork) | **Fixed** |
| GAP #3 | No Arbitrum fork integration test; `foundry.toml` had no `arbitrum` RPC endpoint | Blocking (no real integration coverage) | **Fixed** |

**Execution status:** the fork tests and WSS connectivity are authored and ready but **UNVERIFIED
by me** — see "Environment constraint" — because I cannot execute the Windows Foundry toolchain at
`D:\forge`. They must be run per `RUNBOOK_ARBITRUM_FORK.md`. I have not fabricated any run output.

---

## Environment constraint (why execution is UNVERIFIED)

The host is Windows 11. The assistant runs in an isolated Linux sandbox that (a) currently fails to
boot (`VM_DISK_SPACE_INSUFFICIENT`) and (b) even when running cannot execute the Windows-native
Foundry binaries installed at `D:\forge`, nor the Windows Python venv. There is no Windows shell
tool available. Consequently I could perform full **static / source-level** validation (read every
contract, adapter, interface, ABI, and Python module and hand-trace the encode→decode chain) and
author ready-to-run tests, but I could **not** run `forge`, `anvil`, `cast`, or `pytest`.

Everything below is labelled **PROVEN** (established by reading source) or **UNVERIFIED** (requires
you to run the commands in the runbook).

---

## Step 0 deliverable — exact `SwapStep` and how `SwapStep.data` is encoded

Read directly from `contracts/FlashArbitrageExecutor.sol`:

```solidity
struct SwapStep {
    address adapter;          // must be in approvedAdapters
    address tokenIn;
    address tokenOut;
    uint256 amountOutMinimum; // computed off-chain from executable quote - slippage
    bytes   data;             // adapter-specific extra params
}

struct ArbParams {
    SwapStep[] steps;
    uint256    deadline;
    uint256    minProfit;
}

function executeArbitrage(
    address asset,
    uint256 amount,
    SwapStep[] calldata steps,
    uint256 deadline,
    uint256 minProfit
) external onlyAuthorized nonReentrant;
```

**How `data` is encoded (per adapter, read from the adapter source):**

- **Uniswap V3** (`contracts/adapters/UniswapV3Adapter.sol`): `data` **must** be exactly 32 bytes
  and decode to a single `uint24` fee tier: `if (data.length != 32) revert InvalidFeeData();
  uint24 fee = abi.decode(data, (uint24));`. The Python side matches:
  `encode_uniswap_v3_data(fee) -> abi_encode(["uint24"], [fee])` (32 bytes).
- **Camelot V3** (`contracts/adapters/CamelotV3Adapter.sol`): `data` is **ignored** (parameter is
  `bytes calldata /* data */`) because Algebra pools have no fee-tier selection. The Python side
  matches: `encode_camelot_v3_data() -> b""`.

The whole `ArbParams` is passed to Aave as `abi.encode(arbParams)` and recovered in
`executeOperation` via `abi.decode(params, (ArbParams))`.

---

## The validated chain (hand-trace) — PROVEN compatible

1. **Python builds a step.** `bot/executor_arbitrum.py :: SwapStepArg.as_tuple()` produces
   `(checksum(adapter), checksum(tokenIn), checksum(tokenOut), int(amountOutMinimum), data_bytes)`
   — a 5-field tuple in the order `adapter, tokenIn, tokenOut, amountOutMinimum, data`.
2. **ABI JSON.** `bot/abi/flash_arbitrage_executor.json :: executeArbitrage.steps.components` lists
   the tuple fields in **exactly** that order and types (`address, address, address, uint256,
   bytes`), with the outer call being `executeArbitrage(address asset, uint256 amount, tuple[]
   steps, uint256 deadline, uint256 minProfit)`. web3.py encodes strictly from this JSON.
3. **Solidity decodes.** The `SwapStep` struct field order/types match the ABI JSON one-to-one, so
   the calldata web3.py produces is exactly what `executeArbitrage` expects. ✓
4. **Flash loan.** `executeArbitrage` sets an in-flight guard and calls
   `AAVE_POOL.flashLoanSimple(address(this), asset, amount, abi.encode(arbParams), 0)`.
5. **Callback.** `executeOperation` verifies `msg.sender == AAVE_POOL`, `initiator == this`, and the
   in-flight guard; decodes `ArbParams`; loops the steps, doing `forceApprove(adapter, amountIn)`
   then `IArbitrageAdapter(adapter).executeSwap(tokenIn, tokenOut, amountIn, amountOutMinimum,
   deadline, data)`.
6. **Adapter → router.** Each adapter pulls `tokenIn` from the executor, approves its immutable
   router, and performs a single-hop `exactInputSingle`, returning `tokenOut` to the executor.
7. **Repayment + profit.** After the loop, `finalAmount >= amount + premium + minProfit` is
   enforced (`InsufficientProfit` otherwise); the premium is read from Aave (never hard-coded);
   `amount + premium` is approved back to Aave; profit is swept to `profitRecipient`. ✓

The premium is taken from Aave's `premium` argument, hop count is bounded (`MIN_HOPS=2`,
`MAX_HOPS=4`), the route must start and end in `asset`, tokens must be allow-listed, and adapters
must be approved — all verified in `_validateRoute` and `executeOperation`.

**Python runtime layer (also PROVEN sound by reading source):**

- `bot/provider.py` — real HTTP and WebSocket providers, a reconnecting WS wrapper with exponential
  backoff, health checks, and RPC-error classification (including an Arbitrum `SEQUENCER_DOWN`
  category). WSS is genuinely implemented; only live reachability is unverified.
- `bot/gas_arbitrum.py` — reads `ArbGasInfo.getPricesInWei()` and correctly uses indices `[0]`
  (per-L2-tx), `[1]` (per-L1-calldata-unit), `[5]` (per-arb-gas-total), prices L1 calldata in
  16-byte units (ceiling division), and applies a safety margin. Correct L2+L1 model.
- `bot/price_oracle_arbitrum.py` — implements the standard Chainlink L2 sequencer-uptime gate
  (answer==0 ⇒ up, grace-period-elapsed, heartbeat staleness, `answeredInRound >= roundId`) before
  trusting the ETH/USD price. Correct.

---

## Findings and fixes

### BUG #1 — Uniswap adapter must use the ORIGINAL SwapRouter, not SwapRouter02

**Evidence.** `contracts/adapters/UniswapV3Adapter.sol` calls
`ISwapRouter.exactInputSingle(ExactInputSingleParams{tokenIn, tokenOut, fee, recipient, deadline,
amountIn, amountOutMinimum, sqrtPriceLimitX96})` — the **deadline-bearing** struct of the original
Uniswap V3 `SwapRouter` (`0xE592427A0AEce92De3Edee1F18E0157C05861564`). The interface file
`interfaces/ISwapRouter.sol` documents this explicitly and notes that SwapRouter02
(`0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45`) "removed the deadline field, so it is intentionally
NOT used here." The two contracts therefore have **different function selectors** for
`exactInputSingle`.

But `config/arbitrum.json` only carried `uniswapV3.swapRouter02`, and
`bot/config/settings.py` mapped `uniswap_swap_router ← swapRouter02`.

**Why unit tests missed it.** `test/FlashArbitrageExecutor.t.sol` wires the adapter to
`MockSwapRouter`, which *implements the deadline-bearing ABI*. So the mock matches the adapter and
every unit test passes — precisely the scenario the migration rules warned about ("do not assume
unit tests prove ABI compatibility").

**Actual blast radius (stated precisely, not overstated).** In the current Phase 2 code the config
value `uniswap_swap_router` is consumed **only** by `bot/main.py`, which is the **legacy Polygon
entry point** (its own header says it is "no longer executable end-to-end" and it imports
`config_legacy_polygon`). The live Phase 2 executor (`bot/executor_arbitrum.py`) builds steps from
the **adapter** address (env `UNISWAP_V3_ADAPTER`), and the on-chain adapter holds its router
immutably from construction. So this is a **latent trap**, not a live break today: the moment any
deploy script or future pipeline wires the adapter from `swapRouter02`, every Uniswap hop reverts.

**Fix (minimal, no adapter rewrite):**
- `config/arbitrum.json`: added `uniswapV3.swapRouter = 0xE592427A0AEce92De3Edee1F18E0157C05861564`
  (the original router) alongside a note; kept `swapRouter02` for reference.
- `bot/config/settings.py`: `uniswap_swap_router` now resolves to `uniswapV3.swapRouter`, falling
  back to `swapRouter02` only for pre-existing fixtures. This makes the field semantically correct
  (the router the adapter actually calls).
- The new deploy script and fork test both wire `UniswapV3Adapter` to the original router, so the
  fix is exercised end-to-end.

### BUG #2 — No Phase 2 Arbitrum deploy path

**Evidence.** `script/Deploy.s.sol` deploys the legacy single-DEX `FlashTriangularArbitrage`, calls
`setAllowedRouter`, references `TOKEN_WMATIC` and `$POLYGON_RPC_URL`. There was **no** script to
deploy the Phase 2 `FlashArbitrageExecutor` + `UniswapV3Adapter` + `CamelotV3Adapter` and wire them
(`setApprovedAdapter`, `setAllowedToken`). Without it, Phase 2 cannot be stood up on a fork.

**Fix.** Added `script/DeployArbitrum.s.sol` (the legacy script is left untouched). It reads all
addresses from env, deploys the executor and both adapters, **wires the Uniswap adapter to the
original SwapRouter** (with a prominent CRITICAL comment), approves both adapters, allow-lists the
tokens, and logs the resulting addresses for the operator's `.env`
(`FLASH_EXECUTOR_ADDRESS`, `UNISWAP_V3_ADAPTER`, `CAMELOT_V3_ADAPTER`).

### GAP #3 — No Arbitrum fork integration test

**Evidence.** The only fork test, `test/Integration.t.sol`, targets **Polygon** and the **legacy**
`FlashTriangularArbitrage` (with the old `SwapStep(router, tokenIn, tokenOut, fee, minOut)` shape,
the original router `0xE592…1564`, and Polygon USDC/WMATIC/USDT). It never touches the Phase 2
executor or adapters. `foundry.toml`'s `[rpc_endpoints]` defined only `polygon`.

**Fix.**
- `foundry.toml`: added `arbitrum = "${ARBITRUM_RPC_URL}"`.
- Added `test/ArbitrumFork.t.sol` — a real Arbitrum One fork test built on the same honest pattern
  as `Integration.t.sol` (self-skips without an RPC; never fakes a pass), covering:
  - **Deterministic ABI proofs** — direct `executeSwap` calls through each adapter against the REAL
    routers, asserting non-zero output. These are the decisive guard for BUG #1: if the Uniswap
    adapter were wired to SwapRouter02, the deadline-bearing selector would not exist and the swap
    would revert.
  - **Pool existence / liquidity** sanity checks (Uniswap factory `getPool`, Algebra
    `poolByPair`).
  - **Full-chain mechanics** — `executeArbitrage` for a Uniswap-only 2-hop and a cross-DEX
    (Uniswap+Camelot) 2-hop, exercising `flashLoanSimple → executeOperation → adapters → real
    routers → atomic repayment`. Uses `amountOutMinimum = 1` for mechanics only and asserts honest
    invariants for both outcomes (success ⇒ no residual; revert ⇒ atomic rollback leaves
    `profitRecipient` unchanged). It does **not** fabricate profitability on unmodified market
    state.

---

## Files changed / added

| File | Change |
|------|--------|
| `config/arbitrum.json` | **Edited** — added `uniswapV3.swapRouter` (original router) + explanatory note; kept `swapRouter02`. |
| `bot/config/settings.py` | **Edited** — `uniswap_swap_router` now resolves to `swapRouter` (fallback `swapRouter02`), with a comment explaining the ABI reason. |
| `foundry.toml` | **Edited** — added `arbitrum = "${ARBITRUM_RPC_URL}"` to `[rpc_endpoints]`. |
| `script/DeployArbitrum.s.sol` | **Added** — Phase 2 Arbitrum deploy (executor + both adapters + wiring). |
| `test/ArbitrumFork.t.sol` | **Added** — real Arbitrum One fork integration test. |
| `RUNBOOK_ARBITRUM_FORK.md` | **Added** — exact commands (using `D:\forge`) to run tests / deploy. |

No contract logic, no executor logic, no adapters, no architecture were modified. No DEX was added.

---

## Proven vs. Unverified

**PROVEN (by source reading / hand-trace):**
- Python `SwapStepArg` ↔ `flash_arbitrage_executor.json` ↔ Solidity `SwapStep`/`executeArbitrage`
  field-and-type compatibility.
- Uniswap `data = abi.encode(uint24 fee)` (32 bytes) ↔ adapter `abi.decode(data,(uint24))`;
  Camelot `data = b""` ↔ adapter ignores `data`.
- Adapter ABI intent: Uniswap adapter targets the original deadline-bearing SwapRouter; Camelot
  adapter targets the Algebra router (no fee, `limitSqrtPrice`).
- Aave premium sourced from the callback, hop bounds, route/token/adapter validation, profit sweep.
- Gas (ArbGasInfo) and oracle (sequencer-uptime gate) logic; WSS provider implementation.

**UNVERIFIED (requires running `RUNBOOK_ARBITRUM_FORK.md`):**
- On-chain execution of the fork tests (adapter direct swaps, pool liquidity at a given block, full
  flash-loan mechanics). *I did not run these and have not fabricated results.*
- Live WSS reachability of `wss://arb1.arbitrum.io/ws`.
- The exact on-chain shape of the deployed Camelot/Algebra router vs. the repo's `IAlgebraSwapRouter`
  interface — the cross-DEX fork test is what confirms this on-chain.

---

## How to complete verification

Follow `RUNBOOK_ARBITRUM_FORK.md`. In short, from the repo root with `D:\forge` on PATH and an
Arbitrum One RPC exported as `ARBITRUM_RPC_URL`:

```
forge test --match-path test/ArbitrumFork.t.sol --fork-url arbitrum -vvv
```

A green `test_fork_uniswapAdapterDirectSwap_provesOriginalRouterAbi` is the single most important
signal — it confirms the adapter ↔ real-router ABI (BUG #1) on live Arbitrum state.

---

## Migration-rule compliance

No Phase 3 work; no graph engine; no new DEXes; no SushiSwap; no core-architecture changes; no real
funds; no mainnet transactions (fork/simulation only, and the runbook explicitly warns against
broadcasting during validation); Phase 1 contract and adapters were read before any test was
written; unit tests were explicitly **not** treated as ABI proof (the mock-masking is called out);
the dead sandbox was diagnosed and worked around with static validation; blocked web lookups fell
back to in-repo evidence; and all changes are limited to config/test/deploy fixes for the concrete
issues found.
