# Polygon Triangular Arbitrage: Aave V3 Flash Loan + Uniswap V3

Production-oriented (not toy) triangular arbitrage system: Solidity `0.8.19` flash-loan
contract (Aave V3 `flashLoanSimple`) executing three Uniswap V3 swaps
(`USDC -> WMATIC -> USDT -> USDC`), plus a Python 3 / `web3.py` bot that monitors pools over
WSS, computes executable-quote-based profitability, searches for an approximately optimal
loan size, and executes (or dry-runs) the trade.

> **Read section "L. Security Review" and "M. Known Limitations" before using this with real
> funds.** This system is designed to detect and attempt arbitrage; it does **not** guarantee
> profitability.

---

## A. Architecture

```text
                    ┌────────────────────────┐
                    │ Python Arbitrage Bot    │
                    │ bot/main.py (asyncio)   │
                    │  - WSS Swap-event watch │
                    │  - pools.py (discovery) │
                    │  - quotes.py (QuoterV2) │
                    │  - optimizer.py (search)│
                    │  - arbitrage.py (P&L)   │
                    │  - gas.py               │
                    │  - executor.py (sim+tx) │
                    └───────────┬─────────────┘
                                │ eth_call simulate, then send
                                ▼
                    ┌────────────────────────┐
                    │ FlashTriangularArbitrage│
                    │ .sol (owner/executor)   │
                    └───────────┬─────────────┘
                                │ Pool.flashLoanSimple
                                ▼
                    ┌────────────────────────┐
                    │ Aave V3 Pool            │
                    │ executeOperation() cb   │
                    └───────────┬─────────────┘
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                 ▼
        USDC → WMATIC     WMATIC → USDT      USDT → USDC
      (Uniswap V3 fee A) (Uniswap V3 fee B) (Uniswap V3 fee C)
              └─────────────────┼─────────────────┘
                                ▼
                  repay amount+premium to Aave
                                │
                                ▼
                    transfer realized profit to profitRecipient
                    (contract retains only unrelated dust, if any)
```

Key design choices vs. the requested layout (`# 39`):
- Added `bot/tests/` (pytest) for pure-logic unit tests (slippage math, optimizer grid,
  config validation) — not present in the original skeleton, added for testability.
- Added `requirements-dev.txt` for the Python test dependency (`pytest`) separate from
  runtime `requirements.txt`.
- Pool *addresses* are **not** stored in `config/polygon.json`; they are resolved on-chain via
  `UniswapV3Factory.getPool()` at runtime (see "F" and "M" below for why).

---

## B. Solidity contract

`contracts/FlashTriangularArbitrage.sol` (see file). Highlights:
- `Ownable` + `ReentrancyGuard` + `SafeERC20` (OpenZeppelin v4.9.x).
- `executeArbitrage(asset, amount, SwapStep[3] steps, deadline)` — amount is always
  caller-supplied, never hardcoded; only `owner` or `authorizedExecutor` may call it
  (`onlyAuthorized`), guarded by `nonReentrant`.
- Route continuity validated **before** requesting the flash loan: `steps[0].tokenIn == asset`,
  `steps[i].tokenOut == steps[i+1].tokenIn`, `steps[2].tokenOut == asset`.
- `executeOperation` verifies `msg.sender == AAVE_POOL`, `initiator == address(this)`, an
  internal `_flashLoanInFlight` flag, deadline, and route continuity again at execution time;
  performs exactly 3 `ISwapRouter.exactInputSingle` calls; computes
  `repayment = amount + premium` (premium is the **on-chain** value Aave passes in, never
  hardcoded); reverts if `finalAmount < repayment`; approves Aave for exactly `repayment` via
  `forceApprove`; emits `ArbitrageExecuted` / `ProfitGenerated`.
- `withdrawToken` / `withdrawNative`, both `onlyOwner`.
- Router and both tokens of every leg must be explicitly allow-listed
  (`setAllowedRouter` / `setAllowedToken`, `onlyOwner`) — arbitrary third-party swaps/tokens are
  rejected (`InvalidRouter` / `InvalidAsset`).
- Custom errors for all rejection paths (`ZeroAddress`, `ZeroAmount`, `Unauthorized`,
  `InvalidInitiator`, `InvalidCaller`, `InvalidAsset`, `InvalidRouter`, `InvalidRoute`,
  `Expired`, `InsufficientOutput`, `InsufficientRepayment`).
- `tx.origin` is never used.
- **Profit destination**: `address public profitRecipient` is set at construction (constructor
  requires it to be non-zero) and can be updated via `setProfitRecipient` (`onlyOwner`, also
  rejects zero). It is a deliberately separate, explicitly-configured address, never assumed to
  equal `owner()`. After a successful arbitrage, `executeOperation` (1) approves Aave for exactly
  `amount + premium`, THEN (2) transfers the realized `profit` (if any) to `profitRecipient` via
  `safeTransfer`, emitting `ProfitTransferred(token, recipient, amount)`. Because the approval is
  fixed to exactly the repayment amount before the profit transfer reduces the contract's actual
  balance, and Aave's `transferFrom` pull happens only after `executeOperation` returns, the
  contract's remaining balance at the moment Aave pulls funds is always exactly `amount +
  premium` — the profit transfer can never starve the repayment. This is proven by
  `test/FlashTriangularArbitrage.t.sol::test_criticalAccounting_aaveAndProfitRecipientAndContractBalances`,
  which asserts Aave receives exactly `amount + premium`, `profitRecipient` receives exactly
  `profit`, and the contract's own post-execution balance is `0`. On revert (e.g.
  `InsufficientRepayment`), the entire transaction — including any profit transfer — rolls back
  atomically, so `profitRecipient` provably receives `0` in that case
  (`test/Integration.t.sol` asserts this on both real-market-revert paths).

Interfaces (`interfaces/`): `IAavePool.sol` (`IPool`, `IPoolAddressesProvider`,
`IFlashLoanSimpleReceiver`), `ISwapRouter.sol`, `IQuoterV2.sol`, `IUniswapV3Pool.sol` (+
`IUniswapV3Factory`).

---

## C. Python bot

```text
bot/
  main.py       - asyncio entry point, WSS Swap-event subscription + reconnect, orchestration
  config.py     - env + config/polygon.json loading, dataclasses, validation
  pools.py      - on-chain pool discovery (factory.getPool), token0/token1-safe price reading
  quotes.py     - QuoterV2 executable quotes, slippage math (integer, floor-rounded)
  arbitrage.py  - per-leg quote chaining, gross/net profit computation
  optimizer.py  - grid + local-refinement search for approximately optimal loan size
  executor.py   - nonce management, eth_call simulation, signing, sending, execution lock
  gas.py        - eth_estimateGas + EIP-1559 fee estimation, native/USD cost
  logger.py     - structured JSON logging with secret redaction
  abi/          - minimal ABIs for ERC20, Aave Pool, Uniswap V3 Factory/Pool/QuoterV2, contract
  tests/        - pytest unit tests for pure logic (no network required)
```

No floating point is used for on-chain amounts anywhere in the bot: all quote/profit/slippage
math is done in Python `int` (base units). `Decimal` is used only for human-facing figures
(USD estimates, gas cost in native token).

---

## D. Tests

### Solidity (Foundry)
- `test/FlashTriangularArbitrage.t.sol` — **unit tests** against `test/mocks/` (MockAavePool,
  MockSwapRouter, MockERC20): access control, zero amount, expired deadline, disallowed asset,
  invalid router, broken route continuity, route not returning to borrowed asset, caller !=
  Aave pool, slippage failure, insufficient repayment, **successful arbitrage with exact profit
  assertion**, event emission, `withdrawToken` access control, reentrancy-guard sanity, executor
  admin function access control.
- `test/Integration.t.sol` — **mainnet-fork integration test** against the *real* deployed Aave
  V3 Pool, Uniswap V3 Factory, and (if resolvable) real USDC/WMATIC/USDT pools. No mocks. See
  "H" below for exact fork commands. This test explicitly does **not** assert profitability (see
  its NatSpec) — it proves the flash-loan + 3-swap + repayment mechanics work against live
  contracts, logging the outcome either way via `try/catch`.

### Python (pytest)
- `bot/tests/test_quotes.py` — slippage-minimum integer math (including floor-rounding
  direction), optimizer grid-candidate generation.
- `bot/tests/test_config.py` — env/config validation (dry-run defaults, required fields when
  live, loan-range/slippage sanity checks).

**Foundry vs. Hardhat**: Foundry is used because (1) mainnet forking is a first-class,
single-flag (`--fork-url`) feature with fast Rust-based EVM execution, (2) tests are written in
Solidity itself, so no ABI-encoding boilerplate is needed to call `executeArbitrage`/inspect
custom errors, and (3) `vm.envAddress`/`vm.envUint` make environment-driven, no-hardcoded-address
deployment scripts (`script/Deploy.s.sol`) straightforward.

---

## E. `.env.example`

See `.env.example` in the repo root. Never commit a populated `.env`; `.gitignore` already
excludes it.

---

## F. Polygon mainnet contract-address configuration

| Contract | Address | Source |
|---|---|---|
| Aave V3 PoolAddressesProvider | `0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb` | aave-address-book / Aave docs |
| Aave V3 Pool | `0x794a61358D6845594F94dc1DB02A252b5b4814aD` | aave-address-book / Aave docs |
| Aave V3 ProtocolDataProvider | `0x243Aa95cAC2a25651eda86e80bEe66114413c43b` | aave-address-book |
| Uniswap V3 Factory | `0x1F98431c8aD98523631AE4a59f267346ea31F984` | developers.uniswap.org deployments |
| Uniswap V3 SwapRouter (v1, has `deadline`) | `0xE592427A0AEce92De3Edee1F18E0157C05861564` | developers.uniswap.org deployments |
| Uniswap V3 QuoterV2 | `0x61fFE014bA17989E743c5F6cB21bF9697530B21e` | developers.uniswap.org deployments |
| USDC (native, Circle) | `0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359` | Circle / Polygon docs, 6 decimals |
| USDC.e (bridged, legacy) | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` | PolygonScan, 6 decimals |
| USDT | `0xc2132D05D31c914a87C6611C10748AEb04B58e8F` | PolygonScan, 6 decimals |
| WMATIC (aka WPOL) | `0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270` | Uniswap deployment docs, 18 decimals |

**USDC vs. USDC.e**: native USDC is Circle's current officially-recommended token on Polygon,
but USDC.e has historically carried deeper Uniswap V3 liquidity. Both are pre-configured in
`config/polygon.json`; the default route uses native USDC per current official guidance. Switch
to USDC.e (`USDC_E_BRIDGED_ALTERNATIVE`) if you verify it has materially better liquidity for
your target trade size at the time you deploy — **verify this yourself before going live**.

**Uniswap V3 pool addresses for the exact USDC/WMATIC/USDT trio are intentionally NOT hardcoded
anywhere in this repo.** I could not fully verify, from documentation alone, that a specific
hardcoded pool address currently holds meaningful, stable liquidity — liquidity shifts over
time and hardcoding a stale address is worse than resolving it live. Instead:
- `bot/pools.py::find_best_pool` calls `IUniswapV3Factory.getPool(tokenA, tokenB, fee)` for each
  configured fee-tier candidate (`config/polygon.json` → `route.legs[].feeTierCandidatesBps`)
  and only accepts a pool that both **exists** and reports **non-zero on-chain liquidity**.
- `test/Integration.t.sol::test_fork_poolsExistWithLiquidity` does the same on a live mainnet
  fork and logs the resolved addresses + liquidity for manual inspection.
- If no configured fee tier resolves to a liquid pool for a given pair, the bot/tests raise a
  clear `PoolNotFoundError` / assertion failure rather than silently guessing an address.

Fee tiers configured as *candidates* (bot tries each until one has liquidity), not assumptions:
`USDC/WMATIC`: 0.05%, 0.30% · `WMATIC/USDT`: 0.30%, 0.05% · `USDT/USDC`: 0.05%, 0.01%. **You must
verify actual current fee-tier liquidity yourself** (e.g. via the integration test or
Uniswap's app) before relying on any specific tier for real trade sizes.

### Native USDC vs. USDC.e — explicit statement

This system uses **exactly one** USDC representation throughout, consistently:

- **Native USDC** (`0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359`, 6 decimals) — used as the
  Aave `flashLoanSimple` asset, the first/last leg's token, the contract's allow-listed asset
  (`setAllowedToken`), and the address the Python bot loads from `config/polygon.json`'s
  `tokens.USDC` entry.
- **USDC.e** (`0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`, 6 decimals) exists in
  `config/polygon.json` only as a clearly-labeled, inert alternative
  (`USDC_E_BRIDGED_ALTERNATIVE`) — it is **never** read by `bot/config.py::load_settings`
  (which only iterates the canonical `tokens` map keyed by symbol `USDC`/`WMATIC`/`USDT`), never
  passed to `setAllowedToken` in `script/Deploy.s.sol`, and never referenced by
  `contracts/FlashTriangularArbitrage.sol` or any test.
- **Consequence of mixing them (why this matters)**: native USDC and USDC.e are *different ERC-20
  contracts* with independent balances and independent Uniswap V3 pools. If a deployment
  accidentally allow-listed USDC.e while the bot/tests quoted and flash-borrowed native USDC (or
  vice versa), pool discovery (`factory.getPool`) would resolve to a *different, likely far less
  liquid* pool than intended, quotes would be computed against the wrong pool, and
  `executeArbitrage`'s `allowedTokens` check would simply reject the mismatched address outright
  (fail-safe) rather than silently trading the wrong asset. This is why every allow-list check in
  the contract (`InvalidAsset`) and every token lookup in the bot (`tokens_by_symbol["USDC"]`)
  resolve through a single, named configuration entry rather than a bare hardcoded address
  repeated in multiple places.
- If you intentionally want to switch to USDC.e (e.g. because you've verified it has better
  liquidity for your trade size at deploy time), do so by changing `config/polygon.json`'s
  `tokens.USDC.address` to the USDC.e address (or renaming the alternative entry) — do **not**
  add USDC.e as a second, separate allow-listed token alongside native USDC, since the contract
  has no concept of "equivalent" tokens and would treat them as two unrelated assets.

---

## G. Installation

### Solidity / Foundry
```powershell
# Windows: no official `foundryup` shell one-liner; download the latest win32_amd64 release
# zip from https://github.com/foundry-rs/foundry/releases and add forge.exe/cast.exe/anvil.exe
# to PATH. On macOS/Linux: curl -L https://foundry.paradigm.xyz | bash && foundryup

cd polygon-triangular-arbitrage
forge install OpenZeppelin/openzeppelin-contracts@v4.9.3
forge install foundry-rs/forge-std
forge build
```

### Python bot
```powershell
cd polygon-triangular-arbitrage
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
copy .env.example .env
# edit .env with real values
```

---

## H. Mainnet-fork test commands

```powershell
# Unit tests only (no network required)
forge test --match-path test/FlashTriangularArbitrage.t.sol -vvv

# Mainnet-fork integration tests (requires a Polygon RPC URL, e.g. Alchemy/Infura/QuickNode)
$env:POLYGON_RPC_URL = "https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"
forge test --match-path test/Integration.t.sol --fork-url $env:POLYGON_RPC_URL -vvv

# Everything
forge test --fork-url $env:POLYGON_RPC_URL -vvv
```

Python pure-logic tests:
```powershell
pytest bot/tests -v
```

---

## I. Deployment commands

`script/Deploy.s.sol` reads **everything** from environment variables — nothing is hardcoded in
the script itself.

```powershell
$env:PRIVATE_KEY = "0x..."                # deployer key, never commit
$env:AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
$env:EXECUTOR = "0xYourBotWalletAddress"
$env:UNISWAP_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
$env:TOKEN_USDC = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
$env:TOKEN_WMATIC = "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270"
$env:TOKEN_USDT = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"

forge script script/Deploy.s.sol:Deploy --rpc-url https://polygon-rpc.com --broadcast -vvvv
```

Post-deployment checklist (do these **in order**, do not skip):
1. Verify the deployed address on PolygonScan and confirm bytecode matches the compiled artifact.
2. Confirm `owner()` is the intended admin wallet (e.g. a multisig for production).
3. Confirm `authorizedExecutor()` is the bot's dedicated hot wallet (**not** your main wallet).
4. Call `setAllowedRouter` for the Uniswap SwapRouter (done by `Deploy.s.sol` already; re-verify).
5. Call `setAllowedToken` for USDC/WMATIC/USDT (done by `Deploy.s.sol` already; re-verify).
6. Confirm the contract's native/token balances are zero (nothing accidentally sent yet).
7. Set `CONTRACT_ADDRESS` in `.env`, keep `DRY_RUN=true`, run the bot and confirm it logs
   correct opportunity evaluations without sending anything.
8. Only after a successful **small** live test (smallest feasible loan amount, during a period
   you are actively monitoring), consider enabling larger production execution.

---

## J. Dry-run commands

```powershell
# .env: DRY_RUN=true  (this is also the hardcoded default even if unset)
python -m bot.main
```
In dry-run mode the bot performs the full pipeline (pool discovery → quotes → optimization →
`eth_call` simulation) and logs the would-be transaction, but the send step in
`executor.py::execute` is unconditionally skipped whenever `settings.dry_run` is true — there is
no code path that sends a transaction while this flag is true.

---

## K. Live execution commands

```powershell
# .env: DRY_RUN=false, PRIVATE_KEY and CONTRACT_ADDRESS set
python -m bot.main
```
Only do this after completing the section "I" checklist and reviewing "L"/"M"/"N"/"O" below.

---

## L. Security review

- **Access control**: `executeArbitrage` restricted to `owner`/`authorizedExecutor`
  (`onlyAuthorized`); admin setters restricted to `owner` (`onlyOwner`). No `tx.origin` usage.
- **Reentrancy**: `nonReentrant` on `executeArbitrage`, `withdrawToken`, `withdrawNative`.
  `executeOperation` is intentionally *not* separately guarded by the same modifier (it executes
  synchronously inside the already-guarded `executeArbitrage` call stack; double-guarding with
  the same lock would deadlock).
- **Untrusted external calls**: swap router and both tokens of each leg must be pre-allow-listed
  by the owner; the Aave Pool address is immutable, set once at construction.
- **Approvals**: `forceApprove` (OpenZeppelin `SafeERC20`) is used, which safely handles tokens
  requiring a zero-allowance reset before a new non-zero approval (e.g. USDT-style tokens),
  and approves exactly the amount needed per call rather than an unlimited allowance.
- **Repayment correctness**: `repayment = amount + premium` uses the **on-chain** `premium`
  Aave itself supplies as a callback argument — never a hardcoded/assumed fee. Reverts via
  `InsufficientRepayment` if the route did not produce enough output.
- **Fund-lock risk**: `withdrawToken`/`withdrawNative` let the owner recover any ERC-20/native
  balance left in the contract (residual profit or accidentally-sent tokens). No code path
  permanently locks funds under normal operation; the only way profit becomes hard to retrieve
  is if `owner` itself is lost/compromised (standard `Ownable` risk — use a multisig in
  production).
- **Private-key handling**: the bot loads the key once via `Account.from_key` from an
  environment variable; `logger.py` redacts any 32-byte-hex-looking string and known secret
  field names defensively; the raw key/signed-tx bytes are never logged.
- **Nonce handling**: `NonceManager` reads the `pending` transaction count and tracks the
  next nonce in-process, avoiding the classic "always re-read `get_transaction_count(address)`"
  collision bug; an `asyncio.Lock`-based execution lock additionally prevents two arbitrage
  transactions from ever being built/sent concurrently against the same wallet.
- **Simulation-before-send**: `executor.py::simulate` performs `eth_call` + `estimate_gas`
  immediately before any real submission; a failing simulation aborts the send unconditionally.
- **Dry-run guarantee**: see section K.

## M. Known limitations

- **Uniswap V3 pool addresses for the specific USDC/WMATIC/USDT trio are not hardcoded** because
  I could not independently verify current, durable liquidity for specific pool addresses from
  documentation alone (see section F). They are resolved live via the Factory instead. You
  should still manually sanity-check the resolved pools (via
  `test_fork_poolsExistWithLiquidity` or the Uniswap app) before sizing large trades.
- **Native-token USD price** used for gas-cost conversion in `bot/main.py` is approximated from
  the USDC/WMATIC pool's *spot* price as a **last-resort fallback only** (treating USDC ≈ $1).
  The primary source is now Chainlink's independent MATIC/USD Data Feed on Polygon
  (`0xAB594600376Ec9fD91F8e885dADF0CE036862dE0`, `bot/price_oracle.py`), used specifically
  *because* deriving the native-token price from the very pool being arbitraged would be
  circular and would inherit that pool's own liquidity/manipulation risk. The oracle rejects
  stale (`>1h` old) or non-positive prices outright (`PriceOracleError`) rather than silently
  using bad data; only if the oracle call itself fails does the code fall back to the
  pool-spot-price approximation, and that fallback path is logged loudly as DEGRADED. The USDC
  side of the conversion still assumes USDC ≈ $1.00 (a standard approximation for a major
  fiat-backed stablecoin) rather than reading a second Chainlink USDC/USD feed — I could not
  fully verify a specific Polygon USDC/USD feed address within this audit and chose to state
  that rather than guess one.
- **The optimizer's grid + local-refinement search is an approximation**, not a global optimum
  solver; profitability curves that are non-concave (multiple local maxima) could be missed.
  It is also always evaluated against a point-in-time snapshot of pool state — see "O" below.
- **`ArbitrageFailed` event is declared but, by design, never observably emitted on-chain** — see
  the contract's ATOMICITY NOTE: any in-flight failure reverts the whole transaction (and its
  events) because Aave requires atomic repayment. It exists for interface completeness only.
- **Approximate profitability of this specific route is not claimed.** See section N.
- **No MEV-protection infrastructure is wired in by default** — see section O.

## N. Gas and profitability analysis

Gas cost is estimated per-transaction via `eth_estimateGas` (`bot/gas.py`) with a configurable
safety multiplier (`GAS_LIMIT_MULTIPLIER`, default 1.20) and current Polygon EIP-1559 fee-market
pricing (base fee × 2 + priority fee, matching common wallet heuristics). `netProfit =
grossProfit − AaveFee − gasCost` is enforced before any execution attempt (`bot/arbitrage.py`,
`bot/main.py`); the bot **never** triggers execution merely because `grossProfit > 0`.

`MIN_PROFIT_USD` (default `5`) is a starting configuration value, **not a claim that $5 is
sufficient**. A realistic threshold should additionally price in: RPC/infrastructure cost,
the probability-weighted cost of a reverted-but-still-gas-charged transaction (failed
simulations are free via `eth_call`, but a transaction that reverts *after* being mined still
costs gas), competition from other bots/MEV searchers for the same opportunity, and price
movement between quote time and mining time. Size this threshold to your actual capital,
infrastructure, and risk tolerance — do not treat the default as investment advice.

**Do not assume this system will be profitable.** Real-world profitability depends on
liquidity, competing searchers, gas markets, MEV, network/RPC latency, price movement between
detection and settlement, execution ordering, current pool state, and capital efficiency — all
outside this codebase's control.

**Empirical evidence from this audit**: against a real Polygon mainnet fork, a 1,000 USDC
`USDC→WMATIC→USDT→USDC` route reverted with `InsufficientRepayment(1000500000, 969533830)` (a
~3% shortfall, dominated by price impact in the thin WMATIC/USDT pool at that block). After
seeding ~10,000x additional REAL liquidity into all three pools (via genuine `pool.mint()`
calls, `test/Integration.t.sol::test_fork_controlledSuccessfulArbitrage_withSeededLiquidity`),
the shortfall shrank to `InsufficientRepayment(1000500000, 997155002)` (~0.35%) but the route
*still* reverted — once price impact was removed, the residual loss was simply the combined
0.40% Uniswap fees (0.05%+0.30%+0.05%) plus 0.05% Aave premium, with no genuine cross-pool
mispricing left to capture. This is exactly the expected behavior of a reasonably efficient,
fee-competed market, and is direct evidence supporting the point above: this specific
route/pool-set was not profitable at the tested block, at either its natural liquidity or an
artificially deepened one.

## O. MEV / execution-risk analysis

Risks: price changes between quote and mining, mempool competition (other searchers targeting
the same triangular path), sandwich attacks against the arbitrage tx itself, stale quotes,
nonce conflicts (mitigated by `NonceManager`, not eliminated), and chain reorgs.

Mitigations implemented: event-driven (not blind-polling) monitoring to reduce reaction latency;
fresh QuoterV2 quotes recomputed at trigger time (not cached spot prices); a final `eth_call`
simulation immediately before send; a per-leg `amountOutMinimum` and a short, configurable
`TX_DEADLINE_SECONDS` enforced on-chain; an execution lock preventing overlapping submissions.

**WebSocket monitoring does not guarantee profitable execution.** Between "detect" and "mined",
pool state can change (see section "Known Limitations" / task requirement #42): a competing
transaction can land first, be included earlier in the same block by the block builder, or the
underlying prices can move. This system does not currently integrate a private-transaction /
MEV-protection relay (e.g. a Polygon-compatible private RPC or bundle relay). Whether to add one
is an architectural trade-off: it can reduce (not eliminate) sandwich/front-run risk and failed
public-mempool transactions, at the cost of additional infrastructure dependency, potential
inclusion latency/guarantees that vary by provider, and (for some relays) a fee or profit-share.
For a production deployment competing against other searchers, integrating such a relay is
worth evaluating — it is not implemented here so as not to hardcode a specific third-party
relay endpoint without your explicit choice of provider.

---

## Prerequisites

- Foundry (`forge`, `cast`) for contracts/tests.
- Python 3.11+.
- A Polygon mainnet RPC provider supporting both HTTPS and WSS (Alchemy, Infura, QuickNode, etc).
- A dedicated hot wallet (funded only with what you're willing to risk) for `authorizedExecutor`.

## Polygon RPC setup

Use a paid/reputable RPC provider for both `HTTP_RPC_URL` and `WSS_RPC_URL` — public free RPCs
are typically rate-limited and/or high-latency, both of which materially increase the "stale
opportunity" risk described in section O.

## Bot configuration

All runtime parameters are in `.env` (see `.env.example`) plus `config/polygon.json` for
chain/token/route configuration (addresses, decimals, fee-tier candidates).

## Monitoring

The bot emits structured JSON logs (`bot/logger.py`) to stdout including: timestamp, pool,
token pair, amount, quoted output, fee, gas estimate/price, gross profit, Aave fee, gas cost,
net profit, and transaction hash when applicable. Pipe stdout to your log aggregator of choice.

## Failure scenarios

Handled explicitly:
- **Quote failure** (`QuoteError`): that candidate loan amount is skipped; the optimizer
  continues with other candidates; a fully-failed cycle logs and returns without crashing the
  process.
- **RPC timeout / WSS disconnect**: `watch_pools_forever` catches connection errors and
  reconnects with exponential backoff (1s → 60s cap), then resubscribes.
- **Simulation failure**: transaction is never sent (`SimulationFailedError` path).
- **Broadcast failure**: nonce is released back for reuse (`NonceManager.release_on_failure`).
- **Reverted/failed mined transaction, insufficient liquidity, slippage failure, Aave/Uniswap
  callback failure**: all surface as an `eth_call` revert during simulation (caught) or, if a
  transaction is mined with `status == 0`, is logged with the receipt for investigation; the
  bot does not crash and continues its next cycle.
- **Overlapping opportunities**: `ExecutionLockedError` / `_execution_lock` prevents concurrent
  sends against the same wallet.

## Emergency shutdown

Stop the Python process (it holds no persistent on-chain state that requires a graceful
shutdown sequence). On-chain, the owner can immediately neutralize the contract by calling
`setAllowedRouter(router, false)` and/or `setAllowedToken(token, false)` for all configured
tokens, or by revoking `authorizedExecutor` (`setAuthorizedExecutor` to a burn/zero-equivalent
address is rejected by the zero-address check — set it to the `owner` address itself, which can
then simply choose not to call `executeArbitrage`).

## Withdrawing profits

Owner calls `withdrawToken(tokenAddress, amount)` or `withdrawNative(amount)` — both `onlyOwner`,
both emit a `Withdrawn` event.

---

## Critical self-review

- **Compiles with 0.8.19?** Yes — uses only 0.8.19-compatible syntax (custom errors, no
  transient storage, standard OZ v4.9.x imports). Run `forge build` to confirm in your
  environment (network access required to `forge install` dependencies first).
- **Aave V3 callback signature correct?** Yes: `executeOperation(address,uint256,uint256,address,bytes) returns (bool)`, matching `IFlashLoanSimpleReceiver`.
- **Flash-loan repayment correct?** Yes: `repayment = amount + premium`; `forceApprove` for exactly that amount; Aave's `flashLoanSimple` pulls it after `executeOperation` returns `true`.
- **Aave premium handled correctly?** Yes: `premium` is the callback argument, never hardcoded; `AAVE_FEE_BPS` in `.env` is explicitly documented as an off-chain estimation default only.
- **All three swaps executed?** Yes: `for (uint256 i = 0; i < 3; i++)` over `arbParams.steps`.
- **Route ends in borrowed asset?** Enforced both pre-flight (`_validateRoute`) and at execution time (`if (currentToken != asset) revert InvalidRoute();`).
- **Decimals handled correctly?** Solidity operates purely on integer base units throughout (no on-chain decimal conversion needed); the bot reads `decimals()` where needed and never uses float for amounts.
- **token0/token1 orientation handled?** `bot/pools.py` always reads `token0()`/`token1()` from the pool and derives price via `human_price_of`, never assuming which side USDC is on.
- **Fee tiers correct?** Configurable candidates per leg, resolved via `factory.getPool`; no fee tier is assumed globally.
- **Price impact considered?** Yes — via Uniswap `QuoterV2` executable quotes (`bot/quotes.py`), not spot price products.
- **Slippage enforced?** Yes — `amountOutMinimum` computed off-chain (floor-rounded) and enforced on-chain per swap (`InsufficientOutput`).
- **Gas cost included?** Yes — in both the optimizer's net-profit filter and the executor's real pre-send gas estimate.
- **Flash-loan fee included?** Yes — in `evaluate_route_for_amount`.
- **Loan size actually optimized?** Yes, approximately — grid + local refinement (`bot/optimizer.py`), explicitly documented as an approximation.
- **Transaction simulation performed?** Yes — `eth_call` + `estimate_gas` immediately before any send.
- **Can a stale opportunity cause a dangerous transaction?** Mitigated (fresh quotes + simulation + short deadline) but **not fully eliminated** — see section O; this is a fundamental limitation of any non-atomic-quote system, explicitly disclosed.
- **Can an unauthorized account trigger the contract?** No — `onlyAuthorized` on `executeArbitrage`; `executeOperation` gated on `msg.sender == AAVE_POOL` and `initiator == address(this)`.
- **Can funds become permanently stuck?** Not under normal operation — `withdrawToken`/`withdrawNative` recover any balance; only an `owner`-key-loss scenario would strand funds (standard `Ownable` risk).
- **Approvals safe?** `forceApprove` used throughout (handles non-standard tokens like USDT that require zero-then-set approval).
- **Private keys protected?** Loaded only from env var, never logged (`logger.py` redaction), never hardcoded, `.env` gitignored.
- **Nonce races handled?** `NonceManager` + `pending` count + execution lock; not a 100% guarantee under all pathological RPC-provider behaviors, disclosed as such.
- **Can WSS recover?** Yes — exponential-backoff reconnect + resubscribe loop; a periodic fallback loop provides defense-in-depth against missed events.
- **Does dry-run guarantee no tx sent?** Yes — the send branch in `executor.py::execute` is unconditionally skipped when `settings.dry_run` is true; there is no other code path to `send_raw_transaction`.
- **Do fork tests exercise real Aave/Uniswap?** Yes — `test/Integration.t.sol` uses the real mainnet Aave V3 `Pool` and Uniswap V3 `Factory`/`SwapRouter` addresses against a fork (no mocks in that file); pool addresses are resolved live via the real factory, exactly as the bot does.

**Explicitly uncertain / not independently verifiable by me in this environment:** the *current*
depth and stability of liquidity in the exact USDC/WMATIC, WMATIC/USDT, and USDT/USDC Uniswap V3
pools at whatever fee tier ends up resolving on a given day — this changes over time and must be
checked at deployment time using `test_fork_poolsExistWithLiquidity` or equivalent, not assumed
from this document.
