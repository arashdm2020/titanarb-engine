```text
████████╗██╗████████╗ █████╗ ███╗   ██╗ █████╗ ██████╗ ██████╗
╚══██╔══╝██║╚══██╔══╝██╔══██╗████╗  ██║██╔══██╗██╔══██╗██╔══██╗
   ██║   ██║   ██║   ███████║██╔██╗ ██║███████║██████╔╝██████╔╝
   ██║   ██║   ██║   ██╔══██║██║╚██╗██║██╔══██║██╔══██╗██╔══██╗
   ██║   ██║   ██║   ██║  ██║██║ ╚████║██║  ██║██║  ██║██║  ██║
   ╚═╝   ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝

                 ARBITRUM ONE ARBITRAGE ENGINE
```

# TitanArb

**A production-grade, live-capable DeFi arbitrage engine for Arbitrum One.**

![Arbitrum](https://img.shields.io/badge/Network-Arbitrum%20One-2D374B?style=flat-square)
![Go](https://img.shields.io/badge/Runtime-Go-00ADD8?style=flat-square)
![Aave](https://img.shields.io/badge/Flash%20Liquidity-Aave%20V3-B6509E?style=flat-square)
![Uniswap](https://img.shields.io/badge/DEX-Uniswap%20V3-FF007A?style=flat-square)
![Camelot](https://img.shields.io/badge/DEX-Camelot%20V3-E5523E?style=flat-square)
![Execution](https://img.shields.io/badge/Execution-Live%20Capable-2EA44F?style=flat-square)

TitanArb continuously converts new Arbitrum state into a bounded set of executable arbitrage candidates, ranks the best routes, quotes them against real DEX execution paths, optimizes flash-loan size, verifies economics, simulates the transaction, and only then allows a trade to reach the on-chain executor.

This is not a spot-price comparison script and it is not a fixed triangular-arbitrage demo. TitanArb is built around **executable economics**: flash-loan premium, DEX fees, gas, slippage, price impact, liquidity, block freshness, route quality, and repayment all have to survive before a candidate can execute.

> **The goal is not to trade often. The goal is to identify when a trade is actually worth executing.**

---

## Production status

TitanArb contains the production runtime and deployed Arbitrum contracts used by the engine.

- ✅ **Go production runtime**
- ✅ **Arbitrum One deployment**
- ✅ **Aave V3 flash-loan execution path**
- ✅ **Uniswap V3 + Camelot V3 / Algebra adapters**
- ✅ **2-hop, 3-hop, and 4-hop route search**
- ✅ **Live execution preflight + simulation**
- ✅ **Pair-centric market intelligence**
- ✅ **Persistent RouteMemory and pre-quote ranking**
- ✅ **Bounded quote and optimizer budgets**
- ✅ **Incremental dirty-pool processing**
- ✅ **Checkpointed background reconciliation**
- ✅ **Health/latency-aware multi-provider RPC routing**
- ✅ **Single-owner WSS and single-path transaction broadcast**
- ✅ **Structured production telemetry and Telegram operations**

---

## At a glance

| Area | TitanArb |
|---|---|
| Network | Arbitrum One (`chainId 42161`) |
| Runtime | Go |
| Flash liquidity | Aave V3 |
| DEX execution | Uniswap V3, Camelot V3 / Algebra |
| Route depth | 2-hop, 3-hop, 4-hop cycles |
| Market model | Dirty-pool incremental updates + checkpointed reconciliation |
| Search intelligence | PairScore + RouteMemory + exploit/explore/new scheduling |
| Quote model | Executable DEX quotes, bounded globally |
| Optimizer | Adaptive, score-aware loan-size search |
| RPC | Tiered, health-aware, latency-aware, load-aware |
| Execution | Preflight → economics → freshness → simulation → broadcast |
| Safety boundary | Market discovery cannot bypass execution gates |

---

## Why TitanArb exists

A visible price difference between two pools is not automatically an arbitrage opportunity.

A route can look profitable and still fail after accounting for:

- Aave flash-loan premium
- DEX swap fees
- L1 data cost and L2 gas
- slippage
- price impact
- available liquidity
- stale state
- route execution constraints
- repayment requirements

TitanArb treats those costs as part of the opportunity itself, not as an afterthought.

That changes the core question from:

> “Where is the price different?”

into:

> **“Which route, at what loan size, is still executable and profitable after every real cost?”**

---

## Performance

A major part of TitanArb is keeping the hot path small enough to react to Arbitrum state without drowning in its own RPC workload.

A representative production benchmark after the hot-path/RPC refactor:

| Metric | Before | After |
|---|---:|---:|
| Dirty market cycle | ~16.6s | **~2.56s** |
| Quote duration | ~14.2s | **~1.43s** |
| RPC calls / cycle | ~128 | **~15** |
| Fresh quotes / cycle | ~84–90 | **~4** |
| Block lag | ~66 | **~10** |

Full reconciliation was also moved away from a large synchronous refresh. It now progresses through small checkpointed units and yields to latency-sensitive market work instead of blocking the engine with a single large RPC burst.

Production timings depend on market activity and RPC conditions; the important property is that expensive work is **bounded, prioritized, and observable**.

---

## Architecture

```text
                                Arbitrum One
                                     │
                           WSS heads + pool logs
                                     │
                                     ▼
                         ┌─────────────────────┐
                         │ Latest-head scheduler│
                         │ coalesce stale work  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Market Engine       │
                         │ dirty pool refresh  │
                         │ incremental graph   │
                         └──────────┬──────────┘
                                    │
                   ┌────────────────┴────────────────┐
                   ▼                                 ▼
        ┌─────────────────────┐           ┌─────────────────────┐
        │ Pair Intelligence   │           │ Reconciliation      │
        │ volatility / depth  │           │ checkpointed units  │
        │ venue diversity     │           │ background / yielding│
        └──────────┬──────────┘           └─────────────────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │ Route Intelligence  │
        │ PairScore           │
        │ RouteMemory         │
        │ exploit/explore/new │
        └──────────┬──────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │ Bounded Quotes      │
        │ Uni V3 / Camelot V3 │
        └──────────┬──────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │ Opportunity Engine  │
        │ fees + premium      │
        │ gas + impact        │
        │ net profitability   │
        └──────────┬──────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │ Adaptive Optimizer  │
        │ loan-size search    │
        │ bounded samples     │
        └──────────┬──────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │ Execution Pipeline  │
        │ freshness           │
        │ simulation          │
        │ slippage            │
        │ repayment           │
        └──────────┬──────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │ FlashArbitrage      │
        │ Executor            │
        └──────────┬──────────┘
                   │
             Aave V3 callback
                   │
                   ▼
       Uniswap V3 / Camelot V3 swaps
                   │
                   ▼
            repay + realized PnL
```

The detector and executor are intentionally separate systems. Search intelligence decides **where to spend expensive evaluation budget**; it never grants permission to bypass execution safety.

---

## Market intelligence

TitanArb does not rank the market only by token popularity.

The engine maintains pair-level intelligence using signals such as:

- realized volatility across multiple windows
- turnover and swap activity
- executable depth
- venue diversity
- quote success/failure history
- dislocation frequency
- liquidity quality
- recent failures and cooldowns

Pair intelligence feeds route quality while preserving a strict boundary between:

- **market assets** — assets allowed to participate in route discovery
- **execution / loan assets** — assets explicitly approved for flash-loan starts and execution

A dynamically admitted market asset does not silently become an execution asset.

---

## Route intelligence

TitanArb keeps persistent route history so expensive RPC work is not allocated blindly.

Representative `RouteMemory` signals include:

```text
GapEMA
NetProfitEMA
QuoteSuccessEMA
FailureEMA
LastScore
LastSeenBlock
LastDirtyBlock
CrossVenue
Observations
LastLatencyMS
```

The pre-quote scorer is **zero-RPC** and ranks routes before the first expensive quote.

Route scheduling mixes:

```text
70% exploit
20% explore
10% new / under-observed
```

Cross-venue routes receive a positive search prior, while same-DEX routes remain eligible when their economics justify evaluation.

---

## Bounded hot path

TitanArb deliberately limits how much work a single market cycle may trigger.

Production controls include:

- global quote budget
- bounded routes considered per cycle
- bounded optimizer routes
- score-dependent optimizer samples
- persistent quote cache
- state-aware quote deduplication
- dirty-route invalidation
- latest-head coalescing
- stale-work cancellation

The engine sheds low-priority background work before starving simulation, execution, or transaction-critical RPC.

---

## RPC architecture

RPC selection is part of the trading architecture, not just infrastructure plumbing.

Production read priority is:

```text
1. Alchemy
2. Ankr
3. Chainstack
4. QuickNode
5. Arbitrum official RPC — emergency fallback
```

Within provider pools, selection remains health/latency/inflight-aware rather than blind round-robin.

The RPC layer tracks provider characteristics including:

- latency EMA / p95
- inflight requests
- configured capacity
- block freshness
- transport failures
- rate limits / 429s
- cooldown and probation state
- workload/stage usage

A global read limiter keeps aggregate RPC demand bounded; adding providers does not mean multiplying total request volume.

### Request priority

Latency-sensitive work has scheduling priority over background work:

```text
execution / simulation / critical reads
                 ↓
        quotes / hot market reads
                 ↓
 pool refresh / reconciliation / discovery
                 ↓
       pair-intelligence exploration
```

### WSS

WSS is single-owner. Only one active subscription stream drives market scheduling at a time.

### Broadcast

Signed transactions are broadcast through a single execution path rather than blindly sent through every provider.

---

## Opportunity economics

Every evaluated route carries explicit economics instead of a simple spread percentage.

```text
amount_in
amount_out
gross_profit
Aave premium
DEX fees
L1 data fee
L2 gas estimate
price impact
expected net profit
minimum profit threshold
profitability gap
```

Rejected opportunities are not thrown away. Near-miss data feeds telemetry and future route ranking so the engine learns which routes are close enough to deserve attention.

---

## Flash-loan execution

TitanArb uses Aave V3 flash liquidity as part of the execution model.

A candidate reaching the executor has already passed the detector/economics path, but execution still independently verifies:

1. deployed executor/adapters/routers
2. approved assets and route boundaries
3. market-state freshness
4. minimum-profit economics
5. slippage constraints
6. transaction simulation
7. Aave repayment feasibility
8. signer / nonce / broadcast conditions

If repayment or another required condition fails, the atomic transaction cannot complete successfully.

---

## Smart contracts

### FlashArbitrageExecutor

`contracts/FlashArbitrageExecutor.sol` is the generalized Arbitrum flash-arbitrage executor.

Venue-specific swap behavior is delegated to approved adapters:

- `contracts/adapters/UniswapV3Adapter.sol`
- `contracts/adapters/CamelotV3Adapter.sol`

This keeps the executor generic while isolating DEX-specific router semantics.

### Arbitrum deployment

| Component | Address |
|---|---|
| FlashArbitrageExecutor | `0xdc63781E4f880F3911260Ecf0f1208eB32756666` |
| UniswapV3Adapter | `0xD03BC6e7331E726dA03De74b37437F1ACa2dFD95` |
| CamelotV3Adapter | `0x1318D03c876DE4E3686335B2A80412c83C53A99D` |
| Aave V3 Pool | `0x794a61358D6845594F94dc1DB02A252b5b4814aD` |
| Uniswap V3 Router | `0xE592427A0AEce92De3Edee1F18E0157C05861564` |
| Camelot / Algebra Router | `0x1F721E2E82F6676FCE4eA07A5958cF098D339e18` |

Common market assets include WETH, USDC, USDT, ARB, and dynamically admitted market-only assets such as USDC.e.

---

## Repository layout

```text
.
├── go/                         # Production runtime
│   ├── cmd/titanarb/           # Main process
│   ├── internal/
│   │   ├── config/             # Runtime + market configuration
│   │   ├── market/             # Block-driven market engine
│   │   ├── scheduler/          # Latest-head coalescing
│   │   ├── pools/              # Pool discovery / refresh
│   │   ├── routes/             # 2/3/4-hop route graph
│   │   ├── quotes/             # Executable quote plumbing
│   │   ├── opportunity/        # Economics
│   │   ├── nearmiss/           # Near-miss + RouteMemory
│   │   ├── optimizer/          # Loan-size optimizer
│   │   ├── execution/          # Simulation + live execution
│   │   ├── rpc/                # Managed multi-provider RPC
│   │   ├── websocket/          # Managed WSS ownership
│   │   ├── universe/           # Dynamic market universe
│   │   ├── metrics/            # Runtime metrics
│   │   ├── observability/      # Structured telemetry
│   │   └── telegram/           # Operational interface
│   └── pkg/
├── contracts/
│   ├── FlashArbitrageExecutor.sol
│   └── adapters/
│       ├── UniswapV3Adapter.sol
│       └── CamelotV3Adapter.sol
├── config/
├── test/
├── script/
└── RUNBOOK_ARBITRUM_FORK.md
```

---

## Build and validate

The production Go module lives under `go/`.

```bash
cd go

go test ./cmd/titanarb ./internal/...
go vet ./cmd/titanarb ./internal/...
go build -buildvcs=false ./cmd/titanarb
```

For race-sensitive packages:

```bash
go test -race ./internal/rpc ./internal/market ./internal/pools ./internal/execution ./internal/routes
```

Foundry and Arbitrum fork validation are documented in `RUNBOOK_ARBITRUM_FORK.md`.

---

## Runtime observability

TitanArb emits structured telemetry for the market and execution pipelines, including:

- active pools and assets
- route counts by hop
- routes considered / quoted / skipped
- PairScore and pre-quote statistics
- quote-cache effectiveness
- optimizer runs and sample budgets
- near-miss economics and rejection reason
- cycle duration and block lag
- RPC calls by workload stage
- provider latency, health, cooldown, and traffic share
- reconciliation progress
- simulation attempts
- execution preflight
- transaction outcomes

The operational goal is simple: if the engine is slow, skipping work, rejecting a candidate, or changing providers, the reason should be visible in telemetry.

---

## Design principles

1. **Executable profit matters more than visible spread.**
2. **Search intelligence never weakens execution gates.**
3. **Dynamic market admission never silently expands execution authority.**
4. **The hot path is bounded; background work yields first.**
5. **Dirty state should invalidate only affected work whenever possible.**
6. **RPC resilience must not multiply aggregate RPC demand.**
7. **WSS stays single-owner to avoid duplicate market scheduling.**
8. **Transaction broadcast stays single-path.**
9. **Rejected economics are useful data, not wasted work.**
10. **Production performance is measured from telemetry, not assumed.**

---

## Historical migration notes

<details>
<summary>Python / Polygon history</summary>

TitanArb originally began as a Polygon triangular-arbitrage project using Python, Web3.py, and a fixed three-leg design.

The repository retains migration artifacts, benchmark documents, and the older `FlashTriangularArbitrage.sol` contract for engineering history and regression context.

The **Go / Arbitrum architecture described in this README is the production system**; the legacy Python/Polygon material is not the active runtime.

</details>

---

## Disclaimer

TitanArb is engineering software for detecting and attempting atomic arbitrage. It does not guarantee profitable opportunities or successful execution. Real results depend on live market state, fees, liquidity, RPC quality, execution timing, configuration, and deployed contract state.
