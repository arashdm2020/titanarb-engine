# TitanArb — Arbitrum One Arbitrage Engine

TitanArb is a production-oriented arbitrage engine for **Arbitrum One**. The current runtime is written in **Go** and combines block-driven market discovery, multi-venue route search, executable quoting, adaptive opportunity ranking, Aave V3 flash-loan execution, and a guarded on-chain executor.

The active architecture is built around:

- **Arbitrum One** (`chainId = 42161`)
- **Go runtime** under `go/`
- **Aave V3** flash liquidity
- **Uniswap V3** and **Camelot V3 / Algebra** adapters
- **2-hop, 3-hop, and 4-hop** cycle discovery
- incremental dirty-pool refresh and latest-head block coalescing
- dynamic market-only universe expansion
- pre-quote route intelligence with durable `RouteMemory`
- exploit/explore scheduling
- adaptive optimizer depth
- active-active HTTP RPC reads with provider failover
- single-owner WSS subscriptions
- live execution preflight, simulation, profitability, slippage, repayment, and stale-state gates
- structured observability and Telegram operational reporting

> TitanArb is an engineering system for detecting and attempting arbitrage. It does not guarantee profitable opportunities or successful execution.

---

## 1. Current architecture

```text
                           Arbitrum One
                                │
                     WSS heads / pool logs
                                │
                                ▼
                    ┌─────────────────────┐
                    │ scheduler.Latest    │
                    │ latest-head coalesce│
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Market Engine       │
                    │ incremental refresh │
                    │ dirty route subset  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Route Intelligence  │
                    │ RouteMemory         │
                    │ PreQuoteScore       │
                    │ exploit / explore   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Executable Quotes   │
                    │ Uni V3 / Camelot V3 │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Opportunity Engine  │
                    │ premium + fees      │
                    │ min-profit economics│
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Optimizer           │
                    │ adaptive samples    │
                    │ near-miss ranking   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Execution Pipeline  │
                    │ freshness           │
                    │ simulation          │
                    │ slippage / repayment│
                    │ broadcast           │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ FlashArbitrage      │
                    │ Executor            │
                    └──────────┬──────────┘
                               │
                  Aave V3 flash loan callback
                               │
                               ▼
                  Uniswap V3 / Camelot V3 swaps
                               │
                               ▼
                      repay + realized profit
```

The detector and execution layers are intentionally separated: detector scoring may change which routes receive quote/optimizer budget, but it does not bypass execution profitability or safety checks.

---

## 2. Repository layout

```text
.
├── go/                         # Current production runtime
│   ├── cmd/titanarb/           # Main TitanArb process
│   ├── internal/
│   │   ├── config/             # Runtime / market configuration
│   │   ├── market/             # Block-driven market engine
│   │   ├── scheduler/          # Latest-head coalescing
│   │   ├── pools/              # Pool discovery / refresh
│   │   ├── routes/             # 2/3/4-hop route graph + builders
│   │   ├── quotes/             # Executable quote plumbing
│   │   ├── opportunity/        # Route economics
│   │   ├── nearmiss/           # Rejected economics + RouteMemory
│   │   ├── optimizer/          # Loan-size search
│   │   ├── execution/          # Simulation / live execution pipeline
│   │   ├── rpc/                # Multi-provider HTTP RPC client
│   │   ├── websocket/          # Managed WSS subscriptions
│   │   ├── universe/           # Dynamic market universe
│   │   ├── metrics/            # Runtime metrics
│   │   ├── observability/      # JSONL telemetry
│   │   └── telegram/           # Operational Telegram interface
│   └── pkg/
├── contracts/
│   ├── FlashArbitrageExecutor.sol
│   ├── FlashTriangularArbitrage.sol     # Legacy contract retained for history/tests
│   └── adapters/
│       ├── UniswapV3Adapter.sol
│       └── CamelotV3Adapter.sol
├── config/
├── test/
├── script/
├── RUNBOOK_ARBITRUM_FORK.md
└── GO_MIGRATION_PHASE*.md
```

The old `bot/` Python implementation and Polygon-era material remain in the repository as migration/history artifacts. **They are not the current production runtime.**

---

## 3. Market engine

TitanArb is event-driven rather than a blind full-market polling loop.

### Block scheduling

`scheduler.Latest` keeps a single pending latest head. If several blocks arrive while a market cycle is running, intermediate heads can be superseded instead of building an unbounded backlog.

### Incremental refresh

The market engine distinguishes between:

- **full reconcile** — rebuild/refresh the broader route state periodically or when route-search configuration changes
- **incremental refresh** — update dirty pools and routes affected by those pools

This keeps the route graph reusable between cycles and avoids rebuilding every route on every head.

### Route search

The production route builder supports bounded **2-hop, 3-hop, and 4-hop** cycles and preserves per-loan-asset route budgeting. Routes are assembled from the market graph rather than a single hardcoded triangular path.

---

## 4. Route intelligence and near-miss memory

TitanArb records rejected route economics and uses that history to decide where expensive quote/optimizer work should be spent.

`go/internal/nearmiss` contains:

- full rejected-route economics
- cross-venue detection
- route scoring
- persistent in-process `RouteMemory`
- pre-quote scoring

Representative route-memory fields include:

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

The pre-quote scorer is deliberately **zero-RPC**. It uses cached/history signals before the first expensive route evaluation.

### Exploit / explore

The detector does not only evaluate the current highest-ranked routes. A configurable exploration share keeps unseen and low-observation routes eligible so the scoring loop does not permanently starve new candidates.

Cross-venue routes receive a positive search prior, but same-DEX routes remain eligible; venue diversity is a ranking signal, not an execution rule.

---

## 5. Dynamic universe

TitanArb separates the **market graph** from the **execution / loan-asset boundary**.

A dynamically discovered asset may participate as a market/intermediate graph node without automatically becoming a flash-loan asset or execution-allowlisted token.

This distinction allows the detector to expand connectivity without silently expanding execution authority.

Universe management considers objective market signals such as pool availability, connectivity, quote viability, and configured admission policy.

---

## 6. RPC architecture

The Go runtime supports multiple RPC providers.

### HTTP reads

Read-heavy JSON-RPC traffic can be distributed across healthy providers using an active-active dispatcher. Provider choice considers:

- configured target request budget
- provider cooldown / health
- observed latency
- block freshness
- recent transport / rate-limit failures

A **global read limiter** caps aggregate read traffic so adding a second provider does not double the engine's RPC demand.

### WSS

WebSocket ownership is intentionally single-active:

```text
primary WSS
   ↓ failure
secondary WSS
```

Only one subscription stream is allowed to drive market scheduling at a time, preventing duplicate block/log processing.

### Broadcast

`eth_sendRawTransaction` remains a single-provider path. The RPC layer does not blindly broadcast the same signed transaction through multiple providers.

Provider configuration is environment-driven; provider credentials are not part of the repository.

---

## 7. Opportunity economics

A route is evaluated using executable output and explicit cost components rather than simple spot-price comparison.

The opportunity layer tracks values such as:

```text
amount_in
amount_out
gross_profit
Aave premium
L1 data fee
L2 fee / gas estimate
price impact
expected / net profit
minimum profit threshold
profitability gap
```

Rejected opportunities feed the detector's near-miss telemetry and future route priority.

---

## 8. Optimizer

The optimizer searches loan size for routes that survive initial route selection/evaluation.

Current scheduling supports score-dependent sample depth: stronger candidates can receive more optimizer budget while weak candidates receive less work. The optimizer remains bounded by market-cycle and RPC constraints.

Detector ranking and optimizer ranking have different jobs:

- **Pre-quote score:** which routes deserve expensive evaluation?
- **Post-quote / near-miss score:** which evaluated routes deserve deeper size search?

---

## 9. Execution pipeline

The live execution path is guarded independently from detector scoring.

The execution pipeline preserves checks for:

- chain / deployment preflight
- executor and adapter verification
- route / asset allow-lists
- current state freshness
- minimum-profit economics
- slippage bounds
- simulation
- Aave repayment feasibility
- signer / nonce / transaction handling

A high detector score cannot bypass these gates.

---

## 10. Smart contracts

### `FlashArbitrageExecutor.sol`

Current generalized flash-arbitrage executor used by the Arbitrum architecture.

It delegates swap execution through approved adapters and is designed around Aave V3 flash liquidity and bounded multi-hop routes.

### DEX adapters

Current adapter implementations:

- `contracts/adapters/UniswapV3Adapter.sol`
- `contracts/adapters/CamelotV3Adapter.sol`

These keep venue-specific router semantics outside the generalized executor.

### Legacy contract

`FlashTriangularArbitrage.sol` belongs to the earlier fixed triangular design and is retained for migration/history compatibility. It is not the architectural description of the current Go/Arbitrum engine.

---

## 11. Current Arbitrum deployment

Production deployment addresses currently used by TitanArb:

| Component | Address |
|---|---|
| FlashArbitrageExecutor | `0xdc63781E4f880F3911260Ecf0f1208eB32756666` |
| UniswapV3Adapter | `0xD03BC6e7331E726dA03De74b37437F1ACa2dFD95` |
| CamelotV3Adapter | `0x1318D03c876DE4E3686335B2A80412c83C53A99D` |
| Aave V3 Pool | `0x794a61358D6845594F94dc1DB02A252b5b4814aD` |
| Uniswap V3 Router | `0xE592427A0AEce92De3Edee1F18E0157C05861564` |
| Camelot / Algebra Router | `0x1F721E2E82F6676FCE4eA07A5958cF098D339e18` |

Common configured market assets include WETH, USDC, USDT, ARB, and market-only dynamic assets such as USDC.e when admitted by the universe manager.

Execution-eligible assets and dynamic market assets are intentionally separate configuration concepts.

---

## 12. Configuration

Runtime configuration is environment-driven and market configuration is loaded from repository config files.

Typical runtime categories include:

```text
CHAIN_ID
HTTP / WSS RPC providers
provider target / max RPS
execution mode
private signer configuration
executor / adapter addresses
profit thresholds
route-search depth / budget
slippage / price-impact limits
universe policy
Telegram / observability configuration
pre-quote ranking / exploration controls
```

Do not treat examples in historical Polygon files as current Arbitrum production configuration.

---

## 13. Build and test

The production Go module lives under `go/`.

```bash
cd go

go test ./cmd/titanarb ./internal/...
go vet ./cmd/titanarb ./internal/...
go build -buildvcs=false ./cmd/titanarb
```

For the broadest repository validation where the local platform supports all dependencies:

```bash
cd go
go test ./...
go vet ./...
```

Foundry tests and Arbitrum fork validation are documented separately in `RUNBOOK_ARBITRUM_FORK.md` and the migration/integration notes.

---

## 14. Production runtime

The production process is intended to run as a long-lived service.

Typical deployment shape:

```text
systemd
  └── titanarb
       └── Go binary
            ├── managed HTTP RPC providers
            ├── managed WSS provider
            ├── market engine
            ├── optimizer
            ├── execution worker
            └── observability / Telegram
```

Production deployment should validate tests/build, contract preflight, provider connectivity, and runtime telemetry before considering a release healthy.

---

## 15. Observability

TitanArb emits structured operational events and market-cycle telemetry. Depending on build/configuration, this includes:

- active universe assets
- route counts and hop distribution
- DEX diversity
- routes considered / quoted
- exploit / explore counts
- pre-quote score statistics
- near-miss economics
- optimizer runs / samples
- RPC calls and provider health
- active HTTP / WSS provider
- cycle duration and block lag
- stale candidates
- execution preflight and transaction outcomes

Telegram is an operational view over this telemetry rather than the source of truth; JSONL/runtime logs remain useful for detailed analysis.

---

## 16. Design principles

TitanArb currently follows several important architectural boundaries:

1. **Detector intelligence does not weaken execution gates.**
2. **Dynamic market assets do not automatically become execution assets.**
3. **Cross-venue is a search prior, not a hard requirement.**
4. **Exploration prevents history-driven route starvation.**
5. **Dirty-pool updates should invalidate/revive only affected work where possible.**
6. **RPC resilience must not multiply total RPC demand.**
7. **WSS remains single-owner to avoid duplicate market cycles.**
8. **Transaction broadcast remains single-path.**
9. **Profitability is evaluated after Aave premium and chain fees, not from gross spread alone.**
10. **Production changes are measured through telemetry, not assumed to improve opportunity yield.**

---

## 17. Legacy Polygon / Python implementation

This repository began as a Polygon triangular-arbitrage project using Python, Web3.py, a fixed three-leg Uniswap V3 route, and `FlashTriangularArbitrage.sol`.

That code is intentionally retained for historical context and migration evidence, including:

- `bot/`
- Polygon configuration
- original Python tests
- migration phase documents
- Go-vs-Python benchmarks

The legacy code should not be confused with the current production architecture described above.

---

## 18. Additional documentation

Useful repository documents include:

- `RUNBOOK_ARBITRUM_FORK.md`
- `GO_MIGRATION_PHASE1.md`
- `GO_MIGRATION_PHASE2.md`
- `GO_MIGRATION_PHASE3.md`
- `GO_MIGRATION_PHASE4.md`
- `GO_VS_PYTHON_BENCHMARK.md`
- `GO_VS_PYTHON_FINAL_BENCHMARK.md`
- `PHASE2_INTEGRATION_VALIDATION.md`
- `go/BENCHMARK.md`

Some older documents describe intermediate migration states; when documentation conflicts with current Go source, the current source is authoritative.

---

## License / use

No profitability guarantee is made. Operational behavior depends on live market state, liquidity, fees, provider quality, execution timing, configuration, and deployed contract state.
