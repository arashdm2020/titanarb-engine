# TitanArb Phase 2 — decision intelligence, first shadow milestone

Production baseline: `3888169`, verified 2026-09-06. Existing engine is authoritative. New package `go/internal/decision` is observe-only and has no signer, transaction submission, execution pipeline, or opportunity-event dependency. No strategy, execution, Phase-1 synchronization or provider policy changes are included.

## 1. Source-verified assumptions

Paths below are repository-relative. Line references describe baseline unless stated otherwise.

| Assumption | File / function | Current behavior | Architectural effect |
|---|---|---|---|
| Circular closure | `go/internal/routes/routes.go:78-99`, `buildFromWithShare` | Appends funding token to every 2–4-hop permutation | Does not generate branching partial-liquidation or residual-inventory transactions |
| Same start/end | `go/internal/opportunity/engine.go:317-324,411-417`, `EvaluateSilent`, `Evaluate` | Rejects a different last symbol | Funding, terminal and profit denomination coupled |
| On-chain closure | `contracts/FlashArbitrageExecutor.sol:234-247,275-298` | First and last token equal borrowed asset; all tokens allowed | Residual analysis cannot be promoted simply by changing detector logic |
| Builder closure | `go/internal/execution/builder.go:28-46`, `Request.Validate` | Borrowed asset at both ends, 2–4 steps | Current transaction format stays unchanged |
| Funding membership | `go/internal/config/config.go:88-134`, `ExecutionAssets`, `MarketAssets` | Explicit execution set separate from market intermediates | Market admission alone grants no borrowing permission |
| Asset boundary | `go/cmd/titanarb/main.go`, `routeStartsAndEndsWithExecutionAsset` | Enforces approved start/end | Preserved |
| Same-unit net calculation | `go/internal/pricing/pricing.go`, `Evaluate` | output−input−premium−L1−L2; strict net > MinProfit | Requires all operands in identical token units |
| Fee conversion | `go/internal/opportunity/engine.go:80-100`, `ArbitrumCostModel.Estimate`, `feeToBaseRaw` | USD fee multiplied by 10^assetDecimals, without dividing by asset USD price | **USD-scaled units are not raw WETH/ARB units.** Existing live net is labeled legacy; no live fix applied in Phase 2 |
| Fixed provisional gas | Same cost model | 5,000,000 gas, synthetic ABI-sized payload | Not a per-route simulated gas measurement |
| Minimum denomination | `cmd/titanarb/main.go`, `buildMarketEngine`; `pricing.Evaluate` | One raw minimum loaded and reused in token-unit evaluation | Raw minima cannot be compared across tokens as dollars |
| Initial amount | `cmd/titanarb/main.go`, `marketAmounts` | Per-execution-asset raw config/fallback | One initial size does not establish optimal size |
| Existing prequote | `market/engine.go`, `selectEvaluationCandidates` | RouteMemory + PairScore + dirty/volatility + exploit/explore | Already exists; retained for production |
| Pair aggregation | `market/engine.go`, `routePairQuality` | Weakest edge / harmonic contribution | Existing intelligence is reused as input, not duplicated as a live scheduler |
| Loss labeled impact | `nearmiss/nearmiss.go`, `PriceImpactBPS` | max(0,input−output)/input for a full round trip | This mixes spread, fees and impact; shadow labels it roundtrip loss, actual impact UNKNOWN |
| Quote state | `opportunity/engine.go`, `quoteKey`; `rpc/client.go`, `EthCall` | Persistent cache can key pool LastUpdatedBlock; regular EthCall uses latest | LastUpdatedBlock is not proof of a common quote block. New shadow probes explicitly pin every eth_call |
| DEX scope | `pools/pools.go`, `quotes/quotes.go` | Uniswap V3, Camelot Algebra only | No Curve/Balancer math or bridge execution inferred |

## 2. Implemented economic objects and equation

`decision/economics.go` introduces canonical-address `Asset`, `FundingState`, `Liability`, `ResidualInventory`, `ValuationState`, `PortfolioInput` and `PortfolioResult`. Integer raw balances and rational USD arithmetic prevent floating-point money errors. The runtime is Arbitrum-only; cross-chain balances cannot enter this model.

Let B0 be owned equity before funding, B1 assets after swaps, L principal plus premium, and C unpaid gas/L1/other costs. Borrowed proceeds are not initial equity:

`R = B1 − L − C` in each actual asset; `NetUSD = LiquidationValueUSD(R) − LiquidationValueUSD(B0)`.

Do not subtract principal a second time, add settlement surplus twice, or subtract DEX fees already included in output. Liabilities must first be covered in raw liability-token balances. A valuable ARB balance cannot rescue an unpaid USDC liability. Costs must also be fundable; otherwise no positive complete valuation is emitted.

Each residual value requires an independent, reviewed-token, executable liquidation bound, maximum supported amount, nonfuture fresh timestamp and explicit haircut. Unsupported/stale/self-referential prices return UNKNOWN. No stablecoin is silently pegged to $1. USD is the initial analytical numeraire. Configured symbol names never establish security identity.

The generic multi-asset evaluator passes deterministic residual/settlement tests. **Production independent residual liquidation prices and transaction close-out plans are not yet present.** Runtime portfolio replay therefore reports missing evidence instead of claiming executable profit. Inventory-aware execution is permanently false in this observer.

## 3. Opportunity types and execution boundary

| Type | Analysis in this milestone | Execution |
|---|---|---|
| A atomic circular | Existing economics replay, quality ranking, independent shadow quotes | Existing engine only |
| B two-hop cross-DEX | Explicit canonical pair / different DEX proposal construction | Existing engine only |
| C multi-hop circular | Existing 3/4-hop routes retained in shadow candidate set | Existing engine only |
| D residual inventory | General accounting/valuation implemented; production close-out/pricing evidence missing | Disabled |
| E inventory rebalancing | Design only: compare owned portfolios after funded costs and exposure limits | Disabled |

`ATOMIC_RISK_FREE` is the requested mode label, not a guarantee of risk-free trading. Existing simulation, repayment, sequencer/oracle, stale-state and slippage gates remain authoritative.

## 4. Quality and decision model

Pool score: 50% recent executable depth + 30% quote success + 20% depth freshness − 25% failure penalty. Fresh bidirectional depth requires actual successful samples within five minutes; raw V3 liquidity is not TVL. Unobserved quotes receive a neutral prior. Pool age, provider-specific reliability and independent impact are UNKNOWN where not recorded.

Pair score (all normalized): 15% realized volatility + 15% volume/turnover + 25% depth + 15% usable independent venues + 15% quote success + 15% dislocation − 20% failure − 20% illiquidity. Existing PairMemory components and Confidence are retained with explicit provenance. Scores are explainable heuristics, not calibrated probabilities or expected profit. Historical dislocation can reflect asynchronous venue observations; it does not prove tradable spread.

Route quality combines 40% pair quality, 25% pool quality, cross-venue/dirty bonuses, hop cost and bounded historical gross/net-bps, loss, failure and latency signals. Multi-hop pair/pool aggregation is `0.6*weakest + 0.4*harmonic_mean`. Historical money signals are normalized by input, never compared as raw integers across decimals. Repeated loss reduces priority; exponential decay, a new dirty signal and exploration allow reconsideration. Legacy net is not treated as independently validated USD profit.

At most eight proposals per analysis minute; at least one exploration slot for budget >=2; per-funding-asset quota and rotating start priority. Shadow history is bounded to 4,096 routes and persisted. Canonical full route keys include token addresses, pool addresses, DEXes, directions and fee tiers.

## 5. Shadow market admission

Reuse existing factory/PairMemory discovery; no extra factory scans. A shadow pair needs score >=65, Confidence >=.70, one-hour observation, fresh data, positive observed one-hour activity, no cooldown and two independently usable DEXes. It may add explicit pair edges only, anchored to an existing approved funding asset. No unrelated edges are added merely because a token exists. Deterministic address identities handle symbol collisions.

New candidates remain shadow proposals. `hasCode` and valid decimals alone do NOT prove no honeypot, no tax/rebase, transfer compatibility or safe execution. Token behavioral review and liquidation evidence remain promotion requirements. The existing live PairMemory mode/admission remains untouched.

The observer retains live 3/4-hop candidates and can add bounded two-hop cross-venue proposals up to 512 total analytical candidates. The **live route cap remains 256**. Analytical candidate count is not quote count and not proof of broad market coverage.

## 6. Amount exploration and budgets

Bounded asset-raw ladder: current amount divided by 100,20,10,4,1, deduplicated and positive. A fresh successful first-hop depth bucket can lower the ceiling. No first-hop cap is extrapolated into guaranteed full-route depth. This is a conservative starting ladder, not USD notionals or an optimizer replacement.

One route/size probe per minute, <=12 logical reads/minute, >=500ms local pacing, four-second cancellable deadline, production global provider budgets and Background class. Every actual shadow eth_call uses the same explicit block. Busy hot-path work causes immediate yield. No quote result is extrapolated across sizes. Gross and premium can be measured; net stays UNKNOWN without independently denominated costs. Provider-internal retries may exceed logical-call counts, so provider telemetry remains the authoritative network-work measure.

The observer cannot access submission APIs. Live quote counters/budgets are not reused or reset; shadow quoters use nil live metrics. The existing live event stream is neither consumed nor altered by shadow. Snapshot and observation queues are bounded and nonblocking, with drop counters. JSONL rotates at 32 MiB with one prior segment; dropped samples/rotation must be considered in comparison.

## 7. Wiring and rollback

Only live-source hooks: `market.Engine.SetShadowObserver`, detached snapshot offer after cycle completion, copied per-route evaluation observations; startup in `cmd/titanarb/main.go`.

Configuration:

- `PHASE2_SHADOW_ENABLED=true` default; false removes observer activity at next startup.
- `PHASE2_SHADOW_PROBES_ENABLED=true` default; false retains zero-new-RPC analysis.
- `PHASE2_SHADOW_MAX_READS_PER_MINUTE=12`, validated 1..12.

No `.env` or systemd changes are required. No Phase-2 LIVE mode exists. Persisted route history contains no credentials. Shutdown preserves history; normal minute snapshots also checkpoint it. Failed writes appear as `write_errors` if storage later becomes writable.

## 8. Validation and remaining scope

Tests cover liability settlement, residual assets, valuation freshness/independence/depth, costs/equity exactly once, canonical identities, same-DEX eligibility, route dedupe, exploration, normalization/recovery, detached snapshots, concurrent offers, persistence, pinned reads, budgets, cancellation and unresolved-pool exclusion. Existing market/execution suites must pass unchanged.

Not claimed complete: broad Arbitrum discovery coverage; new token behavioral proof; production independent liquidation price graph; residual partial-close-out planner; net-profit comparison for all assets; economic uplift; execution promotion. These require evidence beyond a score. This deployment is the first bounded shadow milestone, not completion of all Phase-2 success criteria.
