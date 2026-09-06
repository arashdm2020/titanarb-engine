# Phase 2 — current market coverage audit

Source verified on VPS at `3888169`; service PID 209759 since 2026-09-02 14:07:15 UTC before this deployment. Source working tree had no tracked edits. Historical route/runtime reports were read; their August zero-quote observations are superseded by the measurements below.

## Observation baseline

2026-09-06 09:22:22.106428 to 09:37:22.106428 UTC; bounded existing JSONL reads, no extra market RPC. Evidence: `baseline.json`, `pair_baseline.json`, `phase2_snapshot.py`.

| Measurement | Observed |
|---|---:|
| Completed cycles | 59 |
| Superseded events | 1 |
| Live route cache | 256 |
| Two / three / four hops | 88 / 84 / 84 |
| Cross-venue / Uniswap-only | 71 / 185 |
| Routes considered (sum over cycles; not unique) | 3,531 |
| Routes evaluated (sum) | 256 |
| Hop quote calls reported (sum) | 418 |
| Quote failures | 0 |
| Profitable candidates | 0 |
| Optimizer runs | 4 |
| Dirty-pool observations (sum; not unique) | 363 |
| Cycle duration mean / p95 | 8,254 / 12,953 ms |
| Quote duration mean / p95 | 4,834 / 7,136 ms |
| Reported lifetime average / p95 RPS | 3.36 / 8.00 |
| Per-cycle RPC sum / mean | 2,757 / 46.73 |
| Sync lag mean / p95 | 58.27 / 62 blocks |
| Reported cycle lag | 0 |

Zero cycle lag is **not** proof of zero market-state lag: decision-state/synchronizer lag remains ~58 blocks. One superseded event means Phase-1's earlier zero result is not timeless. Cycles and economics are demonstrably progressing; no Phase-1 redesign is included.

## Canonical universe

Pair persistence contains 10 token addresses, 15 canonical pairs, 55 unique pool addresses, and 10 pairs with both supported DEXes. Persistence includes historical/illiquid pools and is **not** the count of currently active pools. The earlier active-pool count 49 remains contextual until a live snapshot supplies it; do not equate 55 tracked with 55 executable.

The live asset field lists six labels: ARB, USDC, USDC.e, USDC_E_BRIDGED_ALTERNATIVE, USDT, WETH. USDC.e and USDC_E_BRIDGED_ALTERNATIVE resolve to one canonical address. Thus this is five distinct live market token addresses, not six. Approved loan assets remain USDC, WETH, ARB, USDT.

The five additional factory-discovered tokens each have only one Uniswap venue, score 8.75, no quote observations/depth and no observed swaps in the baseline. They are discovered, not validated economic expansion. No claim about scam safety follows from contract code or decimals.

Factory checkpoints: Uniswap 498166848 and Camelot 498166848 versus latest market blocks around 502299xxx. Discovery is bounded and incomplete; it cannot establish the percentage of economically relevant Arbitrum covered. Rejected token/pool totals over all discovery history and total Arbitrum TVL/volume coverage are UNKNOWN.

## Pair candidates and priority

| Class | Evidence / priority |
|---|---|
| USDC/USDT | Existing pair score 70.37; two independent venues; high-priority shadow comparison, not automatically a new asset |
| USDC/USDC.e | Score 68.38; live admitted; high-priority comparison of exact cross-venue pool directions |
| USDT/USDC.e | Score 65.51 but cooled; medium priority pending current depth/freshness/cooldown |
| WETH/USDC, WETH/ARB | Scores 64.14 / 57.02 and cooled; medium priority for bounded size evidence, not forced admission |
| Other existing pairs | Evaluate depth, near misses and fresh venue disagreement; score alone is insufficient |
| Five single-venue unknown tokens | Low priority; behavioral validation, depth and cross-venue evidence missing |
| Curve/Balancer/other venues | Expansion design only; no adapter/math/ABI/execution integration exists in current supported-DEx switches |

No defensible token-level dollar-liquidity ranking can be produced from raw V3 liquidity. Pair volume is in token0 units; values across different token0 assets must not be summed as dollars. RV/volume/depth are recorded in `baseline.json`; confidence does not prove provider-independent simultaneous price dispersion.

## Structural restrictions

Actual search is partial dynamic enumeration, not one fixed route and not cross-chain arbitrage. Four funding starts, same-asset closure, unique intermediates, 2–4 hops, configured DEXes/fees, explicit admitted pair edges, live cap 256, dirty/affected selection and fresh quote budgets restrict it. All generated reverse directions are subject to their own enumeration/selection caps; one evaluated direction does not guarantee the reverse is quoted.

There is insufficient runtime enumeration telemetry to calculate all viable paths beyond the capped set. A combinatorial count without executable pools/depth is not economically meaningful. The shadow observer reports its own canonical candidate set separately and cannot change the production route universe.

## Findings requiring separate economic validation

1. One raw initial amount per asset cannot identify a size optimum; no proof yet that a smaller amount recovers profit.
2. Current roundtrip-loss metric is mislabeled as price impact; do not treat it as independent slippage evidence.
3. The live provisional cost conversion uses USD value scaled by token decimals, without a token/USD divisor. This is dimensionally inconsistent for non-dollar funding assets; Phase 2 preserves live behavior and flags legacy net as unverified.
4. Net and gross comparisons must normalize denominations. Current raw minima and route-memory magnitudes are not common-numeraire money.
5. The deployed executor deliberately requires closure in the borrowed token. A residual-inventory transaction needs an explicit atomic close-out plan, liquidity valuation and eventual separately reviewed executor support. None is enabled here.
