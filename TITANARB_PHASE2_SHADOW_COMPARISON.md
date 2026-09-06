# Phase 2 shadow comparison — observation pending

Baseline: production `3888169`, 2026-09-06 09:22:22–09:37:22 UTC. See `TITANARB_PHASE2_MARKET_COVERAGE_AUDIT.md` and `baseline.json`.

The first milestone implements a bounded independent observer, explicit multi-asset accounting and quality/size proposals. This document must be updated after validation, deployment, readiness and at least 60 minutes of observation. Synthetic valuation tests are not production profit evidence.

| Metric | Current baseline | Phase-2 shadow |
|---|---:|---|
| Distinct live market token addresses | 5 | Pending |
| Tracked tokens / pairs / pools | 10 / 15 / 55 | Pending |
| Live routes | 256 | Must remain 256 |
| Routes evaluated in 15m | 256 | Pending |
| Hop quotes in 15m | 418 | Shadow requests separately counted |
| Profitable candidates | 0 | UNKNOWN until independent costs/valuation |
| Cycle mean / p95 | 8.25 / 12.95 sec | Pending |
| Sync lag mean / p95 | 58.27 / 62 blocks | Pending |
| Gross-positive shadow probes | Not measured | Pending |
| Independently net-positive shadow candidates | Not measured | UNKNOWN; liquidation/cost evidence missing |
| Phase-2 broadcasts | N/A | Always 0 by API boundary |

## Interpretation rules

- `current_evaluation`: newly observed route-level results, including raw canonical key, legacy economics and roundtrip loss. Do not count replay as a new shadow quote.
- `shadow_portfolio_replay`: demonstrates liability-first accounting. Missing liquidation prices/costs preclude a complete net valuation.
- `shadow_comparison`: pool/pair scores and what shadow would select. Proposal counts are not evaluations, and repeated proposals are not new market coverage.
- `shadow_quote`: actual independent pinned-block read-only quote. A positive gross value does not establish positive net or execution eligibility.
- Record busy yields, timeouts, drops, write errors, successful probes, all extra provider requests, and timestamp coverage. Compare same-asset units or input-normalized bps, never raw WETH against raw USDC.

## Initial capability verdict

Accounting abstractions: implementation present; runtime independent valuation evidence incomplete.

Quality scores and canonical identities: implemented in shadow. Broader economically useful coverage, better size selection, positive-gross uplift and better near misses: UNVERIFIED.

Execution promotion: NONE. Residual execution and inventory rebalancing remain disabled. A broad Phase-2 PASS requires measured economic and coverage improvements; the observer deployment alone is not sufficient.
