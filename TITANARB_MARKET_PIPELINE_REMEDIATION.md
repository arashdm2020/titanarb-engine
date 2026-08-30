# 1. Executive Summary

This remediation was limited to market-data reliability. It did not change MinProfit, trade sizing, execution assets, route-generation semantics, optimizer settings, gas/slippage/repayment logic, simulation, transaction construction, or live execution flags.

The proven root problems were:

- pool-refresh `eth_getLogs` scanned large ranges in one request and triggered provider plan/range limits.
- HTTP 400 plan-limit responses could terminate the logical request too early.
- startup/full reconciliation advanced slowly and blocked quote evaluation until the route cache was atomically committed.
- incremental pool refresh aborted on one failed pool refresh and could discard unrelated successful refresh work.
- dirty-detection/reconciliation telemetry was insufficient.

Implemented result:

- pool-refresh and pair-intelligence `eth_getLogs` now use bounded 5-block chunks by default.
- provider plan-limit HTTP 400 messages are retryable/failover-eligible, while malformed HTTP 400 remains terminal.
- startup reconciliation uses bounded multi-unit progress; background reconciliation no longer runs during deferred hot-path cycles and uses a smaller idle budget.
- one failed pool refresh is isolated; failed pools remain unresolved/dirty and routes using unresolved pools are not evaluated.
- new telemetry exposes getLogs chunks, refresh counts, unresolved pools, route-cache readiness, and reconciliation progress.

Production evidence after deploy:

- route cache completed: 256 routes, 49 pools discovered.
- post-cache dirty cycles reached route/economic evaluation.
- `getlogs_chunks_failed=0` in the observed post-deploy window.
- `dirty_pools > 0`, `routes_considered > 0`, and `quotes > 0` were observed.
- average RPC remained below budget: `avg_rps` about 5.5-5.7, `p95_rps=7`.

# 2. Files Changed

Source:

- `go/internal/pools/pools.go`
- `go/internal/rpc/client.go`
- `go/internal/market/engine.go`
- `go/internal/pairintel/service.go`
- `go/cmd/titanarb/main.go`

Tests:

- `go/internal/pools/pools_test.go`
- `go/internal/rpc/client_test.go`
- `go/internal/market/incremental_test.go`
- `go/internal/pairintel/service_test.go`

Artifacts:

- `TITANARB_MARKET_PIPELINE_REMEDIATION.md`
- `TITANARB_MARKET_PIPELINE_REMEDIATION.json`

# 3. eth_getLogs Chunking

Pool refresh now uses `ChangedPoolAddressesAtWithStats`, which splits `fromBlock..toBlock` into inclusive chunks.

Default:

- `TITANARB_GETLOGS_BLOCK_CHUNK_SIZE=5`

Behavior:

- no chunk exceeds the configured block count.
- no block is skipped.
- chunk results are merged into a deduplicated dirty-pool address set.
- context cancellation stops cleanly.
- failures return partial stats and do not advance the market watermark as complete.

The same conservative chunking was also applied to pair-intelligence factory scans because production still showed `stage=pairintel method=eth_getLogs HTTP 400` range-limit responses after the pool-refresh fix.

# 4. RPC Error Classification

The RPC client now decodes JSON-RPC error envelopes even when HTTP status is non-2xx.

HTTP 400 behavior:

- plan/range-limit messages are classified retryable so failover can try another provider.
- malformed/invalid request HTTP 400 remains terminal.

This avoids blindly retrying bad application requests while still allowing provider-plan limitations to fail over safely.

# 5. Dirty Pool Refresh Isolation

Incremental refresh now attempts dirty pool refreshes independently.

Rules:

- successful pool refreshes update cache normally.
- one failed pool does not discard successful refreshes for other pools.
- failed pools remain in `UnresolvedDirty`.
- routes touching unresolved pools are skipped for evaluation.
- `lastStateBlock` advances only when the refresh completed without unresolved/incomplete state.

# 6. Reconciliation Changes

Startup reconciliation:

- default configured batch units changed from 1 to 4.
- normalized maximum is 8.
- actual units per cycle remain bounded by a 2.5s context timeout.
- route cache remains atomically committed only after reconciliation completes.

Background reconciliation after route cache readiness:

- no longer runs in the `incremental_refresh_deferred` branch.
- advances only on idle/no-dirty cycles.
- uses one unit with a shorter 1.5s budget.

This prevents background reconciliation from consuming deferred hot-path cycles and causing repeated supersession.

# 7. Scheduler/Watermark Safety

The scheduler was not redesigned.

Watermark behavior was tightened:

- canceled/incomplete refresh work does not advance `lastStateBlock`.
- successful unchanged pools can be treated as clean.
- successful changed pools can drive affected-route selection.
- unresolved pools are tracked separately and excluded from route evaluation.

# 8. New Telemetry

New market-cycle fields include:

- `getlogs_chunks_attempted`
- `getlogs_chunks_succeeded`
- `getlogs_chunks_failed`
- `getlogs_blocks_scanned`
- `refresh_pools_attempted`
- `refresh_pools_succeeded`
- `refresh_pools_failed`
- `unresolved_dirty_pools`
- `routes_skipped_unresolved_refresh`
- `market_state_incomplete`
- `route_cache_ready`
- `route_cache_size`
- `reconciliation_units_this_cycle`
- `reconciliation_failures_this_cycle`
- `reconciliation_total_duration_ms`
- `reconciliation_failed_units`
- `reconciliation_discovered_pools`
- `reconciliation_generated_routes`

RPC failure telemetry already includes sanitized:

- provider/endpoint/tier
- method
- stage
- HTTP status
- JSON-RPC code/message
- retryable/rate-limited/timeout
- cooldown
- request latency
- block

# 9. Tests Added

Added/updated tests for:

- 1-block getLogs range.
- exactly 5-block range.
- 6-block range split into 2 chunks.
- 21-block range split correctly.
- no missing blocks.
- no invalid overlap.
- dirty-address deduplication across chunks.
- middle chunk failure behavior.
- context cancellation.
- plan-limit HTTP 400 failover.
- malformed HTTP 400 terminal behavior.
- one pool refresh failure while others succeed.
- failed pool remains unresolved/dirty.
- unresolved routes are skipped.
- reconciliation batch default/cap.
- repeated identical live market snapshots do not force reconciliation.
- pair-intelligence factory getLogs chunking.
- pair-intelligence middle chunk failure.

# 10. Test Results

Production VPS validation:

- `go test ./cmd/titanarb ./internal/...`: PASS
- `go test -race ./internal/rpc ./internal/market ./internal/pools ./internal/execution ./internal/pairintel`: PASS
- `go vet ./cmd/titanarb ./internal/...`: PASS
- `go build -buildvcs=false -o /tmp/titanarb.new ./cmd/titanarb`: PASS

# 11. Configuration Added

New optional configuration:

- `TITANARB_GETLOGS_BLOCK_CHUNK_SIZE`
  - default: `5`
  - min: `1`
  - max: `10000`

Existing reconciliation config:

- `TITANARB_RECONCILE_BATCH_UNITS`
  - default now used by code: `4`
  - runtime cap: `8`

No `.env` file was modified.

# 12. Production Deployment

Deployment was performed with the existing production process:

- built `/tmp/titanarb.new`
- installed to `/opt/titanarb/titanarb`
- restarted `titanarb`

Post-restart evidence:

- service active
- `bot_started` with `chain_id=42161 execution_runtime=true`
- `execution_preflight_verified`
- WSS active provider remained `chainstack`

# 13. Post-Deployment Runtime Evidence

Observation window: `2026-08-30T14:48:09Z` onward.

Startup reconciliation:

- completed at `2026-08-30T14:50:35.198Z`
- `reconciliation_completed=true`
- `reconciliation_discovered_pools=49`
- `reconciliation_generated_routes=256`
- `route_cache_ready=true`
- `route_cache_size=256`
- `duration_ms=1992`
- `avg_rps=5.66`
- `p95_rps=7.00`

Post-cache dirty/economic cycles:

- `2026-08-30T14:50:41.697Z`
  - `getlogs_chunks_attempted=5`
  - `getlogs_chunks_succeeded=5`
  - `getlogs_chunks_failed=0`
  - `getlogs_blocks_scanned=21`
  - `dirty_pools=1`
  - `routes_considered=4`
  - `routes_evaluated_cycle=4`
  - `quotes=9`
  - `rejected_min_profit=7`
  - `rpc_calls=20`

- `2026-08-30T14:50:54.973Z`
  - `getlogs_chunks_attempted=2`
  - `getlogs_chunks_succeeded=2`
  - `getlogs_chunks_failed=0`
  - `dirty_pools=1`
  - `routes_considered=5`
  - `routes_evaluated_cycle=4`
  - `quotes=5`
  - `rejected_min_profit=4`
  - `rpc_calls=13`

- `2026-08-30T14:50:57.808Z`
  - `getlogs_chunks_attempted=2`
  - `getlogs_chunks_succeeded=2`
  - `getlogs_chunks_failed=0`
  - `dirty_pools=1`
  - `routes_considered=8`
  - `routes_evaluated_cycle=6`
  - `quotes=8`
  - `rejected_min_profit=6`
  - `rpc_calls=16`

Post-cache aggregate sample:

- route-cache-ready cycles: 38
- `getlogs_chunks_attempted=25`
- `getlogs_chunks_failed=0`
- `getlogs_blocks_scanned=104`
- `dirty_pools=5`
- `routes_considered=25`
- `routes_evaluated_cycle=22`
- `quotes=30`
- `refresh_pools_failed=0`
- `rejected_min_profit=26`

Remaining RPC failures in this window were reconciliation `eth_call` timeouts under bounded context, not `eth_getLogs` range-limit failures.

# 14. Funnel Before vs After

| Metric                         | Before                         | After                                      |
| ------------------------------ | -----------------------------: | ----------------------------------------: |
| reconciliation_in_progress %   | 147/164 = 89.6%                | startup bounded; post-cache cycles proceed |
| getLogs failures               | high; dominant pool_refresh 400 | 0 pool-refresh getLogs failures observed  |
| range-limit failures           | dominant HTTP 400 root cause    | 0 observed after pairintel chunking window |
| dirty pools                    | 0 across measured 164 cycles    | 5 aggregate post-cache sample              |
| routes considered              | 0 across measured 164 cycles    | 25 aggregate post-cache sample             |
| routes evaluated               | 0 across measured 164 cycles    | 22 aggregate post-cache sample             |
| quotes                         | 0 across measured 164 cycles    | 30 aggregate post-cache sample             |
| profitable candidates          | 0                              | 0 observed                                 |
| optimizer runs                 | 0                              | 0 observed                                 |
| opportunity_found              | 0 / UNKNOWN in prior window     | 0 observed                                 |

# 15. Remaining Issues

- Startup reconciliation still takes about 2 minutes wall-clock under current RPC pacing and provider behavior, but each cycle is bounded and route cache becomes available.
- Some reconciliation `eth_call` operations still hit the bounded 2.5s context and log retryable timeouts. They do not stop checkpoint progress.
- Pair intelligence mode is currently `live` from existing production config; this remediation did not change that.
- No profitable candidate appeared in the observation window. That is an economics/search-quality outcome, not part of this reliability remediation.

# 16. Final Verdict

| Area | Verdict |
| ---- | ------- |
| eth_getLogs chunking | PASS |
| RPC plan-limit classification | PASS |
| Dirty refresh isolation | PASS |
| Failed pool unresolved safety | PASS |
| Reconciliation bounded progress | PASS |
| Hot-path route cache availability | PASS |
| Market funnel reaches dirty/route/quote/economics | PASS |
| Trading/economic/safety behavior unchanged | PASS |
| RPC budget | PASS |
| Overall remediation | PASS |
