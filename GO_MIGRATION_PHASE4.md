# TitanArb Go Migration — Phase 4 Development

## Scope and activation boundary

Phase 4 adds a Go-only intelligence and operations layer. It does not change Solidity, the deployed contracts, the Python runtime, quote/simulation safety gates, or transaction logic.

`TITANARB_PHASE4_READ_ONLY=true` enables graph refreshes on the Go runtime. The Phase 4 package has no dependency on a signer, transaction builder, simulation client, or broadcast method. It can therefore only discover pools and build read-only candidate routes.

Go mainnet broadcasting remains separately fail-closed behind the existing Phase 3 gate: `DRY_RUN=false`, `EXECUTION_MODE=live`, and `GO_LIVE_EXECUTION=true`. Phase 4 does not set or change any of these values.

## Architecture

```text
Configured allow-listed assets (equal nodes)
  -> real Uniswap V3 / Camelot Algebra pool discovery
  -> dynamic liquidity graph
  -> simple-cycle route candidates
  -> executable quote chain
  -> authoritative premium + asset-denominated L1/L2 cost input
  -> risk-ranked candidate / loan-size optimizer
  -> read-only opportunity event

Operations sink
  -> redacted JSONL histories
  -> asynchronous Telegram notifications
  -> host/process health snapshots
```

The graph uses every token in `config/arbitrum.json` with no base-asset branch. A discovered pool becomes usable only when both endpoints are in the configured execution-safe universe; this preserves the deployed executor allow-list boundary while allowing new configured tokens and discovered pools to enter without source-code token preferences.

## Capital optimization

`internal/optimizer` takes an asset-specific raw-unit range resolved by an external price/capacity layer (for example, a neutral USD exploration range from approximately 100k to multi-million notional). It evaluates multiple sizes, does not assume that the largest size is best, and returns the highest positive executable net-profit result.

The read-only engine refuses to label a route profitable when it lacks any of: an Aave premium, asset-denominated L2 cost, asset-denominated L1/data cost, or positive minimum-profit input. ETH gas is never compared directly to a token result.

Each configured token may supply an optional `usdFeed` in `config/arbitrum.json`. The Phase 4 `ChainlinkPrices` adapter only consumes these explicit mappings; an asset with no configured source is excluded from profitability decisions instead of receiving an inferred price or a token-specific special case.

## Operations

Set only non-secret operational variables locally when needed:

```text
TITANARB_PHASE4_READ_ONLY=true
TITANARB_OBSERVABILITY_DIR=runtime/go
TELEGRAM_BOT_TOKEN=<local secret>
TELEGRAM_CHAT_ID=<private channel>
```

The operations sink persists `opportunities.jsonl`, `trades.jsonl`, `errors.jsonl`, `performance.jsonl`, and `server.jsonl` with mode `0600` under a mode-`0700` directory. It redacts private keys, API keys, RPC/WSS URLs, and Telegram tokens. Telegram is buffered and fail-open: unavailable delivery is counted/dropped without blocking WSS, RPC, quoting, safety, or execution.

## Migration status

| Python / prior Go responsibility | Phase 4 Go component | Status |
| --- | --- | --- |
| Bounded configured route scan | `internal/graph`, `internal/alpha` | Added, read-only |
| Fixed size screening | `internal/optimizer` | Added, read-only |
| Route risk ranking | `internal/risk` | Added, advisory only |
| Operational JSONL | `internal/observability`, `internal/operations` | Added |
| Operations notifications | `internal/telegram` | Added, opt-in / fail-open |
| Host/process monitoring | `internal/monitor` | Added |
| Go mainnet broadcast | Existing Phase 3 execution pipeline | Still blocked pending Phase 3 validation |

## Validation plan

1. Run `go test ./...`, `go vet ./...`, and `go build ./cmd/titanarb`.
2. Run with `TITANARB_PHASE4_READ_ONLY=true` against an Arbitrum RPC to validate discovery only.
3. Test Telegram with a private configured channel only; no secrets are persisted.
4. Keep Go broadcast disabled until the independent Phase 3 execution-validation gate is complete.
