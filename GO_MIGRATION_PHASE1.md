# TitanArb Go Migration — Phase 1

The Python bot remains the sole production execution implementation. The Go
subsystem is intentionally read-only infrastructure.

| Current Python component | Future Go component | Phase-1 status |
|---|---|---|
| `bot/config/settings.py` | `go/internal/config` | Foundation complete |
| `bot/logger.py` | `go/internal/logger` | Foundation complete |
| `bot/provider.py` | `go/internal/rpc`, `go/internal/websocket` | Foundation complete |
| `bot/main_arbitrum.py` | `go/cmd/titanarb` | Runtime shell only; not migrated |
| `bot/arbitrum_pipeline.py` | Future Go route layer | Not migrated |
| `bot/executor_arbitrum.py` | Future Go execution layer | Not migrated |

## Explicit Phase-1 boundary

The Go binary only reads RPC/WSS data and emits metrics. It cannot sign, build,
simulate, or submit arbitrage transactions. Migration of strategy or execution
requires a separately authorized later phase.

## Benchmark

`go test -bench=RPCBlockNumber -benchmem ./...` uses
`TITANARB_BENCH_RPC_URL` when supplied. Record the Python provider’s
`eth_blockNumber` latency with the same endpoint and network conditions before
comparing connection time, event-receive latency, or memory allocation.
