# Phase-1 Benchmark

This benchmark intentionally measures only read-only infrastructure. It does
not construct, sign, simulate, or broadcast transactions.

## Go RPC

Run from `go/` with a local, non-committed environment value:

```powershell
$env:TITANARB_BENCH_RPC_URL = '<local Arbitrum HTTP endpoint>'
go test -run '^$' -bench=RPCBlockNumber -benchmem .
```

The result reports `eth_blockNumber` latency plus allocation count and bytes.

## Python comparison

Run the existing Python provider against the same endpoint and network
conditions, then record the median `eth_blockNumber` latency. Compare only
median/percentile latency; a one-off request is not a measure of trading
performance.

## WSS connection and first event

The Go runtime emits `bot_started`, `wss_connected`, and
`new_block_received`. The elapsed time between these local timestamps gives the
connection and first-header observation time. Process-memory comparisons should
be taken with the same Windows process tool, after the same warm-up period.
