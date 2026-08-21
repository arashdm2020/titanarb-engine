# TitanArb Go Runtime

The Go runtime contains configuration, credential-safe logging, HTTP JSON-RPC,
WSS `newHeads`, metrics, health checks, bounded market discovery, exact executor
calldata construction, simulation, fee repricing, signing primitives, receipt
monitoring, and durable JSONL trade records. It does not import or invoke the
Python runtime.

Run from this directory after configuring the repository root `.env`:

```powershell
go run ./cmd/titanarb
go test ./...
```

`TITANARB_LOG_FORMAT=json` enables JSON logs. `TITANARB_ENV_FILE` selects an
alternate local environment file. Neither mode prints credentials or private keys.

`TITANARB_EXECUTION_RUNTIME=true` starts Go's execution pipeline. A broadcast
remains fail-closed and additionally requires `DRY_RUN=false`,
`EXECUTION_MODE=live`, and `GO_LIVE_EXECUTION=true`. Do not set the
last acknowledgement before Go-only fork and simulation validation are complete.

Set `TITANARB_METRICS_JSON` to a local output path to write a periodic JSON
metrics snapshot. The runtime creates that file with owner-only permissions.

## Benchmark

Run `go test -bench=RPCBlockNumber -benchmem ./...` with
`TITANARB_BENCH_RPC_URL` set locally to measure the Go `eth_blockNumber` call.
Use the same endpoint and conditions when measuring the current Python provider
so connection, block-event latency, and memory results are comparable.
