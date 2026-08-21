# TitanArb Go Migration — Phase 2

The Python bot remains the production executor. The Go market engine is
read-only: it discovers configured real pools, obtains executable quotes, builds
bounded 2--4-hop cycles, and emits advisory opportunity events. It cannot sign,
simulate executor calls, approve tokens, or broadcast transactions.

| Python reference | Go component | Status |
|---|---|---|
| `bot/pools.py` | `go/internal/pools`, `go/internal/cache` | Complete, read-only |
| `bot/quotes.py` | `go/internal/quotes` | Complete, read-only |
| `bot/arbitrum_pipeline.py` route discovery | `go/internal/routes`, `go/internal/market` | Complete for bounded 2--4-hop discovery |
| `bot/gas_arbitrum.py` fee sources | `go/internal/fees` | Complete for read-only provisional estimates |
| `bot/arbitrum_pipeline.py` profitability | `go/internal/pricing`, `go/internal/opportunity` | Partial: read-only screening only |
| `bot/executor_arbitrum.py` | Not migrated | Intentionally not migrated |

## Fee-model boundary

The default runtime model now mirrors Python's provisional read-only calculation:
ArbGasInfo L2 prices, NodeInterface compression-aware L1 cost, Chainlink
ETH/USD conversion, and the configured safety margin. It deliberately remains a
provisional 5M-gas, ABI-size route estimate. An execution-capable phase must
still retain Python's authoritative immediate post-simulation repricing gate.

## Read-only runtime opt-in

Set `TITANARB_MARKET_ENGINE=true` only in the Go process, together with
`TITANARB_MARKET_AMOUNT_RAW`, `TITANARB_MARKET_L2_FEE_RAW`,
`TITANARB_MARKET_L1_FEE_RAW`, and `TITANARB_MARKET_MIN_PROFIT_RAW`. These are
raw units of the configured base asset. They are not written to `.env`, and the
runtime has no transaction code regardless of these values.
