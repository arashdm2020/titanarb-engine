# TitanArb Go Migration — Phase 3

## Implemented Go runtime boundary

The Go subsystem now owns the runtime components required for a staged
execution migration. It has no Python import, subprocess, RPC dependency, or
transaction-building dependency at runtime.

| Legacy reference | Go component | Status |
|---|---|---|
| `bot/executor_arbitrum.py` calldata construction | `go/internal/execution` | Implemented; parity-tested for 2, 3, and 4 hops |
| Python ABI JSON | `go/internal/execution/abi/flash_arbitrage_executor_v1.json` | Versioned Go ABI |
| Python nonce/signing | `go/internal/wallet` | Implemented |
| Python simulation/reprice sequence | `go/internal/simulation`, `go/internal/fees` | Implemented |
| Python oracle/sequencer safety gate | `go/internal/safety` | Implemented, fail-closed |
| Python transaction lifecycle | `go/internal/tx` | Implemented |
| Python operational JSONL trade records | `go/internal/profit` | Implemented |

## Go execution safety sequence

1. Go market engine produces a bounded, quoted opportunity.
2. The Go oracle and sequencer gate validates live chain data.
3. The execution builder validates 2–4-hop continuity, derives non-zero
   per-hop minimum outputs, and encodes `SwapStep[]`.
4. `eth_call` runs against the configured executor.
5. `eth_estimateGas` runs only after the call succeeds.
6. The Arbitrum service recomputes L2 execution cost and compression-aware L1
   posting cost from current node/precompile data.
7. Final base-asset profitability includes principal, current Aave premium,
   L1/L2 costs, and positive `minProfit`. Any non-profitable result rejects.
8. Only then can the signer reserve a pending nonce, sign EIP-1559 data, send,
   wait for a receipt, and persist a JSONL record.

## Controlled cut-over

The Go binary can run discovery, simulation, and execution without Python:

```powershell
go run ./cmd/titanarb
```

Execution is staged and fail-closed. `TITANARB_EXECUTION_RUNTIME=true` enables
the Go execution pipeline, but a mainnet broadcast additionally requires all
of the existing `DRY_RUN=false` and `EXECUTION_MODE=live` safeguards **and**
the independent operational acknowledgement `GO_LIVE_EXECUTION=true`.
That acknowledgement is intentionally not written to `.env` by code.

## Actual Solidity ABI boundary

The current deployed source exposes `InsufficientProfit(required, available)`,
`InsufficientOutput(expectedMin, actual)`, and `InvalidRoute`. It does **not**
declare `InsufficientRepayment`; the Go ABI intentionally reflects source
reality rather than a stale specification.

## Remaining validation gate

The Go execution pipeline has compiled and unit-tested construction, signing
primitives, simulation ordering, oracle checks, fee repricing, and Python ABI
parity. It has not yet completed Go-driven 2/3/4-hop local-fork executions or
the staged mainnet simulation-only exercise. Python remains preserved as the
historical reference until those Go-only validations pass; it must not be
deleted or silently superseded before then.
