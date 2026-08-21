# Go vs Python Market Benchmark

## Fixed workload

Use Arbitrum One at the same current block/RPC provider, the configured token
universe (`USDC`, `WETH`, `ARB`, `USDT`), configured Uniswap fee tiers, and a
bounded 45-second cycle. Record the first completed cycle only; warm caches are
reported separately. Do not run either workload with execution enabled.

## Measurements

| Metric | Go observed | Python observed |
|---|---:|---:|
| RPC `eth_blockNumber` median | 154.80 ms/op | 150.35 ms |
| WSS connection | ~0.51 s | Not sampled in this benchmark |
| First header | ~0.83 s | Not sampled in this benchmark |
| Full configured pool scan | 58 pool observations | Did not complete in 30 s |
| Route evaluation | 108 routes with static inputs; 52 with live fee service | Did not complete in 30 s |

The previous Python full discovery process was stopped after 30 seconds. It was
not a production runner and its termination does not imply a Python failure.
These results do **not** support a claim that Go is faster yet.

## Required closure run

Run both implementations on the same explicitly constrained route set, amount,
and quote count; collect startup time, first-header-to-cycle-complete latency,
pool discovery time, quote throughput, routes/sec, RPC calls, cache hit ratio,
process memory, and CPU from the same Windows process monitor. Only then update
this document with a performance conclusion.
