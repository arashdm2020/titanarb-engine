# Deterministic Go vs Python Benchmark

## Controlled workload

- Fork: local Anvil, Arbitrum One chain ID 42161
- Pinned block: `496890646`
- Tokens: USDC, WETH, ARB, USDT registry; evaluated route was USDC → WETH → ARB → USDC
- Route limit: 1
- Input: `1,000,000,000` raw USDC (1,000 USDC)
- Quotes: 3
- Pool selection: highest-liquidity Uniswap V3 pool per edge
- Execution: none; both workloads are read-only

## Results

| Metric | Python pipeline | Go market components |
|---|---:|---:|
| Final output | 995,754,634 raw USDC | 995,754,634 raw USDC |
| Total cycle (runtime) | 1.7820 s | 0.5174 s |
| Wall clock (same Windows monitor, includes launch) | 3.5443 s | 0.5809 s |
| Pool discovery | 1.6730 s | 0.5122 s |
| Quote/evaluation | 0.1037 s | 0.0047 s |
| RPC calls | 163 | 94 |
| Cache hits | 0 | 0 |
| Peak RSS (same Windows monitor) | 4,304,896 B | 18,321,408 B |
| CPU process time (same Windows monitor) | 0 s (counter resolution) | 0.015625 s |
| Language allocation metric | 3,041,291 B | 1,303,408 B |

The outputs match exactly, so both workloads used compatible pools and quote
inputs at the same pinned fork state. Go was faster in this measured workload
(about 3.44x runtime-cycle speedup). The OS monitor reported higher Go peak RSS;
no memory advantage is claimed. Python's external CPU time rounded to zero, so
the CPU comparison is captured but not resolution-meaningful. This is a
benchmark result only, not an execution or strategy result.
