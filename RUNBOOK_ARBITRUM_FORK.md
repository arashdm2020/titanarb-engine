# Arbitrum One Fork — Phase 2 Integration Runbook

This runbook lists the exact commands to (a) run the Phase 2 Arbitrum fork integration
tests and (b) deploy the Phase 2 contracts to an Arbitrum fork/mainnet. Foundry is installed
on this machine at **`D:\forge`** (forge, anvil, cast, chisel).

> These commands must be run by you in a Windows shell (PowerShell/cmd). The assistant that
> authored these files runs in a Linux sandbox with no access to `D:\forge` and cannot execute
> Windows binaries, so **execution results below are UNVERIFIED until you run them and paste the
> output.** Nothing here has been run for you.

---

## 0. One-time: make the D:\forge tools callable

Either add `D:\forge` to your `PATH`, or call the binaries by full path. Examples below use the
bare names; prefix with `D:\forge\` if they are not on your PATH:

```powershell
# Verify the toolchain is reachable
D:\forge\forge --version
D:\forge\anvil --version
D:\forge\cast --version
```

Work from the repo root:

```powershell
cd D:\TitanArb\polygon-triangular-arbitrage
```

## 1. Provide an Arbitrum One RPC

The fork tests self-skip (they do NOT fake a pass) unless `ARBITRUM_RPC_URL` is set to an
Arbitrum One endpoint that supports historical `eth_call` (an archive/full node — Alchemy,
Infura, QuickNode, or your own node). Set it for the session:

```powershell
$env:ARBITRUM_RPC_URL = "https://arb-mainnet.g.alchemy.com/v2/<YOUR_KEY>"
```

`config/arbitrum.json` also lists the public endpoint `https://arb1.arbitrum.io/rpc`, but public
endpoints frequently rate-limit or reject fork `eth_call` at historical blocks. If a run fails to
fork, switch to a dedicated provider before concluding the validation is impossible.

## 2. Run the fork integration tests

```powershell
# All Phase 2 fork tests (uses the ARBITRUM_RPC_URL env var above)
D:\forge\forge test --match-path test/ArbitrumFork.t.sol --fork-url $env:ARBITRUM_RPC_URL -vvv

# Equivalent, using the named endpoint added to foundry.toml [rpc_endpoints]:
D:\forge\forge test --match-path test/ArbitrumFork.t.sol --fork-url arbitrum -vvv
```

What each test proves:

| Test | Proves |
|------|--------|
| `test_fork_uniswapAdapterDirectSwap_provesOriginalRouterAbi` | Adapter's deadline-bearing `exactInputSingle` matches the REAL original SwapRouter `0xE592…1564`. Reverts if wired to SwapRouter02. |
| `test_fork_camelotAdapterDirectSwap_provesAlgebraAbi` | Adapter's Algebra `exactInputSingle` (limitSqrtPrice, no fee) matches the REAL Camelot router. |
| `test_fork_uniswapPoolExists` / `test_fork_camelotPoolExists` | Target pools exist and report liquidity at the forked block. |
| `test_fork_fullArbitrageMechanics_uniswapOnly_2hop` | Full chain: flashLoanSimple → executeOperation → adapter → real router → atomic repayment (Uniswap only). |
| `test_fork_fullArbitrageMechanics_crossDex_2hop` | Same, mixing Uniswap + Camelot in one atomic flash loan. |

The two `fullArbitrageMechanics` tests use `amountOutMinimum = 1` for **mechanics only** and accept
either outcome honestly: a naturally-occurring round trip is usually NOT profitable, so a revert
is a valid result — the test then asserts the atomicity invariant (profit recipient unchanged).
They do not fabricate profit.

To pin a specific block for reproducibility, add `--fork-block-number <N>`.

## 3. Run the mock unit tests + Python suite (no RPC needed)

```powershell
# Solidity unit tests (mocks; fast)
D:\forge\forge test --match-path test/FlashArbitrageExecutor.t.sol -vv

# Python bot tests (from the project venv)
D:\TitanArb\polygon-triangular-arbitrage\.venv\Scripts\python -m pytest bot/tests -q
```

## 4. Deploy Phase 2 to Arbitrum (fork first, then mainnet only when ready)

`script/DeployArbitrum.s.sol` deploys `FlashArbitrageExecutor` + both adapters and wires them.
Set these env vars first (addresses from `config/arbitrum.json`):

```powershell
$env:PRIVATE_KEY         = "0x..."                                           # deployer key — NEVER commit
$env:AAVE_POOL           = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
$env:EXECUTOR            = "0x..."                                           # bot signer (can be deployer)
$env:PROFIT_RECIPIENT    = "0x..."                                           # must be non-zero
$env:UNISWAP_SWAP_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"     # ORIGINAL router — NOT SwapRouter02
$env:CAMELOT_SWAP_ROUTER = "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18"
$env:TOKEN_WETH          = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
$env:TOKEN_USDC          = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
$env:TOKEN_USDT          = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"     # optional
$env:TOKEN_ARB           = "0x912CE59144191C1204E64559FE8253a0e49E6548"     # optional
```

Dry-run against a local fork (recommended first — no broadcast):

```powershell
# Terminal A: start a local Arbitrum fork
D:\forge\anvil --fork-url $env:ARBITRUM_RPC_URL

# Terminal B: simulate the deploy against the local fork (no --broadcast = simulation)
D:\forge\forge script script/DeployArbitrum.s.sol:DeployArbitrum --rpc-url http://127.0.0.1:8545 -vvvv
```

Only when fully validated, broadcast to mainnet (spends real gas — **do NOT do this as part of
Phase 2 validation**, per the migration rules):

```powershell
D:\forge\forge script script/DeployArbitrum.s.sol:DeployArbitrum --rpc-url $env:ARBITRUM_RPC_URL --broadcast --verify -vvvv
```

After deploy, copy the logged `FLASH_EXECUTOR_ADDRESS`, `UNISWAP_V3_ADAPTER`, `CAMELOT_V3_ADAPTER`
into your `.env`.

## 5. WSS connectivity (separately unverified)

`bot/provider.py` implements a real WebSocket provider with reconnect/backoff, but live WSS
reachability of `wss://arb1.arbitrum.io/ws` cannot be exercised from a fork. Verify it directly:

```powershell
D:\forge\cast block-number --rpc-url wss://arb1.arbitrum.io/ws
```

A returned block number confirms the WSS endpoint is reachable. Until you run this, WSS is
**UNVERIFIED**.
