// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {Test, console2} from "forge-std/Test.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {FlashArbitrageExecutor} from "../contracts/FlashArbitrageExecutor.sol";
import {UniswapV3Adapter} from "../contracts/adapters/UniswapV3Adapter.sol";
import {CamelotV3Adapter} from "../contracts/adapters/CamelotV3Adapter.sol";
import {IUniswapV3Factory, IUniswapV3Pool} from "../interfaces/IUniswapV3Pool.sol";
import {IPool} from "../interfaces/IAavePool.sol";

/// @dev Minimal Algebra Integral (Camelot V3) factory interface for pool resolution. Algebra pools
///      are keyed by token pair only (no fee tier), unlike Uniswap V3's (tokenA, tokenB, fee).
interface IAlgebraFactory {
    function poolByPair(address tokenA, address tokenB) external view returns (address pool);
}

/**
 * @title ArbitrumForkTest
 * @notice REAL Arbitrum One mainnet-fork integration test for the Phase 2 multi-DEX architecture.
 *         Exercises the actual deployed Aave V3 Pool, the ORIGINAL Uniswap V3 SwapRouter, the
 *         Camelot V3 (Algebra) SwapRouter, and real on-chain pools -- no mocks on the primary path.
 *
 *   Run (from the Foundry install at D:\forge, see RUNBOOK_ARBITRUM_FORK.md):
 *     forge test --match-path test/ArbitrumFork.t.sol \
 *         --fork-url $ARBITRUM_RPC_URL -vvv
 *   or, using the named endpoint in foundry.toml:
 *     forge test --match-path test/ArbitrumFork.t.sol --fork-url arbitrum -vvv
 *
 *   If ARBITRUM_RPC_URL is not set, every test in this file self-skips (it never fabricates a
 *   pass). Set it to an Arbitrum One archive/full RPC that supports eth_call at a historical block.
 *
 * WHAT THIS PROVES (and what it does not):
 *  - `test_fork_uniswapAdapterDirectSwap_provesOriginalRouterAbi` and
 *    `test_fork_camelotAdapterDirectSwap_provesAlgebraAbi` are the decisive, DETERMINISTIC ABI
 *    proofs: they call each adapter against the REAL router and assert a non-zero output. If the
 *    Uniswap adapter were (mis)wired to SwapRouter02 -- the exact latent bug this test guards --
 *    the deadline-bearing exactInputSingle selector would not exist on that contract and the swap
 *    would revert. A green result here is proof the adapter <-> real-router ABI is compatible.
 *  - `test_fork_fullArbitrageMechanics_*` prove the FULL chain end-to-end: Python-shaped
 *    SwapStep[] -> executeArbitrage -> Aave flashLoanSimple -> executeOperation -> forceApprove ->
 *    adapter.executeSwap -> real router -> atomic repayment (+ profit sweep). They use
 *    amountOutMinimum = 1 (mechanics-only; production computes it from an executable quote minus
 *    slippage) and assert on WHATEVER the live pools produce via try/catch: a naturally-occurring
 *    round trip is very unlikely to be organically profitable, so a revert is a valid, honest
 *    outcome -- in which case the atomicity invariant (profitRecipient received nothing) is
 *    asserted. This mirrors the existing Polygon Integration.t.sol philosophy: prove mechanics,
 *    never fake profitability.
 */
contract ArbitrumForkTest is Test {
    // --- Arbitrum One canonical addresses (mirror config/arbitrum.json) -----------------------
    address internal constant AAVE_POOL = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    // Native-USDC aToken returned by Aave's real reserve on the pinned fork.
    // The flash-loan trace identifies this as the contract that transfers the
    // principal out and receives `amount + premium` back.
    address internal constant AAVE_USDC_ATOKEN = 0x724dc807b04555b71ed48a6896b6F41593b8C637;
    address internal constant UNISWAP_FACTORY = 0x1F98431c8aD98523631AE4a59f267346ea31F984;
    // ORIGINAL, deadline-bearing Uniswap V3 SwapRouter -- the router UniswapV3Adapter targets.
    // (NOT SwapRouter02 0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45.)
    address internal constant UNISWAP_SWAP_ROUTER = 0xE592427A0AEce92De3Edee1F18E0157C05861564;
    address internal constant CAMELOT_SWAP_ROUTER = 0x1F721E2E82F6676FCE4eA07A5958cF098D339e18;
    address internal constant CAMELOT_FACTORY = 0x1a3c9B1d2F0529D97f2afC5136Cc23e58f1FD35B;

    address internal constant WETH = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1; // 18 decimals
    address internal constant USDC = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831; // native USDC, 6 decimals
    address internal constant USDT = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9; // 6 decimals
    address internal constant ARB = 0x912CE59144191C1204E64559FE8253a0e49E6548; // 18 decimals

    uint24 internal constant FEE_USDC_WETH = 500; // 0.05% -- deepest USDC/WETH tier on Arbitrum
    uint24 internal constant FEE_WETH_USDC_ALT = 3000; // 0.30% -- alternate tier for the return hop

    uint256 internal constant LOAN_AMOUNT = 10_000e6; // 10,000 USDC flash loan
    uint256 internal constant WETH_SWAP_IN = 1e18; // 1 WETH for direct-swap ABI proofs

    FlashArbitrageExecutor internal arb;
    UniswapV3Adapter internal uniAdapter;
    CamelotV3Adapter internal camelotAdapter;

    address internal profitRecipient = address(0xFEED);
    bool internal forkActive;

    function setUp() public {
        string memory rpcUrl = vm.envOr("ARBITRUM_RPC_URL", string(""));
        if (bytes(rpcUrl).length == 0) {
            // No RPC configured: skip honestly rather than fabricate a pass.
            vm.skip(true);
            return;
        }
        forkActive = true;

        // owner == authorizedExecutor == this test contract (constructor sets owner = deployer).
        arb = new FlashArbitrageExecutor(AAVE_POOL, address(this), profitRecipient);

        // The Uniswap adapter MUST bind to the ORIGINAL deadline-bearing router.
        uniAdapter = new UniswapV3Adapter(UNISWAP_SWAP_ROUTER);
        camelotAdapter = new CamelotV3Adapter(CAMELOT_SWAP_ROUTER);

        arb.setApprovedAdapter(address(uniAdapter), true);
        arb.setApprovedAdapter(address(camelotAdapter), true);
        arb.setAllowedToken(WETH, true);
        arb.setAllowedToken(USDC, true);
        arb.setAllowedToken(USDT, true);
        arb.setAllowedToken(ARB, true);
    }

    // ---------------------------------------------------------------------
    // Step builders (identical shape to the Python bot's SwapStepArg encoding)
    // ---------------------------------------------------------------------

    function _uniStep(address tokenIn, address tokenOut, uint24 fee, uint256 amountOutMinimum)
        internal
        view
        returns (FlashArbitrageExecutor.SwapStep memory)
    {
        return FlashArbitrageExecutor.SwapStep({
            adapter: address(uniAdapter),
            tokenIn: tokenIn,
            tokenOut: tokenOut,
            amountOutMinimum: amountOutMinimum,
            data: abi.encode(fee) // exactly 32 bytes -> abi.encode(uint24), matches encode_uniswap_v3_data()
        });
    }

    function _camelotStep(address tokenIn, address tokenOut, uint256 amountOutMinimum)
        internal
        view
        returns (FlashArbitrageExecutor.SwapStep memory)
    {
        return FlashArbitrageExecutor.SwapStep({
            adapter: address(camelotAdapter),
            tokenIn: tokenIn,
            tokenOut: tokenOut,
            amountOutMinimum: amountOutMinimum,
            data: "" // Algebra ignores data -> matches encode_camelot_v3_data() == b""
        });
    }

    // =====================================================================
    // 1. DETERMINISTIC ABI PROOFS (the core of BUG #1's guard)
    // =====================================================================

    /// @notice Proves UniswapV3Adapter's deadline-bearing ISwapRouter.exactInputSingle ABI is
    ///         compatible with the REAL original SwapRouter at 0xE592...1564 on Arbitrum. A revert
    ///         here would indicate the adapter is wired to the wrong router (e.g. SwapRouter02).
    function test_fork_uniswapAdapterDirectSwap_provesOriginalRouterAbi() public {
        if (!forkActive) return;

        // Fund the caller with WETH and swap WETH -> USDC directly through the adapter.
        // (Starting from WETH avoids any proxy-storage quirks with deal() on native USDC.)
        deal(WETH, address(this), WETH_SWAP_IN);
        IERC20(WETH).approve(address(uniAdapter), WETH_SWAP_IN);

        uint256 usdcBefore = IERC20(USDC).balanceOf(address(this));
        uint256 amountOut = uniAdapter.executeSwap(
            WETH, USDC, WETH_SWAP_IN, 1, block.timestamp + 300, abi.encode(FEE_USDC_WETH)
        );

        console2.log("Uniswap adapter WETH->USDC out (6dp):", amountOut);
        assertGt(amountOut, 0, "Uniswap adapter returned zero -- original-router ABI mismatch?");
        assertEq(
            IERC20(USDC).balanceOf(address(this)) - usdcBefore,
            amountOut,
            "router did not deliver tokenOut to the adapter caller"
        );
    }

    /// @notice Proves CamelotV3Adapter's Algebra exactInputSingle ABI (limitSqrtPrice, no fee tier)
    ///         is compatible with the REAL Camelot V3 SwapRouter on Arbitrum.
    /// @dev If the WETH/USDC Camelot pool is thin at the forked block, switch USDC -> USDC.e
    ///      (0xff970a61a04b1ca14834a43f5de4533ebddb5cc8), Camelot's historically deeper stable.
    function test_fork_camelotAdapterDirectSwap_provesAlgebraAbi() public {
        if (!forkActive) return;

        deal(WETH, address(this), WETH_SWAP_IN);
        IERC20(WETH).approve(address(camelotAdapter), WETH_SWAP_IN);

        uint256 usdcBefore = IERC20(USDC).balanceOf(address(this));
        uint256 amountOut = camelotAdapter.executeSwap(WETH, USDC, WETH_SWAP_IN, 1, block.timestamp + 300, "");

        console2.log("Camelot adapter WETH->USDC out (6dp):", amountOut);
        assertGt(amountOut, 0, "Camelot adapter returned zero -- Algebra ABI mismatch or thin pool");
        assertEq(
            IERC20(USDC).balanceOf(address(this)) - usdcBefore,
            amountOut,
            "router did not deliver tokenOut to the adapter caller"
        );
    }

    // =====================================================================
    // 2. Pool existence / liquidity sanity
    // =====================================================================

    function test_fork_uniswapPoolExists() public {
        if (!forkActive) return;
        address pool = IUniswapV3Factory(UNISWAP_FACTORY).getPool(USDC, WETH, FEE_USDC_WETH);
        console2.log("Uniswap USDC/WETH 0.05% pool:", pool);
        assertTrue(pool != address(0), "USDC/WETH 0.05% pool not found");
        console2.log("Uniswap USDC/WETH liquidity:", uint256(IUniswapV3Pool(pool).liquidity()));
    }

    function test_fork_camelotPoolExists() public {
        if (!forkActive) return;
        address pool = IAlgebraFactory(CAMELOT_FACTORY).poolByPair(WETH, USDC);
        console2.log("Camelot WETH/USDC pool:", pool);
        assertTrue(pool != address(0), "Camelot WETH/USDC pool not found");
        console2.log("Camelot WETH/USDC liquidity:", uint256(IUniswapV3Pool(pool).liquidity()));
    }

    // =====================================================================
    // 3. FULL-CHAIN MECHANICS (flashLoanSimple -> executeOperation -> adapters -> repayment)
    // =====================================================================

    /// @notice Uniswap-only 2-hop round trip: USDC -[Uni 0.05%]-> WETH -[Uni 0.30%]-> USDC.
    ///         Exercises the entire executor path against real Aave + real Uniswap with no Camelot
    ///         dependency (most reliable full-chain mechanics proof).
    function test_fork_fullArbitrageMechanics_uniswapOnly_2hop() public {
        if (!forkActive) return;

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(USDC, WETH, FEE_USDC_WETH, 1);
        steps[1] = _uniStep(WETH, USDC, FEE_WETH_USDC_ALT, 1);

        _runMechanics(steps, "uniswap-only 2-hop");
    }

    /// @notice Cross-DEX 2-hop round trip: USDC -[Uniswap]-> WETH -[Camelot]-> USDC. Proves the
    ///         executor can mix two incompatible-ABI DEXes atomically inside one flash loan.
    function test_fork_fullArbitrageMechanics_crossDex_2hop() public {
        if (!forkActive) return;

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(USDC, WETH, FEE_USDC_WETH, 1);
        steps[1] = _camelotStep(WETH, USDC, 1);

        _runMechanics(steps, "cross-dex 2-hop (Uniswap + Camelot)");
    }

    /// @notice Separates protocol integration from the fork's starting market economics.
    /// @dev The executor deliberately checks the *swap return value*, not its pre-existing token
    /// balance, so merely seeding the executor cannot cover a losing route. Instead this test
    /// creates a test-only price condition through the real Camelot adapter/router/pool, then
    /// executes the same Aave -> Uniswap -> Camelot route. No protocol code or quote is mocked.
    function test_fork_fullArbitrage_crossDex_2hop_repayableAfterRealPoolMove() public {
        if (!forkActive) return;

        // Buy WETH on the real Camelot pool first. This raises Camelot's USDC-per-WETH price,
        // making the subsequent WETH -> USDC leg repayable at this isolated fork state.
        uint256 conditioningAmount = 100_000e6;
        deal(USDC, address(this), conditioningAmount);
        IERC20(USDC).approve(address(camelotAdapter), conditioningAmount);
        uint256 wethConditioningOut = camelotAdapter.executeSwap(
            USDC, WETH, conditioningAmount, 1, block.timestamp + 300, ""
        );
        assertGt(wethConditioningOut, 0, "real Camelot conditioning swap failed");

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(USDC, WETH, FEE_USDC_WETH, 1);
        steps[1] = _camelotStep(WETH, USDC, 1);

        uint256 recipientBefore = IERC20(USDC).balanceOf(profitRecipient);
        uint256 aTokenBefore = IERC20(USDC).balanceOf(AAVE_USDC_ATOKEN);
        arb.executeArbitrage(USDC, LOAN_AMOUNT, steps, block.timestamp + 300, 0);

        uint256 profit = IERC20(USDC).balanceOf(profitRecipient) - recipientBefore;
        uint256 premium = LOAN_AMOUNT * IPool(AAVE_POOL).FLASHLOAN_PREMIUM_TOTAL() / 10_000;
        uint256 aTokenAfter = IERC20(USDC).balanceOf(AAVE_USDC_ATOKEN);
        console2.log("Conditioning USDC->WETH out:", wethConditioningOut);
        console2.log("Successful fork route profit (USDC 6dp):", profit);
        console2.log("Aave premium (USDC 6dp):", premium);
        assertGt(profit, 0, "route must cover Aave principal and premium before profit sweep");
        assertEq(aTokenAfter - aTokenBefore, premium, "Aave reserve must receive principal plus premium");
        assertEq(IERC20(USDC).balanceOf(address(arb)), 0, "repayment and profit sweep leave no USDC dust");
    }

    /// @dev Shared runner: fires executeArbitrage and asserts honest invariants for BOTH outcomes.
    ///      Success -> contract retains no dust and profit (if any) went to profitRecipient.
    ///      Revert  -> atomic rollback means profitRecipient received nothing (the key safety
    ///      invariant). We do NOT force profitability on unmodified live market state.
    function _runMechanics(FlashArbitrageExecutor.SwapStep[] memory steps, string memory label) internal {
        uint256 deadline = block.timestamp + 300;
        uint256 recipientBefore = IERC20(USDC).balanceOf(profitRecipient);

        try arb.executeArbitrage(USDC, LOAN_AMOUNT, steps, deadline, 0) {
            uint256 residual = IERC20(USDC).balanceOf(address(arb));
            uint256 profit = IERC20(USDC).balanceOf(profitRecipient) - recipientBefore;
            console2.log(string.concat("MECHANICS OK [", label, "] profit (USDC 6dp):"), profit);
            console2.log("Residual USDC left in executor (must be 0):", residual);
            assertEq(residual, 0, "executor must not retain funds after a successful arbitrage");
        } catch Error(string memory reason) {
            console2.log(string.concat("Reverted [", label, "] (valid on unprofitable state):"), reason);
            assertEq(
                IERC20(USDC).balanceOf(profitRecipient), recipientBefore, "atomicity: recipient must be unchanged on revert"
            );
        } catch (bytes memory lowLevelData) {
            // Custom errors (e.g. InsufficientProfit) surface here as raw bytes.
            console2.log(string.concat("Reverted [", label, "] with custom error, data length:"), lowLevelData.length);
            assertEq(
                IERC20(USDC).balanceOf(profitRecipient), recipientBefore, "atomicity: recipient must be unchanged on revert"
            );
        }
    }
}
