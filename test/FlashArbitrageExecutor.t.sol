// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {Test} from "forge-std/Test.sol";
import {FlashArbitrageExecutor} from "../contracts/FlashArbitrageExecutor.sol";
import {UniswapV3Adapter} from "../contracts/adapters/UniswapV3Adapter.sol";
import {CamelotV3Adapter} from "../contracts/adapters/CamelotV3Adapter.sol";
import {MockERC20} from "./mocks/MockERC20.sol";
import {MockAavePool} from "./mocks/MockAavePool.sol";
import {MockSwapRouter} from "./mocks/MockSwapRouter.sol";
import {MockAlgebraSwapRouter} from "./mocks/MockAlgebraSwapRouter.sol";

/**
 * @title FlashArbitrageExecutorTest
 * @notice Isolated unit tests using mocked Aave Pool + Uniswap SwapRouter + Algebra SwapRouter.
 *         Exercises 2/3/4-hop routes across both adapter types, plus every validation/security
 *         invariant required for Phase 1.
 */
contract FlashArbitrageExecutorTest is Test {
    FlashArbitrageExecutor internal arb;
    MockAavePool internal aave;
    MockSwapRouter internal uniRouter;
    MockAlgebraSwapRouter internal algebraRouter;
    UniswapV3Adapter internal uniAdapter;
    CamelotV3Adapter internal camelotAdapter;

    MockERC20 internal usdc; // 6 decimals
    MockERC20 internal weth; // 18 decimals
    MockERC20 internal arbTok; // 18 decimals
    MockERC20 internal usdt; // 6 decimals

    address internal owner = address(this);
    address internal executor = address(0xE0E);
    address internal stranger = address(0xBAD);
    address internal profitRecipient = address(0xBEEF);

    uint256 internal constant LOAN_AMOUNT = 10_000e6; // 10,000 USDC

    function setUp() public {
        aave = new MockAavePool();
        uniRouter = new MockSwapRouter();
        algebraRouter = new MockAlgebraSwapRouter();

        uniAdapter = new UniswapV3Adapter(address(uniRouter));
        camelotAdapter = new CamelotV3Adapter(address(algebraRouter));

        usdc = new MockERC20("USD Coin", "USDC", 6);
        weth = new MockERC20("Wrapped Ether", "WETH", 18);
        arbTok = new MockERC20("Arbitrum", "ARB", 18);
        usdt = new MockERC20("Tether USD", "USDT", 6);

        arb = new FlashArbitrageExecutor(address(aave), executor, profitRecipient);
        arb.setApprovedAdapter(address(uniAdapter), true);
        arb.setApprovedAdapter(address(camelotAdapter), true);
        arb.setAllowedToken(address(usdc), true);
        arb.setAllowedToken(address(weth), true);
        arb.setAllowedToken(address(arbTok), true);
        arb.setAllowedToken(address(usdt), true);
    }

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
            data: abi.encode(fee)
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
            data: ""
        });
    }

    // ---------------------------------------------------------------------
    // 2-hop: USDC -[Uniswap]-> WETH -[Camelot]-> USDC
    // ---------------------------------------------------------------------

    function test_2hop_crossDex_success() public {
        uniRouter.setRate(address(usdc), address(weth), (uint256(4.1e18) * 1e18) / LOAN_AMOUNT); // ~4.1 WETH out
        algebraRouter.setRate(address(weth), address(usdc), (uint256(10_100e6) * 1e18) / 4.1e18); // back to USDC w/ profit

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(usdc), 1);

        vm.prank(executor);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);

        assertEq(usdc.balanceOf(address(arb)), 0, "contract must not retain profit");
        assertGt(usdc.balanceOf(profitRecipient), 0, "profit must be swept to recipient");
    }

    // ---------------------------------------------------------------------
    // 3-hop: USDC -[Uniswap]-> WETH -[Camelot]-> ARB -[Uniswap]-> USDC
    // ---------------------------------------------------------------------

    function test_3hop_success() public {
        uniRouter.setRate(address(usdc), address(weth), (uint256(4.1e18) * 1e18) / LOAN_AMOUNT);
        algebraRouter.setRate(address(weth), address(arbTok), (uint256(4000e18) * 1e18) / 4.1e18);
        uniRouter.setRate(address(arbTok), address(usdc), (uint256(10_150e6) * 1e18) / 4000e18);

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](3);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(arbTok), 1);
        steps[2] = _uniStep(address(arbTok), address(usdc), 3000, 1);

        vm.prank(executor);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);

        assertEq(usdc.balanceOf(address(arb)), 0);
        assertGt(usdc.balanceOf(profitRecipient), 0);
    }

    // ---------------------------------------------------------------------
    // 4-hop: USDC -[Uniswap]-> WETH -[Camelot]-> ARB -[Uniswap]-> USDT -[Camelot]-> USDC
    // ---------------------------------------------------------------------

    function test_4hop_success() public {
        uniRouter.setRate(address(usdc), address(weth), (uint256(4.1e18) * 1e18) / LOAN_AMOUNT);
        algebraRouter.setRate(address(weth), address(arbTok), (uint256(4000e18) * 1e18) / 4.1e18);
        uniRouter.setRate(address(arbTok), address(usdt), (uint256(10_120e6) * 1e18) / 4000e18);
        algebraRouter.setRate(address(usdt), address(usdc), (uint256(10_180e6) * 1e18) / 10_120e6);

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](4);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(arbTok), 1);
        steps[2] = _uniStep(address(arbTok), address(usdt), 3000, 1);
        steps[3] = _camelotStep(address(usdt), address(usdc), 1);

        vm.prank(executor);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);

        assertEq(usdc.balanceOf(address(arb)), 0);
        assertGt(usdc.balanceOf(profitRecipient), 0);
    }

    // ---------------------------------------------------------------------
    // Hop-count bounds
    // ---------------------------------------------------------------------

    function test_revertsForSingleHopRoute_belowMinHops() public {
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](1);
        steps[0] = _uniStep(address(usdc), address(usdc), 500, 1);
        vm.expectRevert(FlashArbitrageExecutor.InvalidHopCount.selector);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    function test_revertsForFiveHopRoute_aboveMaxHops() public {
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](5);
        for (uint256 i = 0; i < 5; i++) {
            steps[i] = _uniStep(address(usdc), address(usdc), 500, 1);
        }
        vm.expectRevert(FlashArbitrageExecutor.InvalidHopCount.selector);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    // ---------------------------------------------------------------------
    // Authorization
    // ---------------------------------------------------------------------

    function test_executeArbitrage_revertsForUnauthorizedCaller() public {
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(usdc), 1);

        vm.prank(stranger);
        vm.expectRevert(FlashArbitrageExecutor.Unauthorized.selector);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    // ---------------------------------------------------------------------
    // Invalid adapter
    // ---------------------------------------------------------------------

    function test_revertsForUnapprovedAdapter() public {
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(usdc), 1);
        steps[1].adapter = address(0xDEAD);

        vm.expectRevert(FlashArbitrageExecutor.InvalidAdapter.selector);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    // ---------------------------------------------------------------------
    // Invalid token sequence / route
    // ---------------------------------------------------------------------

    function test_revertsForInvalidTokenSequence() public {
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(arbTok), address(usdc), 1); // discontinuity: weth != arbTok

        vm.expectRevert(FlashArbitrageExecutor.InvalidRoute.selector);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    function test_revertsForRouteNotReturningToBorrowedAsset() public {
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(arbTok), 1); // does not return to usdc

        vm.expectRevert(FlashArbitrageExecutor.InvalidRoute.selector);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    function test_revertsForDisallowedAsset() public {
        MockERC20 rogue = new MockERC20("Rogue", "RGE", 18);
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(rogue), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(rogue), 1);

        vm.expectRevert(FlashArbitrageExecutor.InvalidAsset.selector);
        arb.executeArbitrage(address(rogue), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    // ---------------------------------------------------------------------
    // Insufficient profit
    // ---------------------------------------------------------------------

    function test_revertsForInsufficientProfit() public {
        // Exact break-even rates (no room for the requested minProfit).
        uniRouter.setRate(address(usdc), address(weth), (uint256(4e18) * 1e18) / LOAN_AMOUNT);
        uint256 premium = (LOAN_AMOUNT * aave.premiumBps()) / 10_000;
        algebraRouter.setRate(address(weth), address(usdc), (uint256(LOAN_AMOUNT + premium) * 1e18) / 4e18);

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(usdc), 1);

        vm.expectRevert();
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 100e6); // demand 100 USDC profit
    }

    // ---------------------------------------------------------------------
    // Slippage failure
    // ---------------------------------------------------------------------

    function test_slippageFailure_revertsWholeArbitrage() public {
        uniRouter.setRate(address(usdc), address(weth), 1); // effectively ~0 output
        algebraRouter.setRate(address(weth), address(usdc), 1e18);

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1e18); // demand far more than rate produces
        steps[1] = _camelotStep(address(weth), address(usdc), 1);

        vm.expectRevert();
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    // ---------------------------------------------------------------------
    // Expired deadline
    // ---------------------------------------------------------------------

    function test_executeArbitrage_revertsForExpiredDeadline() public {
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(usdc), 1);

        vm.warp(1_000_000);
        vm.expectRevert(FlashArbitrageExecutor.Expired.selector);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp - 1, 0);
    }

    // ---------------------------------------------------------------------
    // Failed repayment (final balance below required repayment)
    // ---------------------------------------------------------------------

    function test_insufficientRepayment_reverts() public {
        // Rates that return exactly the borrowed amount (no premium coverage) -> must revert.
        uniRouter.setRate(address(usdc), address(weth), 1e30); // 10_000e6 -> 10_000e18
        algebraRouter.setRate(address(weth), address(usdc), 1e6); // 10_000e18 -> 10_000e6 (no premium covered)

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(usdc), 1);

        vm.expectRevert();
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }

    // ---------------------------------------------------------------------
    // executeOperation caller guard
    // ---------------------------------------------------------------------

    function test_executeOperation_revertsIfCallerIsNotAavePool() public {
        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(usdc), 1);

        FlashArbitrageExecutor.ArbParams memory p =
            FlashArbitrageExecutor.ArbParams({steps: steps, deadline: block.timestamp + 60, minProfit: 0});
        bytes memory params = abi.encode(p);

        vm.expectRevert(FlashArbitrageExecutor.InvalidCaller.selector);
        arb.executeOperation(address(usdc), LOAN_AMOUNT, 5e6, address(arb), params);
    }

    // ---------------------------------------------------------------------
    // Unauthorized administrative operations
    // ---------------------------------------------------------------------

    function test_setApprovedAdapter_onlyOwner() public {
        vm.prank(stranger);
        vm.expectRevert();
        arb.setApprovedAdapter(address(0xCAFE), true);

        arb.setApprovedAdapter(address(0xCAFE), true);
        assertTrue(arb.approvedAdapters(address(0xCAFE)));
    }

    function test_setAuthorizedExecutor_onlyOwner() public {
        vm.prank(stranger);
        vm.expectRevert();
        arb.setAuthorizedExecutor(stranger);

        arb.setAuthorizedExecutor(stranger);
        assertEq(arb.authorizedExecutor(), stranger);
    }

    function test_setProfitRecipient_onlyOwner() public {
        vm.prank(stranger);
        vm.expectRevert();
        arb.setProfitRecipient(address(0xCAFE));

        arb.setProfitRecipient(address(0xCAFE));
        assertEq(arb.profitRecipient(), address(0xCAFE));
    }

    function test_setProfitRecipient_revertsForZeroAddress() public {
        vm.expectRevert(FlashArbitrageExecutor.ZeroAddress.selector);
        arb.setProfitRecipient(address(0));
    }

    function test_constructor_revertsForZeroProfitRecipient() public {
        vm.expectRevert(FlashArbitrageExecutor.ZeroAddress.selector);
        new FlashArbitrageExecutor(address(aave), executor, address(0));
    }

    function test_withdrawToken_onlyOwner() public {
        usdc.mint(address(arb), 1_000e6);

        vm.prank(stranger);
        vm.expectRevert();
        arb.withdrawToken(address(usdc), 1_000e6);

        uint256 before = usdc.balanceOf(owner);
        arb.withdrawToken(address(usdc), 1_000e6);
        assertEq(usdc.balanceOf(owner) - before, 1_000e6);
    }

    // ---------------------------------------------------------------------
    // Reentrancy guard resets correctly across repeated successful calls
    // ---------------------------------------------------------------------

    function test_reentrancyGuard_resetsAfterSuccess() public {
        uniRouter.setRate(address(usdc), address(weth), (uint256(4.1e18) * 1e18) / LOAN_AMOUNT);
        algebraRouter.setRate(address(weth), address(usdc), (uint256(10_100e6) * 1e18) / 4.1e18);

        FlashArbitrageExecutor.SwapStep[] memory steps = new FlashArbitrageExecutor.SwapStep[](2);
        steps[0] = _uniStep(address(usdc), address(weth), 500, 1);
        steps[1] = _camelotStep(address(weth), address(usdc), 1);

        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
        arb.executeArbitrage(address(usdc), LOAN_AMOUNT, steps, block.timestamp + 60, 0);
    }
}
