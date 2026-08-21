// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

/**
 * @title IAlgebraSwapRouter (minimal Algebra Integral / Camelot V3 SwapRouter interface)
 * @notice Verified against the Camelot V3 (Algebra Integral) deployment on Arbitrum One:
 *         SwapRouter: 0x1F721E2E82F6676FCE4eA07A5958cF098D339e18
 *
 * @dev IMPORTANT DIFFERENCES FROM UNISWAP V3's ISwapRouter (confirmed against Algebra Integral's
 *      periphery SwapRouter.sol source and Algebra's official ISwapRouter API docs):
 *        - NO `fee` field. Algebra pools are single-pool-per-token-pair with a dynamic fee
 *          determined by the pool's plugin, so there is no fee-tier selection at the router
 *          level (contrast with Uniswap V3, which requires selecting a fee tier per call).
 *        - The price-limit field is named `limitSqrtPrice`, not `sqrtPriceLimitX96`.
 *        - `deadline` IS present in this struct (unlike Uniswap's SwapRouter02, which removed
 *          it). This matches the original Uniswap V3 SwapRouter's presence of a deadline field.
 */
interface IAlgebraSwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 limitSqrtPrice;
    }

    /// @notice Swaps `amountIn` of one token for as much as possible of another token.
    /// @param params The parameters necessary for the swap, encoded as `ExactInputSingleParams`.
    /// @return amountOut The amount of the received token.
    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}
