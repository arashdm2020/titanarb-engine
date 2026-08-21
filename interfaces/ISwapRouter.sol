// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

/**
 * @title ISwapRouter (minimal Uniswap V3 SwapRouter interface)
 * @notice Verified against Uniswap V3 Polygon deployment:
 *         SwapRouter: 0xE592427A0AEce92De3Edee1F18E0157C05861564
 *         This is the original (v1) SwapRouter which includes a `deadline` field in its
 *         params structs. The newer SwapRouter02 (0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45)
 *         removed the deadline field, so it is intentionally NOT used here because task
 *         requirements mandate an explicit per-swap deadline.
 */
interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    /// @notice Swaps `amountIn` of one token for as much as possible of another token
    /// @param params The parameters necessary for the swap, encoded as `ExactInputSingleParams`
    /// @return amountOut The amount of the received token
    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);

    struct ExactInputParams {
        bytes path;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
    }

    /// @notice Swaps `amountIn` of one token for as much as possible of another along the specified path
    function exactInput(ExactInputParams calldata params) external payable returns (uint256 amountOut);
}
