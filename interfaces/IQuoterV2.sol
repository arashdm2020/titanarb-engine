// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

/**
 * @title IQuoterV2 (minimal Uniswap V3 QuoterV2 interface)
 * @notice Verified against Uniswap V3 Polygon deployment:
 *         QuoterV2: 0x61fFE014bA17989E743c5F6cB21bF9697530B21e
 * @dev QuoterV2 functions are NOT `view` on-chain (they rely on state-changing calls that get
 *      reverted), but are intended to be invoked off-chain via `eth_call`. They are declared
 *      here without the `view` modifier to match the real deployed ABI. This interface is used
 *      primarily by the off-chain Python bot (through web3.py) rather than on-chain by the
 *      arbitrage contract, since on-chain quoting during execution would waste gas.
 */
interface IQuoterV2 {
    struct QuoteExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint256 amountIn;
        uint24 fee;
        uint160 sqrtPriceLimitX96;
    }

    function quoteExactInputSingle(QuoteExactInputSingleParams memory params)
        external
        returns (
            uint256 amountOut,
            uint160 sqrtPriceX96After,
            uint32 initializedTicksCrossed,
            uint256 gasEstimate
        );

    function quoteExactInput(bytes memory path, uint256 amountIn)
        external
        returns (
            uint256 amountOut,
            uint160[] memory sqrtPriceX96AfterList,
            uint32[] memory initializedTicksCrossedList,
            uint256 gasEstimate
        );
}
