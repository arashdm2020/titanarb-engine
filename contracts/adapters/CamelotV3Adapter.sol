// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import {IArbitrageAdapter} from "../../interfaces/IArbitrageAdapter.sol";
import {IAlgebraSwapRouter} from "../../interfaces/IAlgebraSwapRouter.sol";

/**
 * @title CamelotV3Adapter
 * @notice Stateless adapter for Camelot V3, which is an Algebra Integral deployment and is NOT
 *         ABI-compatible with Uniswap V3 (verified in Phase 0: no fee-tier parameter, single
 *         dynamic-fee pool per token pair, `limitSqrtPrice` field name instead of
 *         `sqrtPriceLimitX96`). This adapter exists specifically because a shared
 *         Uniswap-V3-shaped call cannot serve Camelot.
 *
 * @dev SECURITY MODEL: identical token-flow contract to {UniswapV3Adapter} -- caller must
 *      pre-approve this contract, tokens are pulled via `safeTransferFrom`, and the underlying
 *      router sends `tokenOut` directly to `msg.sender`. The `data` field is intentionally
 *      unused (Algebra pools require no fee-tier selection) but is still accepted to satisfy the
 *      common {IArbitrageAdapter} interface.
 */
contract CamelotV3Adapter is IArbitrageAdapter {
    using SafeERC20 for IERC20;

    /// @notice Camelot V3 (Algebra Integral) SwapRouter.
    IAlgebraSwapRouter public immutable ROUTER;

    error ZeroAddress();

    constructor(address router) {
        if (router == address(0)) revert ZeroAddress();
        ROUTER = IAlgebraSwapRouter(router);
    }

    /// @inheritdoc IArbitrageAdapter
    function executeSwap(
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 amountOutMinimum,
        uint256 deadline,
        bytes calldata /* data */
    ) external override returns (uint256 amountOut) {
        IERC20(tokenIn).safeTransferFrom(msg.sender, address(this), amountIn);
        IERC20(tokenIn).forceApprove(address(ROUTER), amountIn);

        amountOut = ROUTER.exactInputSingle(
            IAlgebraSwapRouter.ExactInputSingleParams({
                tokenIn: tokenIn,
                tokenOut: tokenOut,
                recipient: msg.sender,
                deadline: deadline,
                amountIn: amountIn,
                amountOutMinimum: amountOutMinimum,
                limitSqrtPrice: 0
            })
        );
    }
}
