// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import {IArbitrageAdapter} from "../../interfaces/IArbitrageAdapter.sol";
import {ISwapRouter} from "../../interfaces/ISwapRouter.sol";

/**
 * @title UniswapV3Adapter
 * @notice Stateless adapter that lets FlashArbitrageExecutor execute a single-hop swap against
 *         Uniswap V3 (or any fork exposing an identical SwapRouter ABI, e.g. QuickSwap V3 on
 *         Polygon) without the executor ever holding a direct reference to a swap router.
 *
 * @dev SECURITY MODEL
 *      - This contract is immutable (router set once at construction) and holds no persistent
 *        user funds between calls.
 *      - `data` must decode to exactly one `uint24` fee tier; malformed data reverts via
 *        `abi.decode`, it is never forwarded as raw calldata to an external target.
 *      - Token flow: caller (executor) must have approved this contract for `amountIn` of
 *        `tokenIn` BEFORE calling {executeSwap}; this contract pulls the tokens, swaps, and the
 *        underlying router sends `tokenOut` directly to `msg.sender` (recipient = caller).
 */
contract UniswapV3Adapter is IArbitrageAdapter {
    using SafeERC20 for IERC20;

    /// @notice Uniswap V3 SwapRouter (the original, deadline-bearing router -- NOT SwapRouter02).
    ISwapRouter public immutable ROUTER;

    error ZeroAddress();
    error InvalidFeeData();

    constructor(address router) {
        if (router == address(0)) revert ZeroAddress();
        ROUTER = ISwapRouter(router);
    }

    /// @inheritdoc IArbitrageAdapter
    function executeSwap(
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 amountOutMinimum,
        uint256 deadline,
        bytes calldata data
    ) external override returns (uint256 amountOut) {
        if (data.length != 32) revert InvalidFeeData();
        uint24 fee = abi.decode(data, (uint24));

        IERC20(tokenIn).safeTransferFrom(msg.sender, address(this), amountIn);
        IERC20(tokenIn).forceApprove(address(ROUTER), amountIn);

        amountOut = ROUTER.exactInputSingle(
            ISwapRouter.ExactInputSingleParams({
                tokenIn: tokenIn,
                tokenOut: tokenOut,
                fee: fee,
                recipient: msg.sender,
                deadline: deadline,
                amountIn: amountIn,
                amountOutMinimum: amountOutMinimum,
                sqrtPriceLimitX96: 0
            })
        );
    }
}
