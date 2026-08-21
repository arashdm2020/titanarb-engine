// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {MockERC20} from "./MockERC20.sol";
import {ISwapRouter} from "../../interfaces/ISwapRouter.sol";

/**
 * @notice Minimal mock of Uniswap V3 SwapRouter.exactInputSingle for isolated unit testing.
 *         Exchange rate for each (tokenIn, tokenOut) pair is configurable (scaled by 1e18) so
 *         tests can simulate profitable / unprofitable / slippage-violating routes.
 */
contract MockSwapRouter {
    // rate[tokenIn][tokenOut] = amountOut per 1e18 amountIn units, scaled 1e18
    mapping(address => mapping(address => uint256)) public rate;

    function setRate(address tokenIn, address tokenOut, uint256 rateScaled1e18) external {
        rate[tokenIn][tokenOut] = rateScaled1e18;
    }

    function exactInputSingle(ISwapRouter.ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256 amountOut)
    {
        require(block.timestamp <= params.deadline, "MockSwapRouter: expired");
        require(params.amountIn > 0, "MockSwapRouter: zero amountIn");

        uint256 r = rate[params.tokenIn][params.tokenOut];
        require(r > 0, "MockSwapRouter: no rate configured");

        IERC20(params.tokenIn).transferFrom(msg.sender, address(this), params.amountIn);

        amountOut = (params.amountIn * r) / 1e18;
        require(amountOut >= params.amountOutMinimum, "MockSwapRouter: insufficient output");

        MockERC20(params.tokenOut).mint(params.recipient, amountOut);
    }

    function exactInput(ISwapRouter.ExactInputParams calldata) external payable returns (uint256) {
        revert("MockSwapRouter: exactInput not implemented");
    }
}
