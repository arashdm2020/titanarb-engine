// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {MockERC20} from "./MockERC20.sol";
import {IAlgebraSwapRouter} from "../../interfaces/IAlgebraSwapRouter.sol";

/**
 * @notice Minimal mock of Camelot V3 (Algebra Integral) SwapRouter.exactInputSingle for isolated
 *         unit testing. Deliberately has NO `fee` parameter, matching the real Algebra ABI.
 */
contract MockAlgebraSwapRouter {
    // rate[tokenIn][tokenOut] = amountOut per 1e18 amountIn units, scaled 1e18
    mapping(address => mapping(address => uint256)) public rate;

    function setRate(address tokenIn, address tokenOut, uint256 rateScaled1e18) external {
        rate[tokenIn][tokenOut] = rateScaled1e18;
    }

    function exactInputSingle(IAlgebraSwapRouter.ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256 amountOut)
    {
        require(block.timestamp <= params.deadline, "MockAlgebraSwapRouter: expired");
        require(params.amountIn > 0, "MockAlgebraSwapRouter: zero amountIn");

        uint256 r = rate[params.tokenIn][params.tokenOut];
        require(r > 0, "MockAlgebraSwapRouter: no rate configured");

        IERC20(params.tokenIn).transferFrom(msg.sender, address(this), params.amountIn);

        amountOut = (params.amountIn * r) / 1e18;
        require(amountOut >= params.amountOutMinimum, "MockAlgebraSwapRouter: insufficient output");

        MockERC20(params.tokenOut).mint(params.recipient, amountOut);
    }
}
