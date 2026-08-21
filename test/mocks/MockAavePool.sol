// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {MockERC20} from "./MockERC20.sol";
import {IFlashLoanSimpleReceiver} from "../../interfaces/IAavePool.sol";

/**
 * @notice Minimal mock of Aave V3 Pool.flashLoanSimple for isolated unit testing.
 *         Mints the requested liquidity to itself on demand (simulating a funded reserve) and
 *         enforces repayment of amount + premium exactly like the real Aave Pool.
 */
contract MockAavePool {
    uint128 public premiumBps = 5; // 0.05% default, matches AAVE_FEE_BPS default

    function setPremiumBps(uint128 bps) external {
        premiumBps = bps;
    }

    function FLASHLOAN_PREMIUM_TOTAL() external view returns (uint128) {
        return premiumBps;
    }

    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 /* referralCode */
    ) external {
        // Simulate a funded reserve by minting liquidity directly to this pool.
        MockERC20(asset).mint(address(this), amount);
        IERC20(asset).transfer(receiverAddress, amount);

        uint256 premium = (amount * premiumBps) / 10000;

        bool success = IFlashLoanSimpleReceiver(receiverAddress).executeOperation(
            asset, amount, premium, msg.sender, params
        );
        require(success, "executeOperation failed");

        uint256 repayment = amount + premium;
        IERC20(asset).transferFrom(receiverAddress, address(this), repayment);
    }
}
