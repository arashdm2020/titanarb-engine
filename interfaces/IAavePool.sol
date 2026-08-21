// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

/**
 * @title IPool (minimal Aave V3 Pool interface)
 * @notice Only the functions required by FlashTriangularArbitrage are declared.
 *         Verified against Aave V3 Polygon deployment:
 *         Pool: 0x794a61358D6845594F94dc1DB02A252b5b4814aD
 *         PoolAddressesProvider: 0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb
 */
interface IPool {
    /**
     * @notice Allows a user to request a flash loan for a single asset.
     * @param receiverAddress Address of the contract implementing IFlashLoanSimpleReceiver
     * @param asset Address of the asset to be flash-borrowed
     * @param amount Amount to be flash-borrowed
     * @param params Arbitrary bytes-encoded params passed to executeOperation
     * @param referralCode Referral code (0 if none)
     */
    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external;

    /**
     * @notice Returns the fee (in bps of the flash-loaned amount) charged on flash loans.
     *         For Aave V3 this is normally FLASHLOAN_PREMIUM_TOTAL, expressed in bps (e.g. 5 = 0.05%).
     */
    function FLASHLOAN_PREMIUM_TOTAL() external view returns (uint128);
}

/**
 * @title IPoolAddressesProvider (minimal)
 */
interface IPoolAddressesProvider {
    function getPool() external view returns (address);
}

/**
 * @title IFlashLoanSimpleReceiver
 * @notice Interface that must be implemented by the receiver contract of a flashLoanSimple call.
 */
interface IFlashLoanSimpleReceiver {
    /**
     * @notice Executes an operation after receiving the flash-borrowed asset.
     * @dev Must ensure that the contract can return the debt + premium, e.g. has
     *      enough funds to repay and has approved the Pool to pull the total amount.
     * @param asset The address of the flash-borrowed asset
     * @param amount The amount of the flash-borrowed asset
     * @param premium The fee of the flash-borrowed asset
     * @param initiator The address of the flashloan initiator
     * @param params The byte-encoded params passed when initiating the flashloan
     * @return True if the execution of the operation succeeds, false otherwise
     */
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external returns (bool);
}
