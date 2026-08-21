// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import {IPool, IFlashLoanSimpleReceiver} from "../interfaces/IAavePool.sol";
import {IArbitrageAdapter} from "../interfaces/IArbitrageAdapter.sol";

/**
 * @title FlashArbitrageExecutor
 * @notice Executes a 2-to-4-hop multi-DEX arbitrage funded by an Aave V3 `flashLoanSimple`.
 *         Each hop is routed through an explicitly approved {IArbitrageAdapter} (e.g. Uniswap V3,
 *         Camelot V3) rather than directly calling a swap router, so the route can freely mix
 *         DEXes with incompatible ABIs within a single atomic transaction.
 *
 * @dev SECURITY MODEL
 *      - Only `owner` or `authorizedExecutor` may call {executeArbitrage}.
 *      - `executeOperation` may only be called by the configured Aave V3 Pool, and only while
 *        this contract itself is the flash-loan initiator (same in-flight guard pattern as the
 *        original FlashTriangularArbitrage contract).
 *      - Every hop's adapter must be present in `approvedAdapters`; there is NO arbitrary
 *        `target.call(arbitraryBytes)` path anywhere in this contract. The full generalized
 *        execution surface is: pick one of a small, owner-curated set of adapter contracts, each
 *        of which itself only knows how to talk to one specific, immutably-configured DEX router.
 *      - Every token endpoint of every hop must be explicitly allow-listed by the owner.
 *      - Every swap enforces a caller-supplied `amountOutMinimum`; the whole route enforces a
 *        `deadline`. The final balance must cover `amount + premium + minProfit` or the entire
 *        atomic transaction reverts (`INSUFFICIENT_PROFIT`).
 *      - The Aave flash-loan premium is NEVER hard-coded; it is always the `premium` value Aave
 *        itself supplies to {executeOperation}.
 *      - `tx.origin` is never used for access control.
 */
contract FlashArbitrageExecutor is Ownable, ReentrancyGuard, IFlashLoanSimpleReceiver {
    using SafeERC20 for IERC20;

    // ---------------------------------------------------------------------
    // Types
    // ---------------------------------------------------------------------

    /// @notice A single hop of the route. `adapter` must be in `approvedAdapters`.
    struct SwapStep {
        address adapter;
        address tokenIn;
        address tokenOut;
        uint256 amountOutMinimum; // computed off-chain from executable quote - slippage
        bytes data; // adapter-specific extra params (e.g. abi.encode(uint24 fee) for Uniswap V3)
    }

    /// @notice Full arbitrage instruction set passed through Aave's `params` bytes.
    struct ArbParams {
        SwapStep[] steps;
        uint256 deadline; // unix timestamp after which the whole route is rejected
        uint256 minProfit; // minimum acceptable profit, in the flash-borrowed asset's units
    }

    // ---------------------------------------------------------------------
    // Constants
    // ---------------------------------------------------------------------

    uint256 public constant MIN_HOPS = 2;
    uint256 public constant MAX_HOPS = 4;

    // ---------------------------------------------------------------------
    // Immutable / configuration state
    // ---------------------------------------------------------------------

    IPool public immutable AAVE_POOL;

    address public authorizedExecutor;

    /// @notice Destination for realized profit after every successful arbitrage. Deliberately a
    ///         separate, explicitly-configured address rather than assumed to equal `owner()`.
    address public profitRecipient;

    /// @dev Tracks whether `this` contract is the current, in-flight initiator of a flash loan.
    bool private _flashLoanInFlight;

    mapping(address => bool) public allowedTokens;
    mapping(address => bool) public approvedAdapters;

    // ---------------------------------------------------------------------
    // Events
    // ---------------------------------------------------------------------

    event FlashLoanRequested(address indexed asset, uint256 amount, uint256 timestamp);

    event SwapExecuted(
        uint256 indexed stepIndex,
        address indexed adapter,
        address indexed tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 amountOut
    );

    event ArbitrageExecuted(
        address indexed asset, uint256 amount, uint256 premium, uint256 finalAmount, uint256 profit, uint256 hops
    );

    event ProfitGenerated(address indexed asset, uint256 profit);

    event Withdrawn(address indexed token, address indexed to, uint256 amount);

    event ProfitTransferred(address indexed token, address indexed recipient, uint256 amount);

    event ProfitRecipientUpdated(address indexed recipient);

    event AuthorizedExecutorUpdated(address indexed executor);

    event AllowedTokenUpdated(address indexed token, bool allowed);

    event ApprovedAdapterUpdated(address indexed adapter, bool approved);

    // ---------------------------------------------------------------------
    // Errors
    // ---------------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error Unauthorized();
    error InvalidInitiator();
    error InvalidCaller();
    error InvalidAsset();
    error InvalidAdapter();
    error InvalidRoute();
    error InvalidHopCount();
    error Expired();
    error InsufficientOutput(uint256 expectedMin, uint256 actual);
    error InsufficientProfit(uint256 required, uint256 available);

    // ---------------------------------------------------------------------
    // Modifiers
    // ---------------------------------------------------------------------

    modifier onlyAuthorized() {
        if (msg.sender != owner() && msg.sender != authorizedExecutor) revert Unauthorized();
        _;
    }

    // ---------------------------------------------------------------------
    // Constructor
    // ---------------------------------------------------------------------

    /**
     * @param aavePool Address of the Aave V3 Pool (NOT the PoolAddressesProvider).
     * @param initialExecutor Address permitted to trigger {executeArbitrage} in addition to owner.
     * @param initialProfitRecipient Address that receives realized profit. Must be non-zero.
     */
    constructor(address aavePool, address initialExecutor, address initialProfitRecipient) Ownable() {
        if (aavePool == address(0)) revert ZeroAddress();
        if (initialProfitRecipient == address(0)) revert ZeroAddress();
        AAVE_POOL = IPool(aavePool);
        if (initialExecutor != address(0)) {
            authorizedExecutor = initialExecutor;
            emit AuthorizedExecutorUpdated(initialExecutor);
        }
        profitRecipient = initialProfitRecipient;
        emit ProfitRecipientUpdated(initialProfitRecipient);
    }

    // ---------------------------------------------------------------------
    // Admin configuration
    // ---------------------------------------------------------------------

    function setAuthorizedExecutor(address executor) external onlyOwner {
        if (executor == address(0)) revert ZeroAddress();
        authorizedExecutor = executor;
        emit AuthorizedExecutorUpdated(executor);
    }

    function setProfitRecipient(address recipient) external onlyOwner {
        if (recipient == address(0)) revert ZeroAddress();
        profitRecipient = recipient;
        emit ProfitRecipientUpdated(recipient);
    }

    function setAllowedToken(address token, bool allowed) external onlyOwner {
        if (token == address(0)) revert ZeroAddress();
        allowedTokens[token] = allowed;
        emit AllowedTokenUpdated(token, allowed);
    }

    /// @notice Approves or revokes a DEX adapter. Only approved adapters may ever be called by
    ///         {executeOperation} -- this is the sole extensibility point for adding new DEXes
    ///         (e.g. SushiSwap later) without changing this contract's execution logic.
    function setApprovedAdapter(address adapter, bool approved) external onlyOwner {
        if (adapter == address(0)) revert ZeroAddress();
        approvedAdapters[adapter] = approved;
        emit ApprovedAdapterUpdated(adapter, approved);
    }

    // ---------------------------------------------------------------------
    // Arbitrage entry point
    // ---------------------------------------------------------------------

    /**
     * @notice Requests an Aave V3 flash loan and executes a 2-4 hop multi-DEX arbitrage.
     * @param asset The asset to flash-borrow. Must equal steps[0].tokenIn and the last step's
     *        tokenOut.
     * @param amount The amount to flash-borrow, in the asset's smallest integer unit.
     * @param steps 2 to {MAX_HOPS} swap hops describing the route.
     * @param deadline Unix timestamp after which the whole route is considered stale.
     * @param minProfit Minimum acceptable profit (in `asset` units) for this attempt to succeed.
     */
    function executeArbitrage(
        address asset,
        uint256 amount,
        SwapStep[] calldata steps,
        uint256 deadline,
        uint256 minProfit
    ) external onlyAuthorized nonReentrant {
        if (asset == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();
        if (block.timestamp > deadline) revert Expired();
        if (!allowedTokens[asset]) revert InvalidAsset();

        _validateRoute(asset, steps);

        ArbParams memory arbParams;
        arbParams.steps = steps;
        arbParams.deadline = deadline;
        arbParams.minProfit = minProfit;

        emit FlashLoanRequested(asset, amount, block.timestamp);

        _flashLoanInFlight = true;
        AAVE_POOL.flashLoanSimple(address(this), asset, amount, abi.encode(arbParams), 0);
        _flashLoanInFlight = false;
    }

    function _validateRoute(address asset, SwapStep[] calldata steps) internal view {
        uint256 hops = steps.length;
        if (hops < MIN_HOPS || hops > MAX_HOPS) revert InvalidHopCount();

        if (steps[0].tokenIn != asset) revert InvalidRoute();
        if (steps[hops - 1].tokenOut != asset) revert InvalidRoute();

        for (uint256 i = 0; i < hops; i++) {
            SwapStep calldata s = steps[i];
            if (s.adapter == address(0) || s.tokenIn == address(0) || s.tokenOut == address(0)) {
                revert ZeroAddress();
            }
            if (!approvedAdapters[s.adapter]) revert InvalidAdapter();
            if (!allowedTokens[s.tokenIn] || !allowedTokens[s.tokenOut]) revert InvalidAsset();
            if (s.amountOutMinimum == 0) revert ZeroAmount();
            if (i > 0 && s.tokenIn != steps[i - 1].tokenOut) revert InvalidRoute();
        }
    }

    // ---------------------------------------------------------------------
    // Aave V3 flash loan callback
    // ---------------------------------------------------------------------

    /**
     * @inheritdoc IFlashLoanSimpleReceiver
     */
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        if (msg.sender != address(AAVE_POOL)) revert InvalidCaller();
        if (initiator != address(this)) revert InvalidInitiator();
        if (!_flashLoanInFlight) revert InvalidInitiator();

        ArbParams memory arbParams = abi.decode(params, (ArbParams));
        if (block.timestamp > arbParams.deadline) revert Expired();

        uint256 hops = arbParams.steps.length;
        if (arbParams.steps[0].tokenIn != asset) revert InvalidAsset();

        uint256 amountOut = amount;
        address currentToken = asset;

        for (uint256 i = 0; i < hops; i++) {
            SwapStep memory step = arbParams.steps[i];
            if (step.tokenIn != currentToken) revert InvalidRoute();

            uint256 amountIn = amountOut;
            IERC20(step.tokenIn).forceApprove(step.adapter, amountIn);
            amountOut = IArbitrageAdapter(step.adapter).executeSwap(
                step.tokenIn, step.tokenOut, amountIn, step.amountOutMinimum, arbParams.deadline, step.data
            );

            if (amountOut < step.amountOutMinimum) {
                revert InsufficientOutput(step.amountOutMinimum, amountOut);
            }

            emit SwapExecuted(i, step.adapter, step.tokenIn, step.tokenOut, amountIn, amountOut);
            currentToken = step.tokenOut;
        }

        if (currentToken != asset) revert InvalidRoute();

        uint256 repayment = amount + premium;
        uint256 finalAmount = amountOut;
        uint256 requiredBalance = repayment + arbParams.minProfit;

        if (finalAmount < requiredBalance) revert InsufficientProfit(requiredBalance, finalAmount);

        uint256 profit = finalAmount - repayment;

        // Approve exactly the amount Aave will pull FIRST, so the intended repayment amount is
        // fixed before any other balance movement, then sweep the realized profit out to
        // `profitRecipient` so it never remains permanently trapped in this contract.
        IERC20(asset).forceApprove(address(AAVE_POOL), repayment);

        if (profit > 0) {
            address recipient = profitRecipient;
            if (recipient == address(0)) revert ZeroAddress();
            IERC20(asset).safeTransfer(recipient, profit);
            emit ProfitTransferred(asset, recipient, profit);
        }

        emit ArbitrageExecuted(asset, amount, premium, finalAmount, profit, hops);
        if (profit > 0) emit ProfitGenerated(asset, profit);

        return true;
    }

    // ---------------------------------------------------------------------
    // Fund recovery
    // ---------------------------------------------------------------------

    function withdrawToken(address token, uint256 amount) external onlyOwner nonReentrant {
        if (token == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();
        IERC20(token).safeTransfer(owner(), amount);
        emit Withdrawn(token, owner(), amount);
    }

    function withdrawNative(uint256 amount) external onlyOwner nonReentrant {
        if (amount == 0) revert ZeroAmount();
        (bool success,) = owner().call{value: amount}("");
        require(success, "native transfer failed");
        emit Withdrawn(address(0), owner(), amount);
    }

    receive() external payable {}
}
