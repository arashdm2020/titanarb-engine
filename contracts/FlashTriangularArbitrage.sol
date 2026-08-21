// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import {IPool, IFlashLoanSimpleReceiver} from "../interfaces/IAavePool.sol";
import {ISwapRouter} from "../interfaces/ISwapRouter.sol";

/**
 * @title FlashTriangularArbitrage
 * @notice Executes a 3-leg triangular arbitrage (e.g. USDC -> WMATIC -> USDT -> USDC) funded by
 *         an Aave V3 `flashLoanSimple`, swapping on Uniswap V3.
 *
 * @dev SECURITY MODEL
 *      - Only `owner` or `authorizedExecutor` may call {executeArbitrage}.
 *      - `executeOperation` may only be called by the configured Aave V3 Pool, and only while
 *        this contract itself is the flash-loan initiator (i.e. it can never be triggered by an
 *        unrelated flash loan taken out by a third party against this contract).
 *      - All swap routers and both token endpoints of every route step must be explicitly
 *        allow-listed by the owner before use.
 *      - Every swap enforces a caller-supplied `amountOutMinimum` and the whole route enforces a
 *        transaction `deadline`; stale/expired route data is rejected.
 *      - `tx.origin` is never used for access control.
 *
 *      ATOMICITY NOTE ON ArbitrageFailed
 *      Because `flashLoanSimple` requires the loan + premium to be repaid within the same
 *      transaction, any failure inside {executeOperation} (bad swap, insufficient output,
 *      insufficient repayment, etc.) causes the entire transaction -- including any events -- to
 *      revert and roll back. `ArbitrageFailed` is declared for interface completeness /
 *      potential future non-atomic recovery flows, but in the current atomic design it will
 *      never actually be observed on-chain; off-chain simulation (`eth_call`) performed by the
 *      bot before submission is the real safety net against failed/wasted transactions.
 */
contract FlashTriangularArbitrage is Ownable, ReentrancyGuard, IFlashLoanSimpleReceiver {
    using SafeERC20 for IERC20;

    // ---------------------------------------------------------------------
    // Types
    // ---------------------------------------------------------------------

    /// @notice A single leg of the triangular route.
    struct SwapStep {
        address router; // Uniswap V3 SwapRouter to use for this leg
        address tokenIn;
        address tokenOut;
        uint24 fee; // Uniswap V3 fee tier for the pool used by this leg (e.g. 500 = 0.05%)
        uint256 amountOutMinimum; // computed off-chain from executable quote - slippage
    }

    /// @notice Full arbitrage instruction set passed through Aave's `params` bytes.
    struct ArbParams {
        SwapStep[3] steps;
        uint256 deadline; // unix timestamp after which the whole route is rejected
    }

    // ---------------------------------------------------------------------
    // Immutable / configuration state
    // ---------------------------------------------------------------------

    IPool public immutable AAVE_POOL;

    address public authorizedExecutor;

    /// @notice Destination for realized profit after every successful arbitrage. Deliberately a
    ///         separate, explicitly-configured address rather than assumed to equal `owner()`
    ///         (e.g. a treasury/multisig distinct from the contract admin key).
    address public profitRecipient;

    /// @dev Tracks whether `this` contract is the current, in-flight initiator of a flash loan.
    ///      Prevents `executeOperation` from being invoked by an unrelated flash loan that some
    ///      other contract might take out naming this contract as `receiverAddress` (which Aave
    ///      would reject anyway since receiverAddress IS this contract and initiator would still
    ///      be msg.sender of flashLoanSimple - kept as a defense-in-depth flag).
    bool private _flashLoanInFlight;

    mapping(address => bool) public allowedTokens;
    mapping(address => bool) public allowedRouters;

    // ---------------------------------------------------------------------
    // Events
    // ---------------------------------------------------------------------

    event FlashLoanRequested(address indexed asset, uint256 amount, uint256 timestamp);

    event SwapExecuted(
        uint256 indexed stepIndex,
        address indexed tokenIn,
        address indexed tokenOut,
        uint256 amountIn,
        uint256 amountOut,
        uint24 fee
    );

    event ArbitrageExecuted(
        address indexed asset,
        uint256 amount,
        uint256 premium,
        uint256 finalAmount,
        uint256 profit,
        address tokenA,
        address tokenB,
        address tokenC
    );

    /// @dev See ATOMICITY NOTE above - retained for interface completeness.
    event ArbitrageFailed(address indexed asset, uint256 amount, string reason);

    event ProfitGenerated(address indexed asset, uint256 profit);

    event Withdrawn(address indexed token, address indexed to, uint256 amount);

    event ProfitTransferred(address indexed token, address indexed recipient, uint256 amount);

    event ProfitRecipientUpdated(address indexed recipient);

    event AuthorizedExecutorUpdated(address indexed executor);

    event AllowedTokenUpdated(address indexed token, bool allowed);

    event AllowedRouterUpdated(address indexed router, bool allowed);

    // ---------------------------------------------------------------------
    // Errors
    // ---------------------------------------------------------------------

    error ZeroAddress();
    error ZeroAmount();
    error Unauthorized();
    error InvalidInitiator();
    error InvalidCaller();
    error InvalidAsset();
    error InvalidRouter();
    error InvalidRoute();
    error Expired();
    error InsufficientOutput(uint256 expectedMin, uint256 actual);
    error InsufficientRepayment(uint256 required, uint256 available);

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
     *                 On Polygon mainnet: 0x794a61358D6845594F94dc1DB02A252b5b4814aD
     * @param initialExecutor Address permitted to trigger {executeArbitrage} in addition to owner.
     * @param initialProfitRecipient Address that receives realized profit after every successful
     *        arbitrage. Must be non-zero. Not assumed to equal `owner()` -- pass the same address
     *        explicitly at deploy time if that is actually desired.
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

    /// @notice Updates the profit destination. Must never be zero -- reverts otherwise, which
    ///         guarantees {executeOperation} can never distribute profit into the void.
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

    function setAllowedRouter(address router, bool allowed) external onlyOwner {
        if (router == address(0)) revert ZeroAddress();
        allowedRouters[router] = allowed;
        emit AllowedRouterUpdated(router, allowed);
    }

    // ---------------------------------------------------------------------
    // Arbitrage entry point
    // ---------------------------------------------------------------------

    /**
     * @notice Requests an Aave V3 flash loan and executes a 3-leg triangular arbitrage.
     * @dev Only callable by `owner` or `authorizedExecutor`. Amount is caller-supplied; never
     *      hardcoded. Validates route continuity BEFORE requesting the flash loan so that an
     *      obviously malformed route reverts cheaply without incurring the Aave call.
     * @param asset The asset to flash-borrow. Must equal steps[0].tokenIn and steps[2].tokenOut.
     * @param amount The amount to flash-borrow, in the asset's smallest integer unit.
     * @param steps Exactly 3 swap legs describing the triangular route.
     * @param deadline Unix timestamp after which the whole route is considered stale and rejected.
     */
    function executeArbitrage(
        address asset,
        uint256 amount,
        SwapStep[3] calldata steps,
        uint256 deadline
    ) external onlyAuthorized nonReentrant {
        if (asset == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();
        if (block.timestamp > deadline) revert Expired();
        if (!allowedTokens[asset]) revert InvalidAsset();

        _validateRoute(asset, steps);

        ArbParams memory arbParams;
        arbParams.steps[0] = steps[0];
        arbParams.steps[1] = steps[1];
        arbParams.steps[2] = steps[2];
        arbParams.deadline = deadline;

        emit FlashLoanRequested(asset, amount, block.timestamp);

        _flashLoanInFlight = true;
        AAVE_POOL.flashLoanSimple(address(this), asset, amount, abi.encode(arbParams), 0);
        _flashLoanInFlight = false;
    }

    function _validateRoute(address asset, SwapStep[3] calldata steps) internal view {
        if (steps[0].tokenIn != asset) revert InvalidRoute();
        if (steps[0].tokenOut != steps[1].tokenIn) revert InvalidRoute();
        if (steps[1].tokenOut != steps[2].tokenIn) revert InvalidRoute();
        if (steps[2].tokenOut != asset) revert InvalidRoute();

        for (uint256 i = 0; i < 3; i++) {
            SwapStep calldata s = steps[i];
            if (s.router == address(0) || s.tokenIn == address(0) || s.tokenOut == address(0)) {
                revert ZeroAddress();
            }
            if (!allowedRouters[s.router]) revert InvalidRouter();
            if (!allowedTokens[s.tokenIn] || !allowedTokens[s.tokenOut]) revert InvalidAsset();
            if (s.amountOutMinimum == 0) revert ZeroAmount();
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
        if (arbParams.steps[0].tokenIn != asset) revert InvalidAsset();

        uint256 amountOut = amount;
        address currentToken = asset;

        for (uint256 i = 0; i < 3; i++) {
            SwapStep memory step = arbParams.steps[i];
            if (step.tokenIn != currentToken) revert InvalidRoute();

            amountOut = _swapExactInputSingle(step, amountOut, arbParams.deadline);

            emit SwapExecuted(i, step.tokenIn, step.tokenOut, i == 0 ? amount : amountOut, amountOut, step.fee);
            currentToken = step.tokenOut;
        }

        if (currentToken != asset) revert InvalidRoute();

        uint256 repayment = amount + premium;
        uint256 finalAmount = amountOut;

        if (finalAmount < repayment) revert InsufficientRepayment(repayment, finalAmount);

        uint256 profit = finalAmount - repayment;

        // Approve exactly the amount Aave will pull FIRST, so the intended repayment amount is
        // fixed before any other balance movement, then sweep the realized profit out to
        // `profitRecipient` so it never remains permanently trapped in this contract. After both
        // steps, this contract's remaining balance of `asset` is exactly `repayment` (which Aave
        // pulls via transferFrom immediately after this function returns `true`), leaving only
        // unrelated dust (if any) behind.
        IERC20(asset).forceApprove(address(AAVE_POOL), repayment);

        if (profit > 0) {
            address recipient = profitRecipient;
            if (recipient == address(0)) revert ZeroAddress();
            IERC20(asset).safeTransfer(recipient, profit);
            emit ProfitTransferred(asset, recipient, profit);
        }

        emit ArbitrageExecuted(
            asset,
            amount,
            premium,
            finalAmount,
            profit,
            arbParams.steps[0].tokenOut,
            arbParams.steps[1].tokenOut,
            arbParams.steps[2].tokenOut
        );
        if (profit > 0) emit ProfitGenerated(asset, profit);

        return true;
    }

    function _swapExactInputSingle(SwapStep memory step, uint256 amountIn, uint256 deadline)
        internal
        returns (uint256 amountOut)
    {
        IERC20(step.tokenIn).forceApprove(step.router, amountIn);

        ISwapRouter.ExactInputSingleParams memory swapParams = ISwapRouter.ExactInputSingleParams({
            tokenIn: step.tokenIn,
            tokenOut: step.tokenOut,
            fee: step.fee,
            recipient: address(this),
            deadline: deadline,
            amountIn: amountIn,
            amountOutMinimum: step.amountOutMinimum,
            sqrtPriceLimitX96: 0
        });

        amountOut = ISwapRouter(step.router).exactInputSingle(swapParams);

        if (amountOut < step.amountOutMinimum) {
            revert InsufficientOutput(step.amountOutMinimum, amountOut);
        }
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
        (bool success, ) = owner().call{value: amount}("");
        require(success, "native transfer failed");
        emit Withdrawn(address(0), owner(), amount);
    }

    receive() external payable {}
}
