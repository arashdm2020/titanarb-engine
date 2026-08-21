// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

/**
 * @title IArbitrageAdapter
 * @notice Common interface every DEX-specific adapter must implement. FlashArbitrageExecutor
 *         only ever calls through this interface on addresses explicitly present in its
 *         `approvedAdapters` allow-list -- it never constructs or forwards arbitrary calldata to
 *         an arbitrary target (no `target.call(arbitraryBytes)` pattern anywhere in this system).
 *
 * @dev CALL / TOKEN-FLOW CONTRACT (must be implemented exactly this way by every adapter):
 *      1. The caller (FlashArbitrageExecutor) must `forceApprove(adapter, amountIn)` on `tokenIn`
 *         BEFORE calling {executeSwap}.
 *      2. The adapter pulls `amountIn` of `tokenIn` from `msg.sender` via `safeTransferFrom`.
 *      3. The adapter performs the DEX-specific swap with the underlying router's `recipient`
 *         parameter set to `msg.sender` (the executor), so output tokens land directly back on
 *         the executor without an extra hop.
 *      4. The adapter returns the actual `amountOut` received.
 *      5. The adapter must NEVER retain a token balance across calls (stateless with respect to
 *         user funds) and must revert if the underlying router returns less than
 *         `amountOutMinimum`.
 *
 *      The `data` field carries adapter-specific structured parameters (e.g. the Uniswap V3 fee
 *      tier). Each adapter is solely responsible for decoding and validating its own `data` --
 *      it is never used as raw forwarded calldata to an arbitrary target.
 */
interface IArbitrageAdapter {
    /**
     * @notice Executes a single-hop swap on the adapter's underlying DEX.
     * @param tokenIn Token being sold.
     * @param tokenOut Token being bought.
     * @param amountIn Exact amount of `tokenIn` to sell (already approved to this adapter by the
     *        caller).
     * @param amountOutMinimum Minimum acceptable amount of `tokenOut` (slippage protection).
     * @param deadline Unix timestamp after which the swap must revert.
     * @param data Adapter-specific extra parameters (e.g. `abi.encode(uint24 fee)` for a Uniswap
     *        V3 adapter). Adapters that do not need extra parameters (e.g. Algebra/Camelot, which
     *        has a single dynamic-fee pool per pair) may ignore this field.
     * @return amountOut The actual amount of `tokenOut` received and forwarded to `msg.sender`.
     */
    function executeSwap(
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 amountOutMinimum,
        uint256 deadline,
        bytes calldata data
    ) external returns (uint256 amountOut);
}
