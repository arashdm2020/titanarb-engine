// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

/**
 * @title IUniswapV3Pool (minimal read-only interface)
 * @notice Only the state-reading functions needed for pool validation are declared.
 */
interface IUniswapV3Pool {
    function token0() external view returns (address);

    function token1() external view returns (address);

    function fee() external view returns (uint24);

    function liquidity() external view returns (uint128);

    function slot0()
        external
        view
        returns (
            uint160 sqrtPriceX96,
            int24 tick,
            uint16 observationIndex,
            uint16 observationCardinality,
            uint16 observationCardinalityNext,
            uint8 feeProtocol,
            bool unlocked
        );
}

interface IUniswapV3Factory {
    function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool);
}
