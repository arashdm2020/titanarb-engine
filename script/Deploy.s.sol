// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {Script, console2} from "forge-std/Script.sol";
import {FlashTriangularArbitrage} from "../contracts/FlashTriangularArbitrage.sol";

/**
 * @title Deploy
 * @notice Deploys FlashTriangularArbitrage using configuration read exclusively from environment
 *         variables. Nothing is hardcoded here -- AAVE_POOL, EXECUTOR, UNISWAP_ROUTER and the
 *         allow-listed tokens must all be supplied by the deployer.
 *
 * Required env vars:
 *   PRIVATE_KEY        - deployer private key (never commit this)
 *   AAVE_POOL          - Aave V3 Pool address
 *   EXECUTOR           - address authorized to trigger executeArbitrage (can be deployer initially)
 *   PROFIT_RECIPIENT   - address that receives realized arbitrage profit (must be non-zero; not
 *                        assumed to equal the deployer/owner -- set explicitly)
 *   UNISWAP_ROUTER      - Uniswap V3 SwapRouter address to allow-list
 *   TOKEN_USDC, TOKEN_WMATIC, TOKEN_USDT - token addresses to allow-list
 *
 * Usage (see README for full instructions):
 *   forge script script/Deploy.s.sol:Deploy --rpc-url $POLYGON_RPC_URL --broadcast --verify -vvvv
 */
contract Deploy is Script {
    function run() external returns (FlashTriangularArbitrage arb) {
        uint256 deployerKey = vm.envUint("PRIVATE_KEY");
        address aavePool = vm.envAddress("AAVE_POOL");
        address executor = vm.envAddress("EXECUTOR");
        address profitRecipient = vm.envAddress("PROFIT_RECIPIENT");
        address uniswapRouter = vm.envAddress("UNISWAP_ROUTER");
        address usdc = vm.envAddress("TOKEN_USDC");
        address wmatic = vm.envAddress("TOKEN_WMATIC");
        address usdt = vm.envAddress("TOKEN_USDT");

        require(aavePool != address(0), "AAVE_POOL not set");
        require(profitRecipient != address(0), "PROFIT_RECIPIENT not set");
        require(uniswapRouter != address(0), "UNISWAP_ROUTER not set");

        vm.startBroadcast(deployerKey);

        arb = new FlashTriangularArbitrage(aavePool, executor, profitRecipient);

        arb.setAllowedRouter(uniswapRouter, true);
        arb.setAllowedToken(usdc, true);
        arb.setAllowedToken(wmatic, true);
        arb.setAllowedToken(usdt, true);

        vm.stopBroadcast();

        console2.log("FlashTriangularArbitrage deployed at:", address(arb));
        console2.log("Owner:", arb.owner());
        console2.log("Authorized executor:", arb.authorizedExecutor());
    }
}
