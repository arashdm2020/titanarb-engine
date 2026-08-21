// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {Script, console2} from "forge-std/Script.sol";
import {FlashArbitrageExecutor} from "../contracts/FlashArbitrageExecutor.sol";
import {UniswapV3Adapter} from "../contracts/adapters/UniswapV3Adapter.sol";
import {CamelotV3Adapter} from "../contracts/adapters/CamelotV3Adapter.sol";

/**
 * @title DeployArbitrum
 * @notice Phase 2 deployment for Arbitrum One: deploys the multi-DEX {FlashArbitrageExecutor}
 *         plus the {UniswapV3Adapter} and {CamelotV3Adapter}, then wires them together
 *         (approve adapters + allow-list tokens). This is the Arbitrum counterpart to the legacy
 *         Polygon {Deploy} script (which deploys the single-DEX FlashTriangularArbitrage and is
 *         left unchanged).
 *
 *         Everything is read from environment variables -- nothing chain-specific is hardcoded.
 *
 * ---------------------------------------------------------------------------------------------
 *  CRITICAL: UNISWAP_SWAP_ROUTER must be the ORIGINAL, deadline-bearing Uniswap V3 SwapRouter
 *  (0xE592427A0AEce92De3Edee1F18E0157C05861564 on Arbitrum One), NOT SwapRouter02
 *  (0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45). UniswapV3Adapter calls
 *  ISwapRouter.exactInputSingle with a `deadline` field; SwapRouter02 removed that field and has
 *  a different function selector, so wiring the adapter to SwapRouter02 makes every Uniswap hop
 *  revert. Use config/arbitrum.json -> uniswapV3.swapRouter.
 * ---------------------------------------------------------------------------------------------
 *
 * Required env vars:
 *   PRIVATE_KEY          - deployer private key (never commit this)
 *   AAVE_POOL            - Aave V3 Pool address (0x794a61358D6845594F94dc1DB02A252b5b4814aD)
 *   EXECUTOR             - address authorized to trigger executeArbitrage (can be deployer initially)
 *   PROFIT_RECIPIENT     - address that receives realized profit (must be non-zero; set explicitly)
 *   UNISWAP_SWAP_ROUTER  - ORIGINAL Uniswap V3 SwapRouter (see CRITICAL note above)
 *   CAMELOT_SWAP_ROUTER  - Camelot V3 (Algebra Integral) SwapRouter (0x1F721E2E82F6676FCE4eA07A5958cF098D339e18)
 *   TOKEN_WETH, TOKEN_USDC - core tokens to allow-list (required)
 *
 * Optional env vars (allow-listed only when set to a non-zero address):
 *   TOKEN_USDT, TOKEN_ARB, TOKEN_USDC_E
 *
 * Usage:
 *   forge script script/DeployArbitrum.s.sol:DeployArbitrum \
 *       --rpc-url $ARBITRUM_RPC_URL --broadcast --verify -vvvv
 *
 * After deployment, copy the logged addresses into your .env:
 *   FLASH_EXECUTOR_ADDRESS, UNISWAP_V3_ADAPTER, CAMELOT_V3_ADAPTER
 */
contract DeployArbitrum is Script {
    function run()
        external
        returns (FlashArbitrageExecutor arb, UniswapV3Adapter uniAdapter, CamelotV3Adapter camelotAdapter)
    {
        uint256 deployerKey = vm.envUint("PRIVATE_KEY");
        address aavePool = vm.envAddress("AAVE_POOL");
        address executor = vm.envAddress("EXECUTOR");
        address profitRecipient = vm.envAddress("PROFIT_RECIPIENT");
        address uniswapSwapRouter = vm.envAddress("UNISWAP_SWAP_ROUTER");
        address camelotSwapRouter = vm.envAddress("CAMELOT_SWAP_ROUTER");
        address weth = vm.envAddress("TOKEN_WETH");
        address usdc = vm.envAddress("TOKEN_USDC");

        // Optional tokens: default to address(0) (skip) when unset.
        address usdt = vm.envOr("TOKEN_USDT", address(0));
        address arbTok = vm.envOr("TOKEN_ARB", address(0));
        address usdcE = vm.envOr("TOKEN_USDC_E", address(0));

        require(aavePool != address(0), "AAVE_POOL not set");
        require(profitRecipient != address(0), "PROFIT_RECIPIENT not set");
        require(uniswapSwapRouter != address(0), "UNISWAP_SWAP_ROUTER not set");
        require(camelotSwapRouter != address(0), "CAMELOT_SWAP_ROUTER not set");
        require(weth != address(0), "TOKEN_WETH not set");
        require(usdc != address(0), "TOKEN_USDC not set");

        vm.startBroadcast(deployerKey);

        // 1. Core executor.
        arb = new FlashArbitrageExecutor(aavePool, executor, profitRecipient);

        // 2. DEX adapters. The Uniswap adapter MUST bind to the original deadline-bearing router.
        uniAdapter = new UniswapV3Adapter(uniswapSwapRouter);
        camelotAdapter = new CamelotV3Adapter(camelotSwapRouter);

        // 3. Approve adapters (the only contracts executeOperation may ever call).
        arb.setApprovedAdapter(address(uniAdapter), true);
        arb.setApprovedAdapter(address(camelotAdapter), true);

        // 4. Allow-list tokens (both endpoints of every hop must be allow-listed).
        arb.setAllowedToken(weth, true);
        arb.setAllowedToken(usdc, true);
        if (usdt != address(0)) arb.setAllowedToken(usdt, true);
        if (arbTok != address(0)) arb.setAllowedToken(arbTok, true);
        if (usdcE != address(0)) arb.setAllowedToken(usdcE, true);

        vm.stopBroadcast();

        console2.log("=== Phase 2 Arbitrum deployment ===");
        console2.log("FlashArbitrageExecutor (FLASH_EXECUTOR_ADDRESS):", address(arb));
        console2.log("UniswapV3Adapter       (UNISWAP_V3_ADAPTER):    ", address(uniAdapter));
        console2.log("CamelotV3Adapter       (CAMELOT_V3_ADAPTER):    ", address(camelotAdapter));
        console2.log("Owner:", arb.owner());
        console2.log("Authorized executor:", arb.authorizedExecutor());
        console2.log("Profit recipient:", arb.profitRecipient());
        console2.log("Uniswap adapter router (must be original 0xE592...1564):", address(uniAdapter.ROUTER()));
        console2.log("Camelot adapter router:", address(camelotAdapter.ROUTER()));
    }
}
