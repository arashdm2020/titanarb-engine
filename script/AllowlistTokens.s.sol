// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

import {Script, console2} from "forge-std/Script.sol";
import {FlashTriangularArbitrage} from "../contracts/FlashTriangularArbitrage.sol";

/**
 * @title AllowlistTokens
 * @notice Adds new tokens to the deployed contract's token allow-list.
 *         The contract was deployed with only USDC/WMATIC/USDT allow-listed.
 *         This script sends setAllowedToken transactions from the owner wallet.
 *         No redeployment is needed -- the contract accepts arbitrary SwapStep[3]
 *         as long as every tokenIn/tokenOut is allow-listed.
 *
 * Usage:
 *   forge script script/AllowlistTokens.s.sol:AllowlistTokens \
 *     --rpc-url $POLYGON_RPC_URL --broadcast -vvv
 *
 * Required env vars:
 *   PRIVATE_KEY      - owner private key (must match the contract's owner())
 *   CONTRACT_ADDRESS - deployed FlashTriangularArbitrage address
 *   TOKEN_USDC_E     - USDC.e address (0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174)
 *   TOKEN_WBTC       - WBTC address (0x1BFD67037B42Cf73acf2047067bd4F2C47D9BfD6)
 *   TOKEN_DAI        - DAI address (0x8f3Cf7aD23Cd3CaDbD9735AFf958023239c6A063)
 *   TOKEN_CRV        - CRV address (0x172370d5Cd63279eFa6d502DAB29171933a610AF)
 */
contract AllowlistTokens is Script {
    function run() external {
        uint256 ownerKey = vm.envUint("PRIVATE_KEY");
        address contractAddr = vm.envAddress("CONTRACT_ADDRESS");
        address usdcE = vm.envAddress("TOKEN_USDC_E");
        address wbtc = vm.envAddress("TOKEN_WBTC");
        address dai = vm.envAddress("TOKEN_DAI");
        address crv = vm.envAddress("TOKEN_CRV");

        FlashTriangularArbitrage arb = FlashTriangularArbitrage(payable(contractAddr));
        require(arb.owner() == vm.addr(ownerKey), "Caller is not the contract owner");

        vm.startBroadcast(ownerKey);

        arb.setAllowedToken(usdcE, true);
        console2.log("Allow-listed USDC.e:", usdcE);

        arb.setAllowedToken(wbtc, true);
        console2.log("Allow-listed WBTC:", wbtc);

        arb.setAllowedToken(dai, true);
        console2.log("Allow-listed DAI:", dai);

        arb.setAllowedToken(crv, true);
        console2.log("Allow-listed CRV:", crv);

        vm.stopBroadcast();

        console2.log("Done. All 4 tokens allow-listed on:", contractAddr);
    }
}
