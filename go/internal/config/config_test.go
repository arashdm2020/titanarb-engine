package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validEnv() map[string]string {
	return map[string]string{"CHAIN_ID": "42161", "ARBITRUM_RPC_URL": "https://rpc.example", "ARBITRUM_WSS_RPC_URL": "wss://rpc.example", "FLASH_EXECUTOR_ADDRESS": "0x1111111111111111111111111111111111111111", "UNISWAP_V3_ADAPTER": "0x2222222222222222222222222222222222222222", "CAMELOT_V3_ADAPTER": "0x3333333333333333333333333333333333333333", "PRIVATE_KEY": "not-used-in-phase-one", "DRY_RUN": "true", "EXECUTION_MODE": "dry_run"}
}
func lookup(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}
func TestValidConfig(t *testing.T) {
	if _, err := FromLookup(lookup(validEnv())); err != nil {
		t.Fatal(err)
	}
}
func TestInvalidChain(t *testing.T) {
	v := validEnv()
	v["CHAIN_ID"] = "137"
	if _, err := FromLookup(lookup(v)); err == nil {
		t.Fatal("expected invalid chain error")
	}
}
func TestMissingAddress(t *testing.T) {
	v := validEnv()
	v["UNISWAP_V3_ADAPTER"] = ""
	if _, err := FromLookup(lookup(v)); err == nil {
		t.Fatal("expected missing address error")
	}
}

func TestGenericRPCEnvironmentNames(t *testing.T) {
	v := validEnv()
	v["HTTP_RPC_URL"], v["WS_RPC_URL"] = "https://primary.example", "wss://primary.example"
	delete(v, "ARBITRUM_RPC_URL")
	delete(v, "ARBITRUM_WSS_RPC_URL")
	cfg, err := FromLookup(lookup(v))
	if err != nil || cfg.HTTPRPCURL != "https://primary.example" || cfg.WSRPCURL != "wss://primary.example" {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
}

func TestPrimarySecondaryRPCProviderConfig(t *testing.T) {
	v := validEnv()
	v["RPC_PRIMARY_NAME"] = "quicknode"
	v["RPC_PRIMARY_HTTP"] = "https://quicknode.example"
	v["RPC_PRIMARY_WSS"] = "wss://quicknode.example"
	v["RPC_PRIMARY_MAX_RPS"] = "15"
	v["RPC_PRIMARY_TARGET_RPS"] = "11"
	v["RPC_SECONDARY_NAME"] = "chainstack"
	v["RPC_SECONDARY_HTTP"] = "https://chainstack.example"
	v["RPC_SECONDARY_WSS"] = "wss://chainstack.example"
	v["RPC_SECONDARY_MAX_RPS"] = "25"
	v["RPC_SECONDARY_TARGET_RPS"] = "18"
	v["RPC_PROVIDER_MAX_BLOCK_LAG"] = "4"
	cfg, err := FromLookup(func(k string) string { return v[k] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPRPCURL != "https://quicknode.example" || cfg.WSRPCURL != "wss://quicknode.example" {
		t.Fatalf("primary provider did not become active config: %+v", cfg)
	}
	if len(cfg.RPCProviders) != 2 || cfg.RPCProviders[0].Name != "quicknode" || cfg.RPCProviders[1].Name != "chainstack" {
		t.Fatalf("provider list mismatch: %+v", cfg.RPCProviders)
	}
	if cfg.RPCProviders[0].MaxRPS != 15 || cfg.RPCProviders[1].MaxRPS != 25 {
		t.Fatalf("provider RPS limits mismatch: %+v", cfg.RPCProviders)
	}
	if cfg.RPCProviders[0].TargetRPS != 11 || cfg.RPCProviders[1].TargetRPS != 18 {
		t.Fatalf("provider target RPS mismatch: %+v", cfg.RPCProviders)
	}
	if cfg.RPCProviders[0].MaxBlockLag != 4 || cfg.RPCProviders[1].MaxBlockLag != 4 {
		t.Fatalf("provider block lag mismatch: %+v", cfg.RPCProviders)
	}
}

func TestLoadMarketConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arbitrum.json")
	data := `{"aaveV3":{"pool":"0x1111111111111111111111111111111111111111"},"uniswapV3":{"factory":"0x2222222222222222222222222222222222222222","quoterV2":"0x3333333333333333333333333333333333333333"},"camelotV3":{"algebraFactory":"0x4444444444444444444444444444444444444444","quoter":"0x5555555555555555555555555555555555555555"},"chainlink":{"ethUsdFeed":"0x6666666666666666666666666666666666666666"},"routeDiscovery":{"baseAsset":"USDC","intermediateTokens":["WETH"],"uniswapFeeTiers":[500]},"tokens":{"USDC":{"symbol":"USDC","address":"0x7777777777777777777777777777777777777777","decimals":6}}}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	market, err := LoadMarketConfig(path)
	if err != nil || market.BaseAsset != "USDC" || market.Tokens["USDC"].Decimals != 6 {
		t.Fatalf("market=%+v err=%v", market, err)
	}
}

func TestMarketPriceFeedsAreExplicitOnly(t *testing.T) {
	market := MarketConfig{Tokens: map[string]Token{"A": {Symbol: "A", USDFeed: "0x1111111111111111111111111111111111111111"}, "B": {Symbol: "B"}}}
	feeds := market.USDFeeds()
	if len(feeds) != 1 || feeds["A"] == "" {
		t.Fatalf("unexpected feed mapping: %#v", feeds)
	}
}

func TestMarketAssetsSeparateExecutionBoundaryFromReadOnlyDiscovery(t *testing.T) {
	market := MarketConfig{
		BaseAsset:          "USDC",
		IntermediateTokens: []string{"WETH", "ARB"},
		Tokens: map[string]Token{
			"USDC": {Symbol: "USDC", Address: "0x1111111111111111111111111111111111111111"},
			"WETH": {Symbol: "WETH", Address: "0x2222222222222222222222222222222222222222"},
			"ARB":  {Symbol: "ARB", Address: "0x3333333333333333333333333333333333333333"},
			"READ": {Symbol: "READ", Address: "0x4444444444444444444444444444444444444444"},
		},
	}
	if got := strings.Join(market.ExecutionAssets(), ","); got != "ARB,USDC,WETH" {
		t.Fatalf("unexpected execution asset universe %q", got)
	}
	if got := strings.Join(market.DiscoveryAssets(), ","); got != "ARB,READ,USDC,WETH" {
		t.Fatalf("unexpected discovery asset universe %q", got)
	}
}

func TestExecutionAssetsDoNotUseLegacyBaseWhenNeutralMembershipExists(t *testing.T) {
	market := MarketConfig{
		BaseAsset:           "USDC",
		ExecutionAssetNames: []string{"WETH", "ARB"},
		Tokens: map[string]Token{
			"USDC": {Symbol: "USDC", Address: "0x1111111111111111111111111111111111111111"},
			"WETH": {Symbol: "WETH", Address: "0x2222222222222222222222222222222222222222"},
			"ARB":  {Symbol: "ARB", Address: "0x3333333333333333333333333333333333333333"},
		},
	}
	if got := strings.Join(market.ExecutionAssets(), ","); got != "ARB,WETH" {
		t.Fatalf("legacy base leaked into neutral execution membership: %q", got)
	}
}
