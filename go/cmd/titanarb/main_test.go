package main

import (
	"math/big"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/runtimeconfig"
)

func TestLegacyRawAmountIsNotCopiedAcrossAssets(t *testing.T) {
	t.Setenv("TITANARB_MARKET_AMOUNT_WETH_RAW", "")
	market := config.MarketConfig{
		BaseAsset:           "USDC",
		ExecutionAssetNames: []string{"USDC", "WETH"},
		Tokens: map[string]config.Token{
			"USDC": {Symbol: "USDC"},
			"WETH": {Symbol: "WETH"},
		},
	}
	amounts := marketAmounts(market, big.NewInt(1_000_000_000))
	if amounts["USDC"] == nil || amounts["WETH"] != nil {
		t.Fatalf("unsafe cross-asset legacy amount reuse: %#v", amounts)
	}
}

func TestExplicitPerAssetRawAmountsAreRespected(t *testing.T) {
	t.Setenv("TITANARB_MARKET_AMOUNT_WETH_RAW", "1000000000000000000")
	market := config.MarketConfig{
		BaseAsset:           "USDC",
		ExecutionAssetNames: []string{"USDC", "WETH"},
		Tokens: map[string]config.Token{
			"USDC": {Symbol: "USDC"},
			"WETH": {Symbol: "WETH"},
		},
	}
	amounts := marketAmounts(market, big.NewInt(1_000_000_000))
	if amounts["WETH"] == nil || amounts["USDC"] != nil {
		t.Fatalf("explicit asset sizing ignored: %#v", amounts)
	}
}

func TestParseHexBlock(t *testing.T) {
	got, err := parseHexBlock("0x1d9e9ee9")
	if err != nil || got != 496_934_633 {
		t.Fatalf("parseHexBlock() = %d, %v", got, err)
	}
}

func TestPollInterval(t *testing.T) {
	t.Setenv("POLL_INTERVAL_SECONDS", "3")
	if got := pollInterval(); got != 3*time.Second {
		t.Fatalf("pollInterval() = %s", got)
	}
}

func TestQuoteWorkerCountIsBounded(t *testing.T) {
	t.Setenv("TITANARB_QUOTE_WORKERS", "1000")
	if got := boundedWorkerCount("TITANARB_QUOTE_WORKERS", 8); got != 32 {
		t.Fatalf("upper bound ignored: %d", got)
	}
	t.Setenv("TITANARB_QUOTE_WORKERS", "0")
	if got := boundedWorkerCount("TITANARB_QUOTE_WORKERS", 8); got != 1 {
		t.Fatalf("lower bound ignored: %d", got)
	}
}

func TestMarketSearchOptionsFollowRiskDepth(t *testing.T) {
	balanced := marketSearchOptions(runtimeconfig.Defaults(runtimeconfig.Balanced))
	aggressive := marketSearchOptions(runtimeconfig.Defaults(runtimeconfig.Aggressive))

	if aggressive.EvaluationRoutesPerAsset <= balanced.EvaluationRoutesPerAsset {
		t.Fatalf("aggressive evaluation breadth did not increase: %#v <= %#v", aggressive, balanced)
	}
	if aggressive.OptimizerRoutesPerAsset <= balanced.OptimizerRoutesPerAsset {
		t.Fatalf("aggressive optimizer breadth did not increase: %#v <= %#v", aggressive, balanced)
	}
	if aggressive.OptimizerSamplesPerRoute <= balanced.OptimizerSamplesPerRoute {
		t.Fatalf("aggressive optimizer samples did not increase: %#v <= %#v", aggressive, balanced)
	}
}
