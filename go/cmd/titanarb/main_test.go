package main

import (
	"context"
	"io"
	"math/big"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/execution"
	"github.com/titanarb/titanarb-go/internal/logger"
	"github.com/titanarb/titanarb-go/internal/operations"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
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

func TestApprovedDynamicAssetExpandsMarketOnlyUniverse(t *testing.T) {
	core := config.MarketConfig{
		BaseAsset:           "USDC",
		ExecutionAssetNames: []string{"USDC", "WETH"},
		Tokens: map[string]config.Token{
			"USDC":                       {Symbol: "USDC", Address: "0x0000000000000000000000000000000000000001", Decimals: 6},
			"WETH":                       {Symbol: "WETH", Address: "0x0000000000000000000000000000000000000002", Decimals: 18},
			"USDC_E_BRIDGED_ALTERNATIVE": {Symbol: "USDC.e", Address: "0x0000000000000000000000000000000000000003", Decimals: 6},
		},
	}

	expanded, added, decisions := marketConfigWithApprovedDynamicAssets(core, nil, nil, nil, nil, nil)
	if len(added) != 1 || added[0] != "USDC_E_BRIDGED_ALTERNATIVE" {
		t.Fatalf("USDC.e dynamic candidate not approved through universe manager: %v", added)
	}
	if len(decisions) == 0 || !strings.Contains(strings.Join(decisions, ","), "scan_failed") {
		t.Fatalf("fallback dynamic bootstrap did not explain scanner failure: %v", decisions)
	}
	if got := strings.Join(core.ExecutionAssets(), ","); got != "USDC,WETH" {
		t.Fatalf("core execution assets changed: %s", got)
	}
	if got := strings.Join(expanded.ExecutionAssets(), ","); got != "USDC,USDC_E_BRIDGED_ALTERNATIVE,WETH" {
		t.Fatalf("market universe not expanded: %s", got)
	}
	if routeUsesOnlyAssets([]string{"USDC", "USDC_E_BRIDGED_ALTERNATIVE", "USDC"}, assetSet(core.ExecutionAssets())) {
		t.Fatal("dynamic route was incorrectly execution eligible")
	}
}

func TestLoadForcedTradeConfig(t *testing.T) {
	t.Setenv("FORCE_ONE_TRADE", "true")
	t.Setenv("FORCE_TRADE_ASSET", "USDC")
	t.Setenv("FORCE_TRADE_AMOUNT", "10000000")
	t.Setenv("FORCE_TRADE_ROUTE", "USDC,WETH,USDC")
	dir := t.TempDir()
	got, err := loadForcedTradeConfig(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Enabled || got.Asset != "USDC" || got.Amount.Cmp(big.NewInt(10_000_000)) != 0 || strings.Join(got.Route, ",") != "USDC,WETH,USDC" {
		t.Fatalf("forced config mismatch: %+v", got)
	}
	if !strings.HasPrefix(got.MarkerPath, dir) {
		t.Fatalf("marker path did not use observability dir: %s", got.MarkerPath)
	}
}

func TestLoadForcedTradeConfigRejectsNonCycle(t *testing.T) {
	t.Setenv("FORCE_ONE_TRADE", "true")
	t.Setenv("FORCE_TRADE_ASSET", "USDC")
	t.Setenv("FORCE_TRADE_AMOUNT", "10000000")
	t.Setenv("FORCE_TRADE_ROUTE", "USDC,WETH,ARB")
	if _, err := loadForcedTradeConfig(t.TempDir(), ""); err == nil {
		t.Fatal("invalid forced route accepted")
	}
}

func TestConsumeForcedTradeIsOneShot(t *testing.T) {
	path := t.TempDir() + "/force_one_trade_consumed.json"
	first, err := consumeForcedTrade(path, map[string]any{"route": "USDC,WETH,USDC"})
	if err != nil || !first {
		t.Fatalf("first consume failed: consumed=%t err=%v", first, err)
	}
	second, err := consumeForcedTrade(path, map[string]any{"route": "USDC,WETH,USDC"})
	if err != nil || second {
		t.Fatalf("second consume was not blocked: consumed=%t err=%v", second, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

type fakeForcedBuilder struct {
	calls atomic.Uint64
	opp   *opportunity.Opportunity
}

func (f *fakeForcedBuilder) ForcedOpportunity(context.Context, uint64, []string, *big.Int) (*opportunity.Opportunity, error) {
	f.calls.Add(1)
	return f.opp, nil
}

type fakeForcedPipeline struct {
	calls    atomic.Uint64
	observed atomic.Uint64
}

func (f *fakeForcedPipeline) ProcessWithObserver(_ context.Context, opp *opportunity.Opportunity, observer execution.Observer) execution.Outcome {
	f.calls.Add(1)
	if observer != nil {
		observer("forced_trade_simulation", map[string]any{"result": "pass", "expected_output": opp.AmountOut.String(), "fees": "0", "repayment": opp.AmountIn.String()})
		f.observed.Add(1)
	}
	return execution.Outcome{Decision: "reject", Reason: "unprofitable after final gas repricing", FinalProfit: big.NewInt(-1)}
}

type fakeBlockReader struct{}

func (fakeBlockReader) BlockNumber(context.Context) (uint64, error) { return 123, nil }

func TestRunForcedTradeTriggersOnceAndUsesPipeline(t *testing.T) {
	opp := &opportunity.Opportunity{
		Route: routes.Route{
			Symbols: []string{"USDC", "WETH", "USDC"},
			Hops:    []pools.Pool{{DEX: pools.UniswapV3}, {DEX: pools.CamelotV3}},
		},
		AmountIn:       big.NewInt(1000),
		AmountOut:      big.NewInt(900),
		GrossProfit:    big.NewInt(-100),
		AavePremium:    big.NewInt(1),
		GasEstimate:    big.NewInt(2),
		L1DataFee:      big.NewInt(3),
		ExpectedProfit: big.NewInt(-106),
		MinProfit:      big.NewInt(10),
		Timestamp:      time.Now().UTC(),
	}
	builder := &fakeForcedBuilder{opp: opp}
	pipeline := &fakeForcedPipeline{}
	sink, err := operations.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close(context.Background())
	cfg := forcedTradeConfig{Enabled: true, Asset: "USDC", Amount: big.NewInt(1000), Route: []string{"USDC", "WETH", "USDC"}, MarkerPath: t.TempDir() + "/force_one_trade_consumed.json"}
	log := logger.New(false, io.Discard)

	runForcedTrade(context.Background(), cfg, builder, pipeline, fakeBlockReader{}, sink, log)
	runForcedTrade(context.Background(), cfg, builder, pipeline, fakeBlockReader{}, sink, log)

	if got := builder.calls.Load(); got != 1 {
		t.Fatalf("forced opportunity builder calls=%d", got)
	}
	if got := pipeline.calls.Load(); got != 1 {
		t.Fatalf("execution pipeline calls=%d", got)
	}
	if got := pipeline.observed.Load(); got != 1 {
		t.Fatalf("pipeline observer calls=%d", got)
	}
}
