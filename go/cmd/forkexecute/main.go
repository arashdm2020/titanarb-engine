// forkexecute proves Go-generated execution against a local Anvil Arbitrum fork.
// It is intentionally unavailable on non-loopback endpoints and never changes
// the production runtime configuration.
package main

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/execution"
	"github.com/titanarb/titanarb-go/internal/fees"
	"github.com/titanarb/titanarb-go/internal/fork"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/profit"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/routes"
	"github.com/titanarb/titanarb-go/internal/rpc"
	"github.com/titanarb/titanarb-go/internal/safety"
)

func main() {
	endpoint := os.Getenv("TITANARB_FORK_RPC_URL")
	if !local(endpoint) {
		panic("forkexecute requires a local Anvil HTTP endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	baseCfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	market, err := config.LoadMarketConfig(filepath.Join("..", "config", "arbitrum.json"))
	if err != nil {
		panic(err)
	}
	localChain := rpc.New(endpoint, 45*time.Second, 0, nil)
	if chain, err := localChain.ChainID(ctx); err != nil || chain != config.ArbitrumOneChainID {
		panic("local fork chain ID validation failed")
	}
	controller, err := fork.NewController(endpoint, localChain)
	if err != nil {
		panic(err)
	}
	if err := controller.SyncTimestamp(ctx, time.Now().UTC().Unix()+1); err != nil {
		panic("local fork timestamp synchronization failed: " + err.Error())
	}
	cfg := baseCfg
	cfg.FlashExecutorAddress = required("TITANARB_FORK_EXECUTOR")
	cfg.UniswapV3Adapter = required("TITANARB_FORK_UNISWAP_ADAPTER")
	cfg.CamelotV3Adapter = required("TITANARB_FORK_CAMELOT_ADAPTER")
	cfg.DryRun = false
	cfg.ExecutionMode = "live"
	cfg.BroadcastEnabled = true
	amount := raw("TITANARB_FORK_LOAN_RAW", 10_000_000_000)
	minimum := raw("TITANARB_FORK_MIN_PROFIT_RAW", 5_000_000)
	feeSource := rpc.New(baseCfg.HTTPRPCURL, 30*time.Second, 1, nil) // authoritative Arbitrum fee reads; Anvil lacks ArbGasInfo.
	feeService := fees.New(feeSource, market.ArbGasInfo, market.ETHUSDFeed, 1_000)
	gate := safety.New(localChain, market.ETHUSDFeed, market.SequencerUptimeFeed, time.Duration(cfg.PriceMaxStalenessSeconds)*time.Second, time.Duration(cfg.SequencerGraceSeconds)*time.Second)
	jsonl := os.Getenv("TITANARB_FORK_TRADE_JSON")
	if jsonl == "" {
		jsonl = filepath.Join("runtime", "go-fork-trades.jsonl")
	}
	pipeline, err := execution.NewPipeline(cfg, market, localChain, feeService, gate, minimum, profit.New(jsonl), metrics.New())
	if err != nil {
		panic(err)
	}
	if err := execution.VerifyDeployment(ctx, localChain, cfg, market, pipeline.WalletAddress().Hex()); err != nil {
		panic(err)
	}
	symbols := routeSymbols(required("TITANARB_FORK_HOPS"))
	route, opp := quotedOpportunity(ctx, localChain, market, symbols, amount, minimum)
	req, err := pipeline.BuildRequest(ctx, opp)
	if err != nil {
		panic(err)
	}
	calldata, err := req.Calldata()
	if err != nil {
		panic(err)
	}
	result := pipeline.Process(ctx, opp)
	if result.Decision != "confirmed" || result.Receipt == nil || result.Receipt.Status != "0x1" {
		panic(fmt.Sprintf("GO_FORK_EXECUTION_BLOCKED route=%s decision=%s reason=%s", route.String(), result.Decision, result.Reason))
	}
	if _, err := os.Stat(jsonl); err != nil {
		panic("profit JSONL was not created")
	}
	fmt.Printf("GO_FORK_EXECUTION_PASS route=%s calldata_bytes=%d gas_estimate=%d receipt_status=%s gas_used=%s tx=%s trade_jsonl=%s\n", route.String(), len(calldata), result.Simulation.GasEstimate, result.Receipt.Status, result.Receipt.GasUsed, result.TxHash, jsonl)
}

func quotedOpportunity(ctx context.Context, client *rpc.Client, market config.MarketConfig, symbols []string, amount, minimum *big.Int) (routes.Route, *opportunity.Opportunity) {
	discover := pools.NewDiscoverer(client, market.UniswapFactory, market.CamelotFactory, market.UniswapFeeTiers)
	hops := make([]pools.Pool, 0, len(symbols)-1)
	dexes := routeDEXes(len(symbols) - 1)
	for i := 0; i < len(symbols)-1; i++ {
		found, err := discover.DiscoverPair(ctx, market.Tokens[symbols[i]].Address, market.Tokens[symbols[i+1]].Address)
		if err != nil || len(found) == 0 {
			panic(fmt.Sprintf("missing route edge %s/%s", symbols[i], symbols[i+1]))
		}
		pool, ok := deepestOnDEX(found, dexes[i])
		if !ok {
			panic(fmt.Sprintf("missing required real %s pool for %s/%s", dexes[i], symbols[i], symbols[i+1]))
		}
		hops = append(hops, pool)
	}
	route := routes.Route{Symbols: symbols, Hops: hops}
	evaluator := opportunity.New(client, market, quotes.NewUniswapV3(client, market.UniswapQuoterV2, nil), quotes.NewCamelot(client, market.CamelotQuoter, nil), opportunity.StaticCostModel{L2Fee: big.NewInt(0), L1Fee: big.NewInt(0)}, minimum, 1, nil)
	opp, err := evaluator.Evaluate(ctx, route, amount)
	if err != nil {
		panic(err)
	}
	return route, opp
}
func deepest(found []pools.Pool) pools.Pool {
	best := found[0]
	for _, candidate := range found[1:] {
		if candidate.Liquidity.Cmp(best.Liquidity) > 0 {
			best = candidate
		}
	}
	return best
}

func deepestOnDEX(found []pools.Pool, wanted pools.DEX) (pools.Pool, bool) {
	var best pools.Pool
	ok := false
	for _, candidate := range found {
		if candidate.DEX != wanted {
			continue
		}
		if !ok || candidate.Liquidity.Cmp(best.Liquidity) > 0 {
			best, ok = candidate, true
		}
	}
	return best, ok
}

// These are validation-only real-pool selections. The 2-hop route deliberately
// uses the Camelot return leg conditioned by the fork harness; production route
// discovery is independent and unchanged.
func routeDEXes(hops int) []pools.DEX {
	switch hops {
	case 2:
		return []pools.DEX{pools.UniswapV3, pools.CamelotV3}
	case 3:
		return []pools.DEX{pools.UniswapV3, pools.UniswapV3, pools.UniswapV3}
	case 4:
		return []pools.DEX{pools.UniswapV3, pools.UniswapV3, pools.UniswapV3, pools.UniswapV3}
	default:
		panic("unsupported validation hop count")
	}
}
func routeSymbols(hops string) []string {
	switch hops {
	case "2":
		return []string{"USDC", "WETH", "USDC"}
	case "3":
		return []string{"USDC", "WETH", "ARB", "USDC"}
	case "4":
		return []string{"USDC", "WETH", "ARB", "USDT", "USDC"}
	default:
		panic("TITANARB_FORK_HOPS must be 2, 3, or 4")
	}
}
func required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		panic(name + " is required")
	}
	return value
}
func raw(name string, fallback int64) *big.Int {
	if value := os.Getenv(name); value != "" {
		if parsed, ok := new(big.Int).SetString(value, 10); ok && parsed.Sign() > 0 {
			return parsed
		}
		panic("invalid " + name)
	}
	return big.NewInt(fallback)
}
func local(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" {
		return false
	}
	if strings.EqualFold(u.Hostname(), "localhost") {
		return true
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && ip.IsLoopback()
}
