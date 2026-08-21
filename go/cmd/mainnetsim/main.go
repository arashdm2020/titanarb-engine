// mainnetsim runs one explicit Arbitrum One execution-pipeline validation.
// It is deliberately read-only: broadcast is disabled in-memory before the
// pipeline is constructed and this command never sends a raw transaction.
package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/execution"
	"github.com/titanarb/titanarb-go/internal/fees"
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
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	// These assignments are intentionally local to this process. They make a
	// raw-transaction broadcast impossible even if a live .env is present.
	cfg.DryRun = true
	cfg.ExecutionMode = "dry_run"
	cfg.BroadcastEnabled = false
	market, err := config.LoadMarketConfig(filepath.Join("..", "config", "arbitrum.json"))
	if err != nil {
		panic(err)
	}
	chain := rpc.New(cfg.HTTPRPCURL, 30*time.Second, 1, nil)
	if chainID, err := chain.ChainID(ctx); err != nil || chainID != config.ArbitrumOneChainID {
		panic("Arbitrum One chain validation failed")
	}
	feeService := fees.New(chain, market.ArbGasInfo, market.ETHUSDFeed, 1_000)
	gate := safety.New(chain, market.ETHUSDFeed, market.SequencerUptimeFeed, time.Duration(cfg.PriceMaxStalenessSeconds)*time.Second, time.Duration(cfg.SequencerGraceSeconds)*time.Second)
	pipeline, err := execution.NewPipeline(cfg, market, chain, feeService, gate, big.NewInt(5_000_000), profit.New(filepath.Join("runtime", "mainnet-simulation.jsonl")), metrics.New())
	if err != nil {
		panic(err)
	}
	if err := execution.VerifyDeployment(ctx, chain, cfg, market, pipeline.WalletAddress().Hex()); err != nil {
		panic(err)
	}
	discoverer := pools.NewDiscoverer(chain, market.UniswapFactory, market.CamelotFactory, market.UniswapFeeTiers)
	symbols := []string{"USDC", "WETH", "USDC"}
	hops := make([]pools.Pool, 0, 2)
	for i := 0; i < len(symbols)-1; i++ {
		found, err := discoverer.DiscoverPair(ctx, market.Tokens[symbols[i]].Address, market.Tokens[symbols[i+1]].Address)
		if err != nil || len(found) == 0 {
			panic("real mainnet pool discovery failed")
		}
		hops = append(hops, deepest(found))
	}
	route := routes.Route{Symbols: symbols, Hops: hops}
	evaluator := opportunity.New(chain, market, quotes.NewUniswapV3(chain, market.UniswapQuoterV2, nil), quotes.NewCamelot(chain, market.CamelotQuoter, nil), opportunity.StaticCostModel{L2Fee: big.NewInt(0), L1Fee: big.NewInt(0)}, big.NewInt(5_000_000), 1, nil)
	opp, err := evaluator.Evaluate(ctx, route, big.NewInt(10_000_000_000))
	if err != nil {
		panic(err)
	}
	req, err := pipeline.BuildRequest(ctx, opp)
	if err != nil {
		panic(err)
	}
	calldata, err := req.Calldata()
	if err != nil {
		panic(err)
	}
	result := pipeline.Process(ctx, opp)
	if result.Decision == "confirmed" || result.Decision == "sent" {
		panic("mainnet read-only guard failure")
	}
	gas := uint64(0)
	if result.Simulation != nil {
		gas = result.Simulation.GasEstimate
	}
	fmt.Printf("MAINNET_READ_ONLY_SIMULATION route=%s decision=%s gas_estimate=%d calldata_bytes=%d reason=%s\n", route.String(), result.Decision, gas, len(calldata), result.Reason)
	_ = os.Stdout.Sync()
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
