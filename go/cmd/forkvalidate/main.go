// forkvalidate is a fork-only validation harness. It is never selected by the
// production command and refuses to run without an explicit local RPC URL.
package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/fees"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/routes"
	"github.com/titanarb/titanarb-go/internal/rpc"
)

func main() {
	endpoint := os.Getenv("TITANARB_FORK_RPC_URL")
	if endpoint == "" || endpoint == "http://" {
		panic("TITANARB_FORK_RPC_URL must name a local fork")
	}
	timeout := 90 * time.Second
	if seconds, err := strconv.Atoi(os.Getenv("TITANARB_FORK_TIMEOUT_SECONDS")); err == nil && seconds > 0 {
		timeout = time.Duration(seconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client := rpc.New(endpoint, timeout, 0, nil)
	chain, err := client.ChainID(ctx)
	if err != nil || chain != config.ArbitrumOneChainID {
		panic("fork chain ID validation failed")
	}
	market, err := config.LoadMarketConfig(filepath.Join("..", "config", "arbitrum.json"))
	if err != nil {
		panic(err)
	}
	amount := big.NewInt(10_000_000_000)
	minimum := big.NewInt(5_000_000)
	discover := pools.NewDiscoverer(client, market.UniswapFactory, market.CamelotFactory, market.UniswapFeeTiers)
	uni := quotes.NewUniswapV3(client, market.UniswapQuoterV2, nil)
	camelot := quotes.NewCamelot(client, market.CamelotQuoter, nil)
	evaluator := opportunity.New(client, market, uni, camelot, opportunity.StaticCostModel{L2Fee: big.NewInt(0), L1Fee: big.NewInt(0)}, minimum, 1, nil)
	routesToCheck := map[string][][]string{
		"2":   {{"USDC", "WETH", "USDC"}},
		"3":   {{"USDC", "WETH", "ARB", "USDC"}},
		"4":   {{"USDC", "WETH", "ARB", "USDT", "USDC"}},
		"all": {{"USDC", "WETH", "USDC"}, {"USDC", "WETH", "ARB", "USDC"}, {"USDC", "WETH", "ARB", "USDT", "USDC"}},
	}
	selected := os.Getenv("TITANARB_FORK_HOPS")
	if selected == "" {
		selected = "all"
	}
	for _, symbols := range routesToCheck[selected] {
		hops := make([]pools.Pool, 0, len(symbols)-1)
		for i := 0; i < len(symbols)-1; i++ {
			found, e := discover.DiscoverPair(ctx, market.Tokens[symbols[i]].Address, market.Tokens[symbols[i+1]].Address)
			if e != nil || len(found) == 0 {
				panic(fmt.Sprintf("pool %s/%s: %v", symbols[i], symbols[i+1], e))
			}
			hops = append(hops, choose(found, i))
		}
		route := routes.Route{Symbols: symbols, Hops: hops}
		opp, e := evaluator.Evaluate(ctx, route, amount)
		if e != nil {
			panic(e)
		}
		fmt.Printf("route=%s final_raw=%s expected_prelim_raw=%s\n", route.String(), opp.Hops[len(opp.Hops)-1].AmountOut, opp.ExpectedProfit)
		for i, h := range opp.Hops {
			fmt.Printf("hop=%d dex=%s pool=%s quote_raw=%s\n", i+1, h.Pool.DEX, h.Pool.Address, h.AmountOut)
		}
	}
	_ = fees.NodeInterface
}

// Select the deepest discovered real pool for this edge. This keeps the
// validation route executable without assuming a DEX preference or fabricating
// fee/pool metadata.
func choose(found []pools.Pool, _ int) pools.Pool {
	best := found[0]
	for _, candidate := range found[1:] {
		if candidate.Liquidity.Cmp(best.Liquidity) > 0 {
			best = candidate
		}
	}
	return best
}
