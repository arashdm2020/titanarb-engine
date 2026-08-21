// mainnetobserve performs a bounded, production-read-only mainnet observation.
// It deliberately disables broadcasts in memory before constructing the
// execution pipeline. It neither signs nor sends a transaction.
package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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

const (
	defaultDuration = 45 * time.Second
	maxDuration     = 5 * time.Minute
)

type rejected struct {
	route  string
	profit *big.Int
}

func main() {
	started := time.Now()
	duration := observationDuration()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		fail(err)
	}
	// These process-local settings make signing and broadcasting unreachable,
	// even when the shared environment is configured for live operation.
	cfg.DryRun = true
	cfg.ExecutionMode = "dry_run"
	cfg.BroadcastEnabled = false
	market, err := config.LoadMarketConfig(marketConfigPath())
	if err != nil {
		fail(err)
	}
	amount, err := requiredRawEnv("TITANARB_MARKET_AMOUNT_RAW")
	if err != nil {
		fail(err)
	}
	minimum, err := executionMinProfit()
	if err != nil {
		fail(err)
	}

	m := metrics.New()
	chain := rpc.New(cfg.HTTPRPCURL, 15*time.Second, 1, m)
	chainID, err := chain.ChainID(ctx)
	if err != nil || chainID != config.ArbitrumOneChainID {
		fail(fmt.Errorf("Arbitrum One chain validation failed: %w", err))
	}
	feeService := fees.New(chain, market.ArbGasInfo, market.ETHUSDFeed, safetyMarginBPS())
	gate := safety.New(chain, market.ETHUSDFeed, market.SequencerUptimeFeed, time.Duration(cfg.PriceMaxStalenessSeconds)*time.Second, time.Duration(cfg.SequencerGraceSeconds)*time.Second)
	pipeline, err := execution.NewPipeline(cfg, market, chain, feeService, gate, minimum, profit.New(filepath.Join("runtime", "mainnet-observation.jsonl")), m)
	if err != nil {
		fail(err)
	}
	if err := execution.VerifyDeployment(ctx, chain, cfg, market, pipeline.WalletAddress().Hex()); err != nil {
		fail(err)
	}

	discoverer := pools.NewDiscoverer(chain, market.UniswapFactory, market.CamelotFactory, market.UniswapFeeTiers)
	evaluator := opportunity.New(chain, market, quotes.NewUniswapV3(chain, market.UniswapQuoterV2, m), quotes.NewCamelot(chain, market.CamelotQuoter, m), opportunity.ArbitrumCostModel{Service: feeService}, minimum, 4, m)

	var (
		blocks                = map[uint64]struct{}{}
		cycles, routesChecked uint64
		preliminaryCandidates uint64
		simulationSuccesses   uint64
		closest               []rejected
	)
	for ctx.Err() == nil && simulationSuccesses == 0 {
		block, err := chain.BlockNumber(ctx)
		if err != nil {
			m.IncRPCErrors()
			break
		}
		if _, seen := blocks[block]; !seen {
			blocks[block] = struct{}{}
			m.IncBlocks()
		}
		candidates, err := discoverRoutes(ctx, discoverer, market)
		if err != nil {
			m.IncRPCErrors()
			break
		}
		cycles++
		for _, route := range candidates {
			if ctx.Err() != nil {
				break
			}
			routesChecked++
			opp, err := evaluator.Evaluate(ctx, route, amount)
			if err != nil {
				continue
			}
			if opp.ExpectedProfit.Cmp(minimum) <= 0 {
				closest = append(closest, rejected{route: route.String(), profit: new(big.Int).Set(opp.ExpectedProfit)})
				continue
			}
			preliminaryCandidates++
			result := pipeline.Process(ctx, opp)
			if result.Decision == "simulated" {
				simulationSuccesses++
				break
			}
		}
		if ctx.Err() == nil && simulationSuccesses == 0 {
			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Second):
			}
		}
	}
	sort.Slice(closest, func(i, j int) bool { return closest[i].profit.Cmp(closest[j].profit) > 0 })
	if len(closest) > 3 {
		closest = closest[:3]
	}
	snapshot := m.Snapshot()
	fmt.Printf("MAINNET_OBSERVATION duration=%s cycles=%d blocks=%d routes=%d quotes=%d preliminary_candidates=%d simulation_attempts=%d simulation_successes=%d simulation_failures=%d post_gas_rejections=%d quote_failures=%d broadcasts=%d\n",
		time.Since(started).Round(time.Millisecond), cycles, len(blocks), routesChecked, snapshot.Quotes, preliminaryCandidates, snapshot.SimulationAttempts, simulationSuccesses, snapshot.SimulationFailures, snapshot.PostGasRejections, snapshot.QuoteFailures, snapshot.TransactionsBroadcast)
	for _, item := range closest {
		fmt.Printf("CLOSEST_REJECTED route=%s expected_net_profit_raw=%s min_profit_raw=%s\n", item.route, item.profit, minimum)
	}
	if snapshot.TransactionsBroadcast != 0 {
		fail(fmt.Errorf("read-only guard failure: broadcasts=%d", snapshot.TransactionsBroadcast))
	}
	_ = os.Stdout.Sync()
}

// discoverRoutes intentionally bounds observation to the previously validated
// production token cycle family. It still reads native factory/pool state on
// each cycle and never manufactures a pool or quote.
func discoverRoutes(ctx context.Context, discoverer *pools.Discoverer, market config.MarketConfig) ([]routes.Route, error) {
	pairs := [][2]string{{"USDC", "WETH"}, {"WETH", "ARB"}, {"ARB", "USDC"}, {"ARB", "USDT"}, {"USDT", "USDC"}}
	selected := make(map[string]pools.Pool, len(pairs))
	type result struct {
		pair  [2]string
		found []pools.Pool
		err   error
	}
	jobs := make(chan [2]string)
	results := make(chan result, len(pairs))
	workers := 4
	if workers > len(pairs) {
		workers = len(pairs)
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pair := range jobs {
				in, inOK := market.Tokens[pair[0]]
				out, outOK := market.Tokens[pair[1]]
				if !inOK || !outOK {
					results <- result{pair: pair, err: fmt.Errorf("configured token missing for %s/%s", pair[0], pair[1])}
					continue
				}
				found, err := discoverer.DiscoverPair(ctx, in.Address, out.Address)
				results <- result{pair: pair, found: found, err: err}
			}
		}()
	}
	go func() {
		for _, pair := range pairs {
			select {
			case jobs <- pair:
			case <-ctx.Done():
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()
	for item := range results {
		if item.err != nil {
			return nil, fmt.Errorf("discover %s/%s: %w", item.pair[0], item.pair[1], item.err)
		}
		if len(item.found) == 0 {
			return nil, fmt.Errorf("no liquid supported pool for %s/%s", item.pair[0], item.pair[1])
		}
		selected[pairKey(item.pair[0], item.pair[1])] = deepest(item.found)
	}
	return []routes.Route{
		{Symbols: []string{"USDC", "WETH", "USDC"}, Hops: []pools.Pool{selected[pairKey("USDC", "WETH")], selected[pairKey("USDC", "WETH")]}},
		{Symbols: []string{"USDC", "WETH", "ARB", "USDC"}, Hops: []pools.Pool{selected[pairKey("USDC", "WETH")], selected[pairKey("WETH", "ARB")], selected[pairKey("ARB", "USDC")]}},
		{Symbols: []string{"USDC", "WETH", "ARB", "USDT", "USDC"}, Hops: []pools.Pool{selected[pairKey("USDC", "WETH")], selected[pairKey("WETH", "ARB")], selected[pairKey("ARB", "USDT")], selected[pairKey("USDT", "USDC")]}},
	}, nil
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

func pairKey(a, b string) string {
	if a < b {
		return a + "/" + b
	}
	return b + "/" + a
}

func observationDuration() time.Duration {
	value := strings.TrimSpace(os.Getenv("TITANARB_OBSERVATION_DURATION"))
	if value == "" {
		return defaultDuration
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return defaultDuration
	}
	if duration > maxDuration {
		return maxDuration
	}
	return duration
}

func requiredRawEnv(name string) (*big.Int, error) {
	value, ok := new(big.Int).SetString(strings.TrimSpace(os.Getenv(name)), 10)
	if !ok || value.Sign() <= 0 {
		return nil, fmt.Errorf("%s must be a positive raw token amount", name)
	}
	return value, nil
}

func executionMinProfit() (*big.Int, error) {
	if value, err := requiredRawEnv("TITANARB_EXECUTION_MIN_PROFIT_RAW"); err == nil {
		return value, nil
	}
	return requiredRawEnv("TITANARB_MARKET_MIN_PROFIT_RAW")
}

func safetyMarginBPS() uint64 {
	value, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("GAS_SAFETY_MARGIN_BPS")), 10, 64)
	if err != nil {
		return 1_000
	}
	return value
}

func marketConfigPath() string {
	if configured := os.Getenv("TITANARB_MARKET_CONFIG"); configured != "" {
		return configured
	}
	for _, candidate := range []string{"config/arbitrum.json", filepath.Join("..", "config", "arbitrum.json")} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "config/arbitrum.json"
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "mainnet observation error:", err)
	os.Exit(1)
}
