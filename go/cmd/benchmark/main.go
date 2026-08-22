// titanarb-benchmark runs one deterministic, read-only three-quote workload.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/rpc"
)

type result struct {
	Runtime          string  `json:"runtime"`
	Block            uint64  `json:"block"`
	Route            string  `json:"route"`
	RouteLimit       int     `json:"route_limit"`
	QuoteCount       int     `json:"quote_count"`
	PoolDiscoveryS   float64 `json:"pool_discovery_s"`
	QuoteS           float64 `json:"quote_s"`
	RouteEvaluationS float64 `json:"route_evaluation_s"`
	TotalCycleS      float64 `json:"total_cycle_s"`
	RPCCalls         uint64  `json:"rpc_calls"`
	CacheHits        uint64  `json:"cache_hits"`
	MemoryPeakBytes  uint64  `json:"memory_peak_bytes"`
	CPUProcessS      float64 `json:"cpu_process_s"`
	AmountOut        string  `json:"amount_out"`
}

func cpuTime() float64 {
	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return 0
	}
	times, err := proc.Times()
	if err != nil {
		return 0
	}
	return times.User + times.System
}

func main() {
	cpuStart := cpuTime()
	ctx := context.Background()
	cfg, err := config.LoadMarketConfig(os.Getenv("BENCH_CONFIG"))
	if err != nil {
		panic(err)
	}
	m := metrics.New()
	client := rpc.New(os.Getenv("BENCH_RPC_URL"), 15*time.Second, 0, m)
	symbols := []string{"USDC", "WETH", "ARB", "USDC"}
	discoverer := pools.NewDiscoverer(client, cfg.UniswapFactory, cfg.CamelotFactory, cfg.UniswapFeeTiers)
	started := time.Now()
	discoveryStarted := time.Now()
	hops := make([]pools.Pool, 0, 3)
	for i := 0; i < 3; i++ {
		a, b := cfg.Tokens[symbols[i]], cfg.Tokens[symbols[i+1]]
		found, err := discoverer.DiscoverPair(ctx, a.Address, b.Address)
		if err != nil {
			panic(err)
		}
		var chosen *pools.Pool
		for j := range found {
			if found[j].DEX == pools.UniswapV3 && (chosen == nil || found[j].Liquidity.Cmp(chosen.Liquidity) > 0) {
				chosen = &found[j]
			}
		}
		if chosen == nil {
			panic("expected Uniswap pool")
		}
		hops = append(hops, *chosen)
	}
	discoveryDuration := time.Since(discoveryStarted)

	uni := quotes.NewUniswapV3(client, cfg.UniswapQuoterV2, m)
	quoteStarted := time.Now()
	amount := big.NewInt(1_000_000_000)
	for i, pool := range hops {
		quote, err := uni.Quote(ctx, quotes.Request{
			TokenIn:  cfg.Tokens[symbols[i]].Address,
			TokenOut: cfg.Tokens[symbols[i+1]].Address,
			AmountIn: amount,
			Pool:     pool,
		})
		if err != nil {
			panic(err)
		}
		amount = quote.AmountOut
	}
	quoteDuration := time.Since(quoteStarted)
	block, _ := client.BlockNumber(ctx)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	snapshot := m.Snapshot()
	_ = json.NewEncoder(os.Stdout).Encode(result{
		Runtime:          "go",
		Block:            block,
		Route:            "USDC -> WETH -> ARB -> USDC",
		RouteLimit:       1,
		QuoteCount:       3,
		PoolDiscoveryS:   discoveryDuration.Seconds(),
		QuoteS:           quoteDuration.Seconds(),
		RouteEvaluationS: quoteDuration.Seconds(),
		TotalCycleS:      time.Since(started).Seconds(),
		RPCCalls:         snapshot.RPCCalls,
		CacheHits:        snapshot.CacheHits,
		MemoryPeakBytes:  mem.TotalAlloc,
		CPUProcessS:      cpuTime() - cpuStart,
		AmountOut:        amount.String(),
	})
	fmt.Fprint(os.Stderr, "")
}
