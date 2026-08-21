// Package market connects pool discovery and bounded opportunity evaluation.
package market

import (
	"context"
	"math/big"
	"sync"

	"github.com/titanarb/titanarb-go/internal/cache"
	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
)

type Engine struct {
	market           config.MarketConfig
	discoverer       *pools.Discoverer
	cache            *cache.PoolCache
	evaluator        *opportunity.Engine
	Events           <-chan opportunity.Event
	metrics          *metrics.Metrics
	discoveryWorkers int
	amount           *big.Int
}

func New(market config.MarketConfig, discoverer *pools.Discoverer, cache *cache.PoolCache, evaluator *opportunity.Engine, amount *big.Int, workers int, metrics *metrics.Metrics) *Engine {
	if workers < 1 {
		workers = 1
	}
	return &Engine{market: market, discoverer: discoverer, cache: cache, evaluator: evaluator, Events: evaluator.Events, amount: new(big.Int).Set(amount), discoveryWorkers: workers, metrics: metrics}
}

// Cycle refreshes the finite configured pair set, builds only 2--4-hop routes,
// then evaluates those routes. It has no signing or transaction capability.
func (e *Engine) Cycle(ctx context.Context) ([]routes.Route, error) {
	symbols := append([]string{e.market.BaseAsset}, e.market.IntermediateTokens...)
	type job struct{ from, to string }
	jobs := make(chan job)
	results := make(chan struct {
		pair  routes.Pair
		found []pools.Pool
		err   error
	}, len(symbols)*(len(symbols)-1))
	var wg sync.WaitGroup
	for i := 0; i < e.discoveryWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				a := e.market.Tokens[j.from]
				b := e.market.Tokens[j.to]
				found, err := e.discoverer.DiscoverPair(ctx, a.Address, b.Address)
				results <- struct {
					pair  routes.Pair
					found []pools.Pool
					err   error
				}{routes.Pair{From: j.from, To: j.to}, found, err}
			}
		}()
	}
	go func() {
		for _, from := range symbols {
			for _, to := range symbols {
				if from != to {
					select {
					case jobs <- job{from, to}:
					case <-ctx.Done():
						close(jobs)
						wg.Wait()
						close(results)
						return
					}
				}
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()
	byPair := make(map[routes.Pair][]pools.Pool)
	for result := range results {
		if result.err != nil {
			continue
		}
		byPair[result.pair] = result.found
		for _, pool := range result.found {
			e.cache.Put(pool)
			if e.metrics != nil {
				e.metrics.IncPoolsDiscovered()
				if pool.DEX == pools.UniswapV3 {
					e.metrics.IncUniswapPools()
				} else if pool.DEX == pools.CamelotV3 {
					e.metrics.IncCamelotPools()
				}
			}
		}
	}
	routesFound := routes.Build(e.market.BaseAsset, e.market.IntermediateTokens, byPair, 256)
	e.evaluator.EvaluateMany(ctx, routesFound, e.amount)
	return routesFound, nil
}
