// Package alpha is the Phase 4 read-only intelligence engine. It is deliberately
// unable to sign, simulate execution, or broadcast transactions.
package alpha

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/titanarb/titanarb-go/internal/cache"
	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/graph"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/risk"
	"github.com/titanarb/titanarb-go/internal/search"
	"github.com/titanarb/titanarb-go/internal/universe"
)

type PairDiscoverer interface {
	DiscoverPair(context.Context, string, string) ([]pools.Pool, error)
}
type PremiumSource interface {
	Premium(context.Context, *big.Int) (*big.Int, error)
}

// FeeModel returns already-converted raw units of the route's loan asset. A
// missing conversion is a rejection, not an invitation to compare ETH to tokens.
type FeeModel interface {
	Estimate(context.Context, config.Token, graph.Route, []Hop) (l2, l1 *big.Int, err error)
}

type Hop struct {
	Pool                pools.Pool
	AmountIn, AmountOut *big.Int
	QuoteGas            uint64
	Fee                 uint32
}
type QuoteChain struct {
	Asset               string
	Route               graph.Route
	AmountIn, AmountOut *big.Int
	Hops                []Hop
	Timestamp           time.Time
}
type Candidate struct {
	QuoteChain
	AavePremium, L2Fee, L1Fee, NetProfit *big.Int
	Risk                                 risk.Assessment
	Profitable                           bool
	Reason                               string
}
type RefreshStats struct {
	Pairs    int
	Pools    int
	Failures int
	Routes   int
}

type Engine struct {
	Registry     *universe.Registry
	Graph        *graph.Graph
	Cache        *cache.PoolCache
	discoverer   PairDiscoverer
	uni, camelot quotes.Quoter
	workers      int
}

func New(tokens map[string]config.Token, discoverer PairDiscoverer, uni, camelot quotes.Quoter, workers int) *Engine {
	if workers < 1 {
		workers = 1
	}
	return &Engine{Registry: universe.New(tokens), Graph: graph.New(tokens), Cache: cache.NewPoolCache(), discoverer: discoverer, uni: uni, camelot: camelot, workers: workers}
}

// Refresh dynamically discovers every pair from the configured token universe.
// Tokens originate in configuration (not code), so adding an allow-listed token
// changes the graph without a token-identity rule or a source edit.
func (e *Engine) Refresh(ctx context.Context, maxHops, perAssetRoutes int) (RefreshStats, error) {
	if e.discoverer == nil {
		return RefreshStats{}, fmt.Errorf("pool discoverer is required")
	}
	names := e.Registry.TokenNames()
	type pair struct{ a, b config.Token }
	pairs := make([]pair, 0, len(names)*(len(names)-1)/2)
	for i, aName := range names {
		for _, bName := range names[i+1:] {
			a, _ := e.Registry.Token(aName)
			b, _ := e.Registry.Token(bName)
			pairs = append(pairs, pair{a, b})
		}
	}
	type found struct {
		pools []pools.Pool
		err   error
	}
	jobs := make(chan pair)
	results := make(chan found, len(pairs))
	var wg sync.WaitGroup
	for i := 0; i < e.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				pools, err := e.discoverer.DiscoverPair(ctx, p.a.Address, p.b.Address)
				results <- found{pools, err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, p := range pairs {
			select {
			case jobs <- p:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	stats := RefreshStats{Pairs: len(pairs)}
	for result := range results {
		if result.err != nil {
			stats.Failures++
			continue
		}
		for _, pool := range result.pools {
			e.Cache.Put(pool)
			if e.Registry.AddPool(pool) && e.Graph.UpsertPool(pool, .5) {
				stats.Pools++
			}
		}
	}
	stats.Routes = len(e.Graph.Cycles(maxHops, perAssetRoutes))
	return stats, nil
}

func (e *Engine) Routes(maxHops, perAssetRoutes int) []graph.Route {
	return e.Graph.Cycles(maxHops, perAssetRoutes)
}

type QuoteResult struct {
	Chain QuoteChain
	Err   error
}

// QuoteMany evaluates independent graph routes with a bounded worker pool.
// Hops inside one route remain sequential because each real quote output is the
// next hop's exact input.
func (e *Engine) QuoteMany(ctx context.Context, routes []graph.Route, amounts map[string]*big.Int) []QuoteResult {
	work := search.Map(ctx, routes, e.workers, func(ctx context.Context, route graph.Route) (QuoteChain, error) {
		return e.Quote(ctx, route.Assets[0], route, amounts[route.Assets[0]])
	})
	result := make([]QuoteResult, 0, len(work))
	for _, item := range work {
		result = append(result, QuoteResult{Chain: item.Result, Err: item.Err})
	}
	return result
}

func (e *Engine) Quote(ctx context.Context, asset string, route graph.Route, amount *big.Int) (QuoteChain, error) {
	if amount == nil || amount.Sign() <= 0 || len(route.Hops) < 2 || len(route.Assets) != len(route.Hops)+1 || route.Assets[0] != asset || route.Assets[len(route.Assets)-1] != asset {
		return QuoteChain{}, fmt.Errorf("invalid asset-continuous cycle")
	}
	current := new(big.Int).Set(amount)
	hops := make([]Hop, 0, len(route.Hops))
	for i, pool := range route.Hops {
		in, ok := e.Registry.Token(route.Assets[i])
		if !ok {
			return QuoteChain{}, fmt.Errorf("unknown input token %s", route.Assets[i])
		}
		out, ok := e.Registry.Token(route.Assets[i+1])
		if !ok {
			return QuoteChain{}, fmt.Errorf("unknown output token %s", route.Assets[i+1])
		}
		if !pool.Supports(in.Address, out.Address) {
			return QuoteChain{}, fmt.Errorf("pool/token continuity failure at hop %d", i+1)
		}
		quoter := e.selectQuoter(pool)
		if quoter == nil {
			return QuoteChain{}, fmt.Errorf("unsupported dex at hop %d", i+1)
		}
		result, err := quoter.Quote(ctx, quotes.Request{TokenIn: in.Address, TokenOut: out.Address, AmountIn: current, Pool: pool})
		if err != nil {
			return QuoteChain{}, fmt.Errorf("quote hop %d: %w", i+1, err)
		}
		if result.AmountOut == nil || result.AmountOut.Sign() <= 0 {
			return QuoteChain{}, fmt.Errorf("zero output at hop %d", i+1)
		}
		hops = append(hops, Hop{Pool: pool, AmountIn: new(big.Int).Set(current), AmountOut: new(big.Int).Set(result.AmountOut), QuoteGas: result.EstimatedGas, Fee: result.Fee})
		current = result.AmountOut
	}
	return QuoteChain{Asset: asset, Route: route, AmountIn: new(big.Int).Set(amount), AmountOut: new(big.Int).Set(current), Hops: hops, Timestamp: time.Now().UTC()}, nil
}

// Evaluate never marks a candidate profitable unless both asset-denominated fee
// components and Aave premium are provided by authoritative callers.
func (e *Engine) Evaluate(ctx context.Context, chain QuoteChain, premium PremiumSource, fees FeeModel, minProfit *big.Int, assessment risk.Assessment) (Candidate, error) {
	candidate := Candidate{QuoteChain: chain, Risk: assessment, AavePremium: big.NewInt(0), L2Fee: big.NewInt(0), L1Fee: big.NewInt(0), NetProfit: new(big.Int).Neg(big.NewInt(1))}
	asset, ok := e.Registry.Token(chain.Asset)
	if !ok {
		return candidate, fmt.Errorf("unknown loan asset")
	}
	if premium == nil || fees == nil || minProfit == nil {
		candidate.Reason = "missing authoritative premium, fee conversion, or minimum profit"
		return candidate, nil
	}
	p, err := premium.Premium(ctx, chain.AmountIn)
	if err != nil {
		return candidate, err
	}
	l2, l1, err := fees.Estimate(ctx, asset, chain.Route, chain.Hops)
	if err != nil {
		return candidate, err
	}
	if p == nil || l2 == nil || l1 == nil {
		candidate.Reason = "incomplete authoritative cost"
		return candidate, nil
	}
	minimum := new(big.Int).Mul(minProfit, big.NewInt(int64(assessment.RequiredProfitMultiplierBPS)))
	minimum.Div(minimum, big.NewInt(10_000))
	net := new(big.Int).Sub(chain.AmountOut, chain.AmountIn)
	net.Sub(net, p)
	net.Sub(net, l2)
	net.Sub(net, l1)
	candidate.AavePremium, candidate.L2Fee, candidate.L1Fee, candidate.NetProfit = new(big.Int).Set(p), new(big.Int).Set(l2), new(big.Int).Set(l1), net
	candidate.Profitable = net.Cmp(minimum) > 0
	if !candidate.Profitable {
		candidate.Reason = "net profit does not exceed risk-adjusted minimum"
	}
	return candidate, nil
}

func (e *Engine) selectQuoter(pool pools.Pool) quotes.Quoter {
	if pool.DEX == pools.UniswapV3 {
		return e.uni
	}
	if pool.DEX == pools.CamelotV3 {
		return e.camelot
	}
	return nil
}

func SortCandidates(candidates []Candidate) {
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].NetProfit.Cmp(candidates[j].NetProfit) > 0 })
}
