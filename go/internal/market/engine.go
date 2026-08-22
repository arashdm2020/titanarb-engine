// Package market connects pool discovery and bounded opportunity evaluation.
package market

import (
	"context"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/titanarb/titanarb-go/internal/cache"
	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/graph"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/optimizer"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
	"github.com/titanarb/titanarb-go/internal/volatility"
)

type Engine struct {
	market           config.MarketConfig
	discoverer       *pools.Discoverer
	cache            *cache.PoolCache
	evaluator        *opportunity.Engine
	Events           <-chan opportunity.Event
	metrics          *metrics.Metrics
	discoveryWorkers int
	amounts          map[string]*big.Int
	statsMu          sync.RWMutex
	activePools      uint64
	cycles           uint64
	volatility       *volatility.Tracker
	optimizer        optimizer.Optimizer
	liquidity        func(context.Context, string) (*big.Int, error)
	routeCache       []routes.Route
	cyclesSinceFull  uint64
	lastMaxHops      int
	lastMaxRoutes    int
	lastStateBlock   uint64
}

type Snapshot struct {
	ActivePools uint64
	Cycles      uint64
}

type CycleReport struct {
	StateBlock        uint64
	FullReconcile     bool
	DirtyPools        uint64
	RoutesRecomputed  uint64
	RoutesReused      uint64
	QuoteDuration     time.Duration
	OptimizerDuration time.Duration
	OptimizerRuns     uint64
	OptimizerSamples  uint64
	RoutesEvaluated   uint64
	Duration          time.Duration
	Routes            []routes.Route
}

const fullReconcileEvery = 240
const maxOptimizerRoutesPerAsset = 2
const maxEvaluationRoutesPerAsset = 12
const optimizerSamplesPerRoute = 8

func New(market config.MarketConfig, discoverer *pools.Discoverer, cache *cache.PoolCache, evaluator *opportunity.Engine, amount *big.Int, workers int, metrics *metrics.Metrics) *Engine {
	amounts := map[string]*big.Int{}
	if amount != nil && market.BaseAsset != "" {
		amounts[market.BaseAsset] = new(big.Int).Set(amount)
	}
	return NewWithAmounts(market, discoverer, cache, evaluator, amounts, workers, metrics, nil)
}

// NewWithAmounts accepts exact raw units for each loan asset. Callers must not
// reuse a raw amount across tokens with different decimals or values.
func NewWithAmounts(market config.MarketConfig, discoverer *pools.Discoverer, cache *cache.PoolCache, evaluator *opportunity.Engine, amounts map[string]*big.Int, workers int, metrics *metrics.Metrics, liquidity func(context.Context, string) (*big.Int, error)) *Engine {
	if workers < 1 {
		workers = 1
	}
	validAmounts := make(map[string]*big.Int, len(amounts))
	for symbol, amount := range amounts {
		if _, configured := market.Tokens[symbol]; configured && amount != nil && amount.Sign() > 0 {
			validAmounts[symbol] = new(big.Int).Set(amount)
		}
	}
	return &Engine{market: market, discoverer: discoverer, cache: cache, evaluator: evaluator, Events: evaluator.Events, amounts: validAmounts, discoveryWorkers: workers, metrics: metrics, volatility: volatility.NewTracker(), optimizer: optimizer.Optimizer{Workers: workers}, liquidity: liquidity}
}

// Cycle refreshes the deployed executor's complete allow-listed universe,
// builds 2--4-hop routes from every possible loan asset, then evaluates them.
// It has no signing or transaction capability.
func (e *Engine) Cycle(ctx context.Context) ([]routes.Route, error) {
	return e.CycleWithLimits(ctx, 4, 256)
}

// CycleWithLimits keeps the execution-safe 2--4-hop contract boundary while
// allowing an operator risk profile to adjust search breadth at the next
// market cycle. It does not alter slippage, repayment, or execution gates.
func (e *Engine) CycleWithLimits(ctx context.Context, maxHops, maxRoutes int) ([]routes.Route, error) {
	return e.CycleWithSearch(ctx, maxHops, maxRoutes, 1)
}

// CycleWithSearch additionally consumes a risk-profile supplied volatility
// weight. It prioritizes dynamic pool movement and cross-venue dispersion but
// never excludes an asset due to volatility.
func (e *Engine) CycleWithSearch(ctx context.Context, maxHops, maxRoutes int, volatilityWeight float64) ([]routes.Route, error) {
	report, err := e.CycleAt(ctx, 0, maxHops, maxRoutes, volatilityWeight)
	return report.Routes, err
}

// CycleAt performs a periodic full reconciliation and incremental mutable-state
// refreshes in between. A zero stateBlock keeps backwards-compatible latest
// reads for tests and callers without a WSS trigger.
func (e *Engine) CycleAt(ctx context.Context, stateBlock uint64, maxHops, maxRoutes int, volatilityWeight float64) (CycleReport, error) {
	started := time.Now()
	report := CycleReport{StateBlock: stateBlock}
	if maxHops < 2 {
		maxHops = 2
	}
	if maxHops > 4 {
		maxHops = 4
	}
	if maxRoutes < 1 {
		return report, nil
	}
	full := len(e.routeCache) == 0 || e.cyclesSinceFull >= fullReconcileEvery || maxHops != e.lastMaxHops || maxRoutes != e.lastMaxRoutes
	var dirty map[string]struct{}
	var err error
	if full {
		e.routeCache, err = e.fullReconcile(ctx, stateBlock, maxHops, maxRoutes)
		if err != nil {
			return report, err
		}
		e.cyclesSinceFull = 0
		e.lastMaxHops, e.lastMaxRoutes = maxHops, maxRoutes
		report.FullReconcile = true
		dirty = make(map[string]struct{}, len(e.cache.Snapshot()))
		for _, pool := range e.cache.Snapshot() {
			dirty[strings.ToLower(pool.Address)] = struct{}{}
		}
	} else {
		fromBlock := stateBlock
		if e.lastStateBlock > 0 && e.lastStateBlock < stateBlock {
			fromBlock = e.lastStateBlock + 1
		}
		dirty, err = e.incrementalRefresh(ctx, fromBlock, stateBlock)
		if err != nil {
			return report, err
		}
		e.cyclesSinceFull++
	}
	report.DirtyPools = uint64(len(dirty))
	active := liquidPools(e.cache.Snapshot())
	signals := map[string]volatility.Signal{}
	if e.volatility != nil {
		signals = e.volatility.Observe(active)
	}
	routesFound := refreshRoutes(e.routeCache, active)
	optimizerStarted := time.Now()
	sort.SliceStable(routesFound, func(i, j int) bool {
		left := routeVolatility(routesFound[i], signals, volatilityWeight)
		right := routeVolatility(routesFound[j], signals, volatilityWeight)
		if left != right {
			return left > right
		}
		return routesFound[i].String() < routesFound[j].String()
	})
	affected := routesAffectedBy(routesFound, dirty, report.FullReconcile)
	report.OptimizerDuration = time.Since(optimizerStarted)
	report.RoutesRecomputed = uint64(len(affected))
	if len(routesFound) > len(affected) {
		report.RoutesReused = uint64(len(routesFound) - len(affected))
	}
	e.statsMu.Lock()
	e.activePools = uint64(len(active))
	e.cycles = uint64(len(routesFound))
	e.statsMu.Unlock()
	quoteStarted := time.Now()
	evaluation := e.evaluate(ctx, affected)
	report.QuoteDuration = time.Since(quoteStarted)
	report.OptimizerRuns = evaluation.OptimizerRuns
	report.OptimizerSamples = evaluation.OptimizerSamples
	report.RoutesEvaluated = evaluation.RoutesEvaluated
	report.Duration = time.Since(started)
	report.Routes = routesFound
	if stateBlock > 0 {
		e.lastStateBlock = stateBlock
	}
	return report, nil
}

func (e *Engine) fullReconcile(ctx context.Context, stateBlock uint64, maxHops, maxRoutes int) ([]routes.Route, error) {
	symbols := e.market.ExecutionAssets()
	type job struct{ from, to string }
	jobs := make(chan job)
	results := make(chan struct {
		pair  routes.Pair
		found []pools.Pool
		err   error
	}, len(symbols)*(len(symbols)-1)/2)
	var wg sync.WaitGroup
	for i := 0; i < e.discoveryWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				a := e.market.Tokens[j.from]
				b := e.market.Tokens[j.to]
				var found []pools.Pool
				var err error
				if stateBlock > 0 {
					found, err = e.discoverer.DiscoverPairAt(ctx, a.Address, b.Address, stateBlock)
				} else {
					found, err = e.discoverer.DiscoverPair(ctx, a.Address, b.Address)
				}
				results <- struct {
					pair  routes.Pair
					found []pools.Pool
					err   error
				}{routes.Pair{From: j.from, To: j.to}, found, err}
			}
		}()
	}
	go func() {
		for i, from := range symbols {
			for _, to := range symbols[i+1:] {
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
		close(jobs)
		wg.Wait()
		close(results)
	}()
	byPair := make(map[routes.Pair][]pools.Pool)
	var discovered []pools.Pool
	var firstErr error
	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		byPair[result.pair] = result.found
		byPair[routes.Pair{From: result.pair.To, To: result.pair.From}] = result.found
		for _, pool := range result.found {
			discovered = append(discovered, pool)
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
	if firstErr != nil {
		return nil, firstErr
	}
	e.cache.Replace(discovered)

	routesFound := routes.BuildAll(symbols, byPair, maxRoutes)

	filtered := routesFound[:0]
	for _, route := range routesFound {
		if len(route.Hops) <= maxHops {
			filtered = append(filtered, route)
		}
	}
	routesFound = filtered
	return routesFound, nil
}

func (e *Engine) incrementalRefresh(ctx context.Context, fromBlock, stateBlock uint64) (map[string]struct{}, error) {
	current := e.cache.Snapshot()
	if len(current) == 0 {
		return map[string]struct{}{}, nil
	}
	addresses := make([]string, 0, len(current))
	for _, pool := range current {
		addresses = append(addresses, pool.Address)
	}
	dirty, logErr := e.discoverer.ChangedPoolAddressesAt(ctx, addresses, fromBlock, stateBlock)
	refresh := current
	if logErr == nil {
		refresh = poolsByAddress(current, dirty)
	} else {
		// Correctness fallback: if log-based invalidation is unavailable, refresh
		// and recompute every cached pool rather than risk a stale candidate.
		dirty = make(map[string]struct{}, len(current))
		for _, pool := range current {
			dirty[strings.ToLower(pool.Address)] = struct{}{}
		}
	}
	if len(refresh) == 0 {
		return dirty, nil
	}
	type result struct {
		before, after pools.Pool
		err           error
	}
	jobs := make(chan pools.Pool)
	results := make(chan result, len(refresh))
	var wg sync.WaitGroup
	for i := 0; i < e.discoveryWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pool := range jobs {
				updated, err := e.discoverer.RefreshPoolAt(ctx, pool, stateBlock)
				results <- result{before: pool, after: updated, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, pool := range refresh {
			select {
			case jobs <- pool:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	for result := range results {
		if result.err != nil {
			return nil, result.err
		}
		e.cache.Put(result.after)
		if logErr != nil && poolChanged(result.before, result.after) {
			dirty[strings.ToLower(result.after.Address)] = struct{}{}
		}
	}
	return dirty, nil
}

func poolsByAddress(all []pools.Pool, selected map[string]struct{}) []pools.Pool {
	result := make([]pools.Pool, 0, len(selected))
	for _, pool := range all {
		if _, ok := selected[strings.ToLower(pool.Address)]; ok {
			result = append(result, pool)
		}
	}
	return result
}

type evaluationReport struct {
	OptimizerRuns    uint64
	OptimizerSamples uint64
	RoutesEvaluated  uint64
}

func (e *Engine) evaluate(ctx context.Context, candidates []routes.Route) evaluationReport {
	report := evaluationReport{}
	byAsset := make(map[string][]routes.Route)
	for _, route := range candidates {
		byAsset[route.Symbols[0]] = append(byAsset[route.Symbols[0]], route)
	}
	for asset, candidates := range byAsset {
		amount, ok := e.amounts[asset]
		if !ok {
			// Discovery remains asset-agnostic, but economics are unsafe without
			// an asset-specific notional. Skip rather than reuse another token's
			// raw units or manufacture a USD conversion.
			continue
		}
		evaluationCandidates := boundedRoutes(candidates, maxEvaluationRoutesPerAsset)
		report.RoutesEvaluated += uint64(len(evaluationCandidates))
		evaluated := e.evaluateCurrent(ctx, evaluationCandidates, amount)

		optimizerCandidates := selectOptimizerCandidates(evaluated, maxOptimizerRoutesPerAsset)
		optimized := e.optimizeRoutes(ctx, asset, optimizerCandidates, amount)
		report.OptimizerRuns += optimized.OptimizerRuns
		report.OptimizerSamples += optimized.OptimizerSamples
	}
	return report
}

type evaluatedRoute struct {
	route routes.Route
	score *big.Int
}

func (e *Engine) evaluateCurrent(ctx context.Context, candidates []routes.Route, amount *big.Int) []evaluatedRoute {
	if len(candidates) == 0 {
		return nil
	}
	jobs := make(chan routes.Route)
	results := make(chan evaluatedRoute, len(candidates))
	var wg sync.WaitGroup
	for i := 0; i < e.discoveryWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for route := range jobs {
				op, err := e.evaluator.Evaluate(ctx, route, amount)
				if err != nil || op == nil || op.ExpectedProfit == nil {
					continue
				}
				results <- evaluatedRoute{route: route, score: new(big.Int).Set(op.ExpectedProfit)}
			}
		}()
	}
	for _, route := range candidates {
		select {
		case jobs <- route:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			close(results)
			return collectEvaluated(results)
		}
	}
	close(jobs)
	wg.Wait()
	close(results)
	return collectEvaluated(results)
}

func collectEvaluated(results <-chan evaluatedRoute) []evaluatedRoute {
	var evaluated []evaluatedRoute
	for result := range results {
		evaluated = append(evaluated, result)
	}
	return evaluated
}

func selectOptimizerCandidates(evaluated []evaluatedRoute, limit int) []routes.Route {
	if limit < 1 || len(evaluated) == 0 {
		return nil
	}
	sort.SliceStable(evaluated, func(i, j int) bool {
		left, right := evaluated[i].score, evaluated[j].score
		if left == nil || right == nil {
			return right == nil
		}
		if left.Cmp(right) != 0 {
			return left.Cmp(right) > 0
		}
		return evaluated[i].route.String() < evaluated[j].route.String()
	})
	if len(evaluated) > limit {
		evaluated = evaluated[:limit]
	}
	selected := make([]routes.Route, 0, len(evaluated))
	for _, item := range evaluated {
		selected = append(selected, item.route)
	}
	return selected
}

func poolChanged(before, after pools.Pool) bool {
	return !bigEqual(before.SqrtPriceX96, after.SqrtPriceX96) || !bigEqual(before.Liquidity, after.Liquidity)
}

func bigEqual(a, b *big.Int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Cmp(b) == 0
}

func refreshRoutes(cached []routes.Route, active []pools.Pool) []routes.Route {
	byAddress := make(map[string]pools.Pool, len(active))
	for _, pool := range active {
		byAddress[strings.ToLower(pool.Address)] = pool
	}
	result := make([]routes.Route, 0, len(cached))
	for _, route := range cached {
		copy := routes.Route{Symbols: append([]string(nil), route.Symbols...), Hops: make([]pools.Pool, len(route.Hops))}
		valid := true
		for i, pool := range route.Hops {
			updated, ok := byAddress[strings.ToLower(pool.Address)]
			if !ok {
				valid = false
				break
			}
			copy.Hops[i] = updated
		}
		if valid {
			result = append(result, copy)
		}
	}
	return result
}

func liquidPools(all []pools.Pool) []pools.Pool {
	result := make([]pools.Pool, 0, len(all))
	for _, pool := range all {
		if pool.Liquidity != nil && pool.Liquidity.Sign() > 0 {
			result = append(result, pool)
		}
	}
	return result
}

func routesAffectedBy(all []routes.Route, dirty map[string]struct{}, full bool) []routes.Route {
	if full {
		return all
	}
	result := make([]routes.Route, 0)
	for _, route := range all {
		for _, pool := range route.Hops {
			if _, ok := dirty[strings.ToLower(pool.Address)]; ok {
				result = append(result, route)
				break
			}
		}
	}
	return result
}

func routeVolatility(route routes.Route, signals map[string]volatility.Signal, weight float64) float64 {
	if len(route.Hops) == 0 {
		return 0
	}
	var total float64
	for _, pool := range route.Hops {
		total += signals[strings.ToLower(pool.Address)].Score(weight)
	}
	return total / float64(len(route.Hops))
}

func (e *Engine) Snapshot() Snapshot {
	e.statsMu.RLock()
	defer e.statsMu.RUnlock()
	return Snapshot{ActivePools: e.activePools, Cycles: e.cycles}
}

type optimizerAdapter struct {
	evaluator *opportunity.Engine
}

func (a optimizerAdapter) Evaluate(ctx context.Context, asset string, route graph.Route, amount *big.Int) (optimizer.Evaluation, error) {
	r := routes.Route{
		Symbols: append([]string(nil), route.Assets...),
		Hops:    append([]pools.Pool(nil), route.Hops...),
	}

	op, err := a.evaluator.EvaluateSilent(ctx, r, amount)
	if err != nil {
		return optimizer.Evaluation{}, err
	}

	return optimizer.Evaluation{
		Amount:     new(big.Int).Set(amount),
		NetProfit:  new(big.Int).Set(op.ExpectedProfit),
		Executable: op.ExpectedProfit != nil && op.ExpectedProfit.Sign() > 0,
	}, nil
}

func boundedRoutes(candidates []routes.Route, limit int) []routes.Route {
	if limit < 1 || len(candidates) <= limit {
		return candidates
	}
	return candidates[:limit]
}

func (e *Engine) optimizeRoutes(ctx context.Context, asset string, candidates []routes.Route, current *big.Int) evaluationReport {
	report := evaluationReport{}
	if current == nil || current.Sign() <= 0 {
		return report
	}

	max := new(big.Int).Mul(current, big.NewInt(10))

	if e.liquidity != nil {
		if available, err := e.liquidity(ctx, asset); err == nil {
			max = capLoanMax(max, available)
		}
	}

	for _, route := range candidates {
		graphRoute := graph.Route{
			Assets: append([]string(nil), route.Symbols...),
			Hops:   append([]pools.Pool(nil), route.Hops...),
		}

		report.OptimizerRuns++
		best, err := e.optimizer.Optimize(
			ctx,
			optimizerAdapter{evaluator: e.evaluator},
			asset,
			graphRoute,
			func() optimizer.Range {
				min := new(big.Int).Div(new(big.Int).Set(current), big.NewInt(10))
				if min.Sign() <= 0 {
					min = new(big.Int).Set(current)
				}
				return optimizer.Range{
					Min:     min,
					Max:     max,
					Samples: optimizerSamplesPerRoute,
				}
			}(),
		)
		report.OptimizerSamples += uint64(best.Evaluated)

		if err != nil {
			continue
		}

		// Optimizer-selected amount becomes the evaluation amount only when
		// economics are positive. Negative candidates remain rejected.
		if best.Amount != nil && best.NetProfit != nil && best.NetProfit.Sign() > 0 {
			optimizedRoute := routes.Route{
				Symbols: append([]string(nil), route.Symbols...),
				Hops:    append([]pools.Pool(nil), route.Hops...),
			}
			_, _ = e.evaluator.Evaluate(ctx, optimizedRoute, best.Amount)
		}
	}
	return report
}

func capLoanMax(configured, available *big.Int) *big.Int {
	if configured == nil {
		return nil
	}
	max := new(big.Int).Set(configured)
	if available != nil && available.Sign() > 0 && available.Cmp(max) < 0 {
		max = new(big.Int).Set(available)
	}
	return max
}
