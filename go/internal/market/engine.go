// Package market connects pool discovery and bounded opportunity evaluation.
package market

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/titanarb/titanarb-go/internal/cache"
	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/graph"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/nearmiss"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/optimizer"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
	"github.com/titanarb/titanarb-go/internal/volatility"
)

type Engine struct {
	market            config.MarketConfig
	discoverer        *pools.Discoverer
	cache             *cache.PoolCache
	evaluator         *opportunity.Engine
	Events            <-chan opportunity.Event
	metrics           *metrics.Metrics
	discoveryWorkers  int
	amounts           map[string]*big.Int
	statsMu           sync.RWMutex
	activePools       uint64
	cycles            uint64
	volatility        *volatility.Tracker
	optimizer         optimizer.Optimizer
	liquidity         func(context.Context, string) (*big.Int, error)
	routeCache        []routes.Route
	universeAssets    []string
	dynamicAssets     []string
	universeDecisions []string
	cyclesSinceFull   uint64
	lastMaxHops       int
	lastMaxRoutes     int
	lastStateBlock    uint64
	routeFailures     map[string]uint64
	routeMemory       *nearmiss.RouteMemory
}

type Snapshot struct {
	ActivePools uint64
	Cycles      uint64
}

type CycleReport struct {
	StateBlock              uint64
	FullReconcile           bool
	DirtyPools              uint64
	RoutesRecomputed        uint64
	RoutesReused            uint64
	QuoteDuration           time.Duration
	OptimizerDuration       time.Duration
	OptimizerRuns           uint64
	OptimizerSamples        uint64
	RoutesEvaluated         uint64
	Duration                time.Duration
	Routes                  []routes.Route
	UniverseAssets          []string
	DynamicAssets           []string
	UniverseDecisions       []string
	RouteCountBefore        int
	RouteCountAfter         int
	RoutesByHop             map[int]int
	DEXRoutes               map[string]int
	TopNearMisses           []map[string]any
	RouteScores             map[string]int
	OptimizerBudget         map[string]int
	RejectionReasons        map[string]int
	ExploitSelected         uint64
	ExploreSelected         uint64
	MemoryRoutes            int
	AvgPreQuoteScore        int64
	CrossVenueQuoted        uint64
	SameDEXQuoted           uint64
	RoutesConsidered        uint64
	QuoteAgeBlocks          uint64
	PreQuoteRanking         bool
	RPCCallsByStage         map[string]uint64
	QuoteCacheHits          uint64
	QuoteDedupHits          uint64
	OptimizerSaved          uint64
	RoutesSkippedByPreQuote uint64
}

const fullReconcileEvery = 240
const incrementalRefreshBatchBlocks = 8
const maxOptimizerRoutesPerAsset = 2
const maxEvaluationRoutesPerAsset = 12
const optimizerSamplesPerRoute = 8

type SearchOptions struct {
	EvaluationRoutesPerAsset int
	OptimizerRoutesPerAsset  int
	OptimizerSamplesPerRoute int
	DisablePreQuoteRanking   bool
	ExploreRatioBPS          int
}

func DefaultSearchOptions() SearchOptions {
	return SearchOptions{
		EvaluationRoutesPerAsset: maxEvaluationRoutesPerAsset,
		OptimizerRoutesPerAsset:  maxOptimizerRoutesPerAsset,
		OptimizerSamplesPerRoute: optimizerSamplesPerRoute,
		DisablePreQuoteRanking:   false,
		ExploreRatioBPS:          2_000,
	}
}

func (o SearchOptions) Normalized() SearchOptions {
	defaults := DefaultSearchOptions()
	if o.EvaluationRoutesPerAsset < 1 {
		o.EvaluationRoutesPerAsset = defaults.EvaluationRoutesPerAsset
	}
	if o.OptimizerRoutesPerAsset < 1 {
		o.OptimizerRoutesPerAsset = defaults.OptimizerRoutesPerAsset
	}
	if o.OptimizerSamplesPerRoute < 2 {
		o.OptimizerSamplesPerRoute = defaults.OptimizerSamplesPerRoute
	}
	if o.ExploreRatioBPS < 0 {
		o.ExploreRatioBPS = 0
	}
	if o.ExploreRatioBPS == 0 {
		o.ExploreRatioBPS = defaults.ExploreRatioBPS
	}
	if o.ExploreRatioBPS > 5_000 {
		o.ExploreRatioBPS = 5_000
	}
	return o
}

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
	return &Engine{market: market, discoverer: discoverer, cache: cache, evaluator: evaluator, Events: evaluator.Events, amounts: validAmounts, discoveryWorkers: workers, metrics: metrics, volatility: volatility.NewTracker(), optimizer: optimizer.Optimizer{Workers: workers}, liquidity: liquidity, universeAssets: market.ExecutionAssets(), routeFailures: make(map[string]uint64), routeMemory: nearmiss.NewRouteMemory(8192)}
}

func (e *Engine) SetUniverseTelemetry(active, dynamic, decisions []string) {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	e.universeAssets = append([]string(nil), active...)
	e.dynamicAssets = append([]string(nil), dynamic...)
	e.universeDecisions = append([]string(nil), decisions...)
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

// ForcedOpportunity builds one configured route for a one-shot operational
// experiment. It bypasses market discovery ranking only; quote reads, fee
// calculation, min-profit economics, and the downstream execution pipeline
// remain unchanged.
func (e *Engine) ForcedOpportunity(ctx context.Context, stateBlock uint64, symbols []string, amount *big.Int) (*opportunity.Opportunity, error) {
	if len(symbols) < 3 || len(symbols) > 5 {
		return nil, fmt.Errorf("forced route must contain a 2-4 hop cycle")
	}
	if symbols[0] == "" || symbols[len(symbols)-1] != symbols[0] {
		return nil, fmt.Errorf("forced route must start and end with the loan asset")
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("forced amount must be positive")
	}
	for _, symbol := range symbols {
		if _, ok := e.market.Tokens[symbol]; !ok {
			return nil, fmt.Errorf("forced route token %s is not configured", symbol)
		}
	}
	choices := make([][]pools.Pool, 0, len(symbols)-1)
	for i := 0; i < len(symbols)-1; i++ {
		in, out := e.market.Tokens[symbols[i]], e.market.Tokens[symbols[i+1]]
		var found []pools.Pool
		var err error
		if stateBlock > 0 {
			found, err = e.discoverer.DiscoverPairAt(ctx, in.Address, out.Address, stateBlock)
		} else {
			found, err = e.discoverer.DiscoverPair(ctx, in.Address, out.Address)
		}
		if err != nil {
			return nil, err
		}
		found = liquidPools(found)
		if len(found) == 0 {
			return nil, fmt.Errorf("no liquid pool for forced hop %s -> %s", symbols[i], symbols[i+1])
		}
		choices = append(choices, found)
	}
	var best *opportunity.Opportunity
	for _, route := range forcedRouteCombinations(symbols, choices, 32) {
		op, err := e.evaluator.EvaluateSilent(ctx, route, amount)
		if err != nil || op == nil || op.ExpectedProfit == nil {
			continue
		}
		op.SourceBlock = forcedRouteSourceBlock(op.Route)
		if op.SourceBlock == 0 {
			op.SourceBlock = stateBlock
		}
		if best == nil || op.ExpectedProfit.Cmp(best.ExpectedProfit) > 0 {
			best = op
		}
	}
	if best == nil {
		return nil, fmt.Errorf("forced route produced no quotable pool combination")
	}
	return best, nil
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
	return e.CycleAtWithSearchOptions(ctx, stateBlock, maxHops, maxRoutes, volatilityWeight, DefaultSearchOptions())
}

// CycleAtWithSearchOptions lets runtime risk profiles widen or narrow the
// read-only market search envelope without touching execution safety gates.
func (e *Engine) CycleAtWithSearchOptions(ctx context.Context, stateBlock uint64, maxHops, maxRoutes int, volatilityWeight float64, options SearchOptions) (CycleReport, error) {
	options = options.Normalized()
	started := time.Now()
	report := CycleReport{StateBlock: stateBlock}
	report.RouteCountBefore = len(e.routeCache)
	e.statsMu.RLock()
	report.UniverseAssets = append([]string(nil), e.universeAssets...)
	report.DynamicAssets = append([]string(nil), e.dynamicAssets...)
	report.UniverseDecisions = append([]string(nil), e.universeDecisions...)
	e.statsMu.RUnlock()
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
	if !full && shouldDeferIncrementalRefresh(e.lastStateBlock, stateBlock, incrementalRefreshBatchBlocks) {
		routesFound := refreshRoutes(e.routeCache, liquidPools(e.cache.Snapshot()))
		report.RoutesReused = uint64(len(routesFound))
		report.Duration = time.Since(started)
		report.Routes = routesFound
		report.RouteCountAfter = len(routesFound)
		report.RoutesByHop = routesByHop(routesFound)
		report.DEXRoutes = routeDEXDiversity(routesFound)
		e.statsMu.Lock()
		e.activePools = uint64(len(e.cache.Snapshot()))
		e.cycles = uint64(len(routesFound))
		e.statsMu.Unlock()
		return report, nil
	}
	var dirty map[string]struct{}
	var err error
	rpcAtStart := e.rpcCalls()
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
		if shouldAdvanceFullReconcileCounter(dirty) {
			e.cyclesSinceFull++
		}
	}
	rpcAfterRefresh := e.rpcCalls()
	report.DirtyPools = uint64(len(dirty))
	active := liquidPools(e.cache.Snapshot())
	signals := map[string]volatility.Signal{}
	if e.volatility != nil {
		signals = e.volatility.Observe(active)
	}
	routesFound := refreshRoutes(e.routeCache, active)
	sort.SliceStable(routesFound, func(i, j int) bool {
		left := routeVolatility(routesFound[i], signals, volatilityWeight)
		right := routeVolatility(routesFound[j], signals, volatilityWeight)
		if left != right {
			return left > right
		}
		return routesFound[i].String() < routesFound[j].String()
	})
	affected := routesAffectedBy(routesFound, dirty, report.FullReconcile)
	e.reviveDirtyRouteMemory(affected, stateBlock)
	report.RoutesRecomputed = uint64(len(affected))
	if len(routesFound) > len(affected) {
		report.RoutesReused = uint64(len(routesFound) - len(affected))
	}
	e.statsMu.Lock()
	e.activePools = uint64(len(active))
	e.cycles = uint64(len(routesFound))
	e.statsMu.Unlock()
	quoteStarted := time.Now()
	if e.evaluator != nil {
		e.evaluator.ResetQuoteCache(stateBlock)
	}
	evaluation := e.evaluate(ctx, affected, options, signals, volatilityWeight, stateBlock)
	rpcAfterEvaluation := e.rpcCalls()
	report.QuoteDuration = time.Since(quoteStarted)
	report.OptimizerDuration = evaluation.OptimizerDuration
	report.OptimizerRuns = evaluation.OptimizerRuns
	report.OptimizerSamples = evaluation.OptimizerSamples
	report.RoutesEvaluated = evaluation.RoutesEvaluated
	report.TopNearMisses = nearmiss.Snapshot(evaluation.NearMisses, 10)
	report.RouteScores = nearmiss.Distribution(evaluation.NearMisses)
	report.OptimizerBudget = evaluation.OptimizerBudget
	report.RejectionReasons = nearmiss.RejectionReasons(evaluation.NearMisses)
	report.ExploitSelected = evaluation.ExploitSelected
	report.ExploreSelected = evaluation.ExploreSelected
	report.MemoryRoutes = evaluation.MemoryRoutes
	report.AvgPreQuoteScore = evaluation.AvgPreQuoteScore
	report.CrossVenueQuoted = evaluation.CrossVenueQuoted
	report.SameDEXQuoted = evaluation.SameDEXQuoted
	report.RoutesConsidered = evaluation.RoutesConsidered
	report.QuoteAgeBlocks = evaluation.QuoteAgeBlocks
	report.PreQuoteRanking = !options.DisablePreQuoteRanking
	report.RPCCallsByStage = map[string]uint64{
		"pool_refresh": safeUint64Delta(rpcAfterRefresh, rpcAtStart),
		"evaluation":   safeUint64Delta(rpcAfterEvaluation, rpcAfterRefresh),
	}
	report.QuoteCacheHits = evaluation.QuoteCacheHits
	report.QuoteDedupHits = evaluation.QuoteDedupHits
	report.OptimizerSaved = evaluation.OptimizerSaved
	report.RoutesSkippedByPreQuote = evaluation.RoutesSkippedByPreQuote
	report.Duration = time.Since(started)
	report.Routes = routesFound
	report.RouteCountAfter = len(routesFound)
	report.RoutesByHop = routesByHop(routesFound)
	report.DEXRoutes = routeDEXDiversity(routesFound)
	if stateBlock > 0 {
		e.lastStateBlock = stateBlock
	}
	return report, nil
}

func shouldAdvanceFullReconcileCounter(dirty map[string]struct{}) bool {
	return len(dirty) > 0
}

func shouldDeferIncrementalRefresh(lastStateBlock, stateBlock uint64, batchBlocks uint64) bool {
	if batchBlocks < 2 || lastStateBlock == 0 || stateBlock <= lastStateBlock {
		return false
	}
	return stateBlock-lastStateBlock < batchBlocks
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

	routesFound := routes.BuildForStarts(e.loanAssets(), symbols, byPair, maxRoutes)

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
		address := strings.ToLower(result.after.Address)
		changed := poolChanged(result.before, result.after)
		if logErr != nil && changed {
			dirty[address] = struct{}{}
		}
		if logErr == nil && !changed {
			delete(dirty, address)
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
	OptimizerRuns           uint64
	OptimizerSamples        uint64
	RoutesEvaluated         uint64
	OptimizerDuration       time.Duration
	NearMisses              []nearmiss.Record
	OptimizerBudget         map[string]int
	ExploitSelected         uint64
	ExploreSelected         uint64
	MemoryRoutes            int
	AvgPreQuoteScore        int64
	CrossVenueQuoted        uint64
	SameDEXQuoted           uint64
	RoutesConsidered        uint64
	QuoteAgeBlocks          uint64
	QuoteCacheHits          uint64
	QuoteDedupHits          uint64
	OptimizerSaved          uint64
	RoutesSkippedByPreQuote uint64
}

func (e *Engine) evaluate(ctx context.Context, candidates []routes.Route, options SearchOptions, signals map[string]volatility.Signal, volatilityWeight float64, stateBlock uint64) evaluationReport {
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
		evaluationCandidates, selection := e.selectEvaluationCandidates(candidates, options, signals, volatilityWeight, stateBlock)
		report.RoutesConsidered += uint64(len(candidates))
		if len(candidates) > len(evaluationCandidates) {
			report.RoutesSkippedByPreQuote += uint64(len(candidates) - len(evaluationCandidates))
		}
		report.ExploitSelected += uint64(selection.Exploit)
		report.ExploreSelected += uint64(selection.Explore)
		report.CrossVenueQuoted += uint64(selection.CrossVenue)
		report.SameDEXQuoted += uint64(selection.SameDEX)
		report.AvgPreQuoteScore = combineAverages(report.AvgPreQuoteScore, report.RoutesEvaluated, selection.AvgScore, uint64(len(evaluationCandidates)))
		report.RoutesEvaluated += uint64(len(evaluationCandidates))
		evaluated := e.evaluateCurrent(ctx, evaluationCandidates, amount, stateBlock)
		e.updateRouteFailures(evaluated)
		e.updateRouteMemory(evaluated, stateBlock)
		for _, item := range evaluated {
			if item.nearMiss != nil && item.nearMiss.GapToProfit != nil && item.nearMiss.GapToProfit.Sign() > 0 {
				report.NearMisses = append(report.NearMisses, *item.nearMiss)
			}
			if item.quoteAgeBlocks > report.QuoteAgeBlocks {
				report.QuoteAgeBlocks = item.quoteAgeBlocks
			}
		}

		optimizerCandidates := selectOptimizerCandidates(evaluated, options.OptimizerRoutesPerAsset)
		optimizerStarted := time.Now()
		optimized := e.optimizeRoutes(ctx, asset, optimizerCandidates, amount, options.OptimizerSamplesPerRoute)
		report.OptimizerDuration += time.Since(optimizerStarted)
		report.OptimizerRuns += optimized.OptimizerRuns
		report.OptimizerSamples += optimized.OptimizerSamples
		report.OptimizerSaved += optimized.OptimizerSaved
		if report.OptimizerBudget == nil {
			report.OptimizerBudget = make(map[string]int)
		}
		for key, value := range optimized.OptimizerBudget {
			report.OptimizerBudget[key] += value
		}
	}
	if e.routeMemory != nil {
		report.MemoryRoutes = e.routeMemory.Len()
	}
	if e.evaluator != nil {
		stats := e.evaluator.QuoteCacheStats()
		report.QuoteCacheHits = stats.Hits
		report.QuoteDedupHits = stats.DedupHits
	}
	return report
}

type evaluatedRoute struct {
	route          routes.Route
	score          *big.Int
	nearMiss       *nearmiss.Record
	routeScore     int64
	quoteAgeBlocks uint64
}

func (e *Engine) evaluateCurrent(ctx context.Context, candidates []routes.Route, amount *big.Int, stateBlock uint64) []evaluatedRoute {
	if len(candidates) == 0 {
		return nil
	}
	failures := e.routeFailureSnapshot()
	jobs := make(chan routes.Route)
	results := make(chan evaluatedRoute, len(candidates))
	var wg sync.WaitGroup
	for i := 0; i < e.discoveryWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for route := range jobs {
				started := time.Now()
				op, err := e.evaluator.Evaluate(ctx, route, amount)
				if err != nil || op == nil || op.ExpectedProfit == nil {
					continue
				}
				reason := ""
				if op.MinProfit != nil && op.ExpectedProfit.Cmp(op.MinProfit) <= 0 {
					reason = "profitability threshold not met"
				}
				record := nearmiss.FromOpportunity(op, reason, time.Since(started), failures[routeKey(route)])
				results <- evaluatedRoute{route: route, score: new(big.Int).Set(op.ExpectedProfit), nearMiss: &record, routeScore: record.Score, quoteAgeBlocks: quoteAgeBlocks(stateBlock, forcedRouteSourceBlock(route))}
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

type evaluationSelection struct {
	Exploit    int
	Explore    int
	CrossVenue int
	SameDEX    int
	AvgScore   int64
}

type scoredRoute struct {
	route        routes.Route
	key          string
	score        int64
	observations uint64
	crossVenue   bool
}

func (e *Engine) selectEvaluationCandidates(candidates []routes.Route, options SearchOptions, signals map[string]volatility.Signal, volatilityWeight float64, stateBlock uint64) ([]routes.Route, evaluationSelection) {
	limit := options.EvaluationRoutesPerAsset
	if options.DisablePreQuoteRanking {
		selected := boundedRoutes(dedupeRoutes(candidates), limit)
		return selected, summarizeLegacySelection(selected)
	}
	if e.routeMemory == nil {
		e.routeMemory = nearmiss.NewRouteMemory(8192)
	}
	if limit < 1 || len(candidates) == 0 {
		return nil, evaluationSelection{}
	}
	scored := make([]scoredRoute, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, route := range candidates {
		key := routeKey(route)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		cross := nearmiss.CrossVenue(route)
		stats, _ := e.routeMemory.Stats(key)
		score := e.routeMemory.PreQuoteScore(nearmiss.PreQuoteInput{
			Key:          key,
			CrossVenue:   cross,
			Volatility:   routeVolatility(route, signals, volatilityWeight),
			CurrentBlock: stateBlock,
		})
		scored = append(scored, scoredRoute{route: route, key: key, score: score, observations: stats.Observations, crossVenue: cross})
	}
	if len(scored) == 0 {
		return nil, evaluationSelection{}
	}
	if limit > len(scored) {
		limit = len(scored)
	}
	exploreSlots := (limit * options.ExploreRatioBPS) / 10_000
	if options.ExploreRatioBPS > 0 && exploreSlots == 0 && limit > 1 {
		exploreSlots = 1
	}
	if exploreSlots > limit {
		exploreSlots = limit
	}
	exploitSlots := limit - exploreSlots
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].key < scored[j].key
	})
	selected := make([]routes.Route, 0, limit)
	selectedKeys := make(map[string]struct{}, limit)
	add := func(item scoredRoute) bool {
		if len(selected) >= limit {
			return false
		}
		if _, ok := selectedKeys[item.key]; ok {
			return false
		}
		selectedKeys[item.key] = struct{}{}
		selected = append(selected, item.route)
		return true
	}
	selection := evaluationSelection{}
	for _, item := range scored {
		if selection.Exploit >= exploitSlots {
			break
		}
		if add(item) {
			selection.Exploit++
		}
	}
	explore := append([]scoredRoute(nil), scored...)
	sort.SliceStable(explore, func(i, j int) bool {
		if explore[i].observations != explore[j].observations {
			return explore[i].observations < explore[j].observations
		}
		if explore[i].score != explore[j].score {
			return explore[i].score > explore[j].score
		}
		return explore[i].key < explore[j].key
	})
	for _, item := range explore {
		if selection.Explore >= exploreSlots {
			break
		}
		if add(item) {
			selection.Explore++
		}
	}
	for _, item := range scored {
		if len(selected) >= limit {
			break
		}
		if add(item) {
			selection.Exploit++
		}
	}
	var total int64
	for _, route := range selected {
		key := routeKey(route)
		for _, item := range scored {
			if item.key == key {
				total += item.score
				if item.crossVenue {
					selection.CrossVenue++
				} else {
					selection.SameDEX++
				}
				break
			}
		}
	}
	if len(selected) > 0 {
		selection.AvgScore = total / int64(len(selected))
	}
	return selected, selection
}

func summarizeLegacySelection(selected []routes.Route) evaluationSelection {
	selection := evaluationSelection{Exploit: len(selected)}
	for _, route := range selected {
		if nearmiss.CrossVenue(route) {
			selection.CrossVenue++
		} else {
			selection.SameDEX++
		}
	}
	return selection
}

func dedupeRoutes(candidates []routes.Route) []routes.Route {
	if len(candidates) < 2 {
		return candidates
	}
	seen := make(map[string]struct{}, len(candidates))
	out := make([]routes.Route, 0, len(candidates))
	for _, route := range candidates {
		key := routeKey(route)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out
}

type optimizerCandidate struct {
	route     routes.Route
	score     int64
	netProfit *big.Int
	minProfit *big.Int
	gap       *big.Int
}

func selectOptimizerCandidates(evaluated []evaluatedRoute, limit int) []optimizerCandidate {
	if limit < 1 || len(evaluated) == 0 {
		return nil
	}
	sort.SliceStable(evaluated, func(i, j int) bool {
		if evaluated[i].routeScore != evaluated[j].routeScore {
			return evaluated[i].routeScore > evaluated[j].routeScore
		}
		left, right := evaluated[i].score, evaluated[j].score
		if left == nil || right == nil {
			return right == nil
		}
		if left.Cmp(right) != 0 {
			return left.Cmp(right) > 0
		}
		return evaluated[i].route.String() < evaluated[j].route.String()
	})
	selected := make([]optimizerCandidate, 0, len(evaluated))
	for _, item := range evaluated {
		if !optimizerEligible(item) {
			continue
		}
		candidate := optimizerCandidate{route: item.route, score: item.routeScore}
		if item.nearMiss != nil {
			if item.nearMiss.NetProfit != nil {
				candidate.netProfit = new(big.Int).Set(item.nearMiss.NetProfit)
			}
			if item.nearMiss.MinProfit != nil {
				candidate.minProfit = new(big.Int).Set(item.nearMiss.MinProfit)
			}
			if item.nearMiss.GapToProfit != nil {
				candidate.gap = new(big.Int).Set(item.nearMiss.GapToProfit)
			}
		}
		selected = append(selected, candidate)
		if len(selected) >= limit {
			break
		}
	}
	return selected
}

func optimizerEligible(item evaluatedRoute) bool {
	if item.score != nil && item.score.Sign() > 0 {
		return true
	}
	if item.nearMiss == nil || item.nearMiss.GapToProfit == nil || item.nearMiss.MinProfit == nil || item.nearMiss.MinProfit.Sign() <= 0 {
		return false
	}
	// Optimizer work is expensive. Run it only when the first-pass economics are
	// close enough that sizing can plausibly matter. Far-below routes remain in
	// near-miss memory and pre-quote ranking, but do not receive deep samples.
	limit := new(big.Int).Mul(item.nearMiss.MinProfit, big.NewInt(3))
	return item.nearMiss.GapToProfit.Cmp(limit) <= 0
}

func (e *Engine) routeFailureSnapshot() map[string]uint64 {
	e.statsMu.RLock()
	defer e.statsMu.RUnlock()
	out := make(map[string]uint64, len(e.routeFailures))
	for key, value := range e.routeFailures {
		out[key] = value
	}
	return out
}

func (e *Engine) updateRouteFailures(evaluated []evaluatedRoute) {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	if e.routeFailures == nil {
		e.routeFailures = make(map[string]uint64)
	}
	for _, item := range evaluated {
		if item.score == nil {
			continue
		}
		key := routeKey(item.route)
		if item.nearMiss != nil && item.nearMiss.GapToProfit != nil && item.nearMiss.GapToProfit.Sign() > 0 {
			e.routeFailures[key]++
			continue
		}
		delete(e.routeFailures, key)
	}
}

func (e *Engine) updateRouteMemory(evaluated []evaluatedRoute, block uint64) {
	if e.routeMemory == nil {
		e.routeMemory = nearmiss.NewRouteMemory(8192)
	}
	for _, item := range evaluated {
		if item.nearMiss == nil {
			continue
		}
		e.routeMemory.Observe(routeKey(item.route), *item.nearMiss, block)
	}
}

func (e *Engine) reviveDirtyRouteMemory(affected []routes.Route, block uint64) {
	if e.routeMemory == nil || len(affected) == 0 {
		return
	}
	for _, route := range affected {
		e.routeMemory.MarkDirty(routeKey(route), block)
	}
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

func (e *Engine) loanAssets() []string {
	assets := make([]string, 0, len(e.amounts))
	for asset, amount := range e.amounts {
		if amount != nil && amount.Sign() > 0 {
			assets = append(assets, asset)
		}
	}
	sort.Strings(assets)
	if len(assets) == 0 {
		return e.market.ExecutionAssets()
	}
	return assets
}

func routesByHop(input []routes.Route) map[int]int {
	out := make(map[int]int)
	for _, route := range input {
		out[len(route.Hops)]++
	}
	return out
}

func routeDEXDiversity(input []routes.Route) map[string]int {
	out := make(map[string]int)
	for _, route := range input {
		seen := make(map[pools.DEX]struct{})
		for _, hop := range route.Hops {
			seen[hop.DEX] = struct{}{}
		}
		switch len(seen) {
		case 0:
			out["unknown"]++
		case 1:
			for dexName := range seen {
				out[string(dexName)]++
			}
		default:
			out["cross_venue"]++
		}
	}
	return out
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

func combineAverages(current int64, currentCount uint64, next int64, nextCount uint64) int64 {
	total := currentCount + nextCount
	if total == 0 {
		return 0
	}
	return int64((int64(currentCount)*current + int64(nextCount)*next) / int64(total))
}

func quoteAgeBlocks(stateBlock, sourceBlock uint64) uint64 {
	if stateBlock == 0 || sourceBlock == 0 || stateBlock <= sourceBlock {
		return 0
	}
	return stateBlock - sourceBlock
}

func (e *Engine) rpcCalls() uint64 {
	if e == nil || e.metrics == nil {
		return 0
	}
	return e.metrics.Snapshot().RPCCalls
}

func safeUint64Delta(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func (e *Engine) optimizeRoutes(ctx context.Context, asset string, candidates []optimizerCandidate, current *big.Int, samples int) evaluationReport {
	report := evaluationReport{OptimizerBudget: make(map[string]int)}
	if current == nil || current.Sign() <= 0 {
		return report
	}

	max := new(big.Int).Mul(current, big.NewInt(10))

	if e.liquidity != nil {
		if available, err := e.liquidity(ctx, asset); err == nil {
			max = capLoanMax(max, available)
		}
	}

	for _, candidate := range candidates {
		route := candidate.route
		allocatedSamples := samplesForScore(candidate.score, samples)
		report.OptimizerBudget[routeKey(route)] = allocatedSamples
		graphRoute := graph.Route{
			Assets: append([]string(nil), route.Symbols...),
			Hops:   append([]pools.Pool(nil), route.Hops...),
		}

		probeSamples := minInt(allocatedSamples, 3)
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
					Samples: probeSamples,
				}
			}(),
		)
		report.OptimizerSamples += uint64(best.Evaluated)

		if err != nil {
			continue
		}

		if shouldStopOptimizerAfterProbe(candidate, best, current) || probeSamples >= allocatedSamples {
			if allocatedSamples > best.Evaluated {
				report.OptimizerSaved += uint64(allocatedSamples - best.Evaluated)
			}
			if best.Amount != nil && best.NetProfit != nil && best.NetProfit.Sign() > 0 {
				optimizedRoute := routes.Route{
					Symbols: append([]string(nil), route.Symbols...),
					Hops:    append([]pools.Pool(nil), route.Hops...),
				}
				_, _ = e.evaluator.Evaluate(ctx, optimizedRoute, best.Amount)
			}
			continue
		}

		deep, err := e.optimizer.Optimize(
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
					Samples: allocatedSamples,
				}
			}(),
		)
		report.OptimizerSamples += uint64(deep.Evaluated)

		if err == nil && deep.NetProfit != nil && (best.NetProfit == nil || deep.NetProfit.Cmp(best.NetProfit) > 0) {
			best = deep
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

func samplesForScore(score int64, base int) int {
	if base < 2 {
		base = 2
	}
	switch {
	case score >= 4_000:
		return base
	case score < 2_000:
		return 2
	default:
		if base/2 < 2 {
			return 2
		}
		return base / 2
	}
}

func shouldStopOptimizerAfterProbe(candidate optimizerCandidate, best optimizer.OptimalLoan, current *big.Int) bool {
	if best.NetProfit == nil {
		return true
	}
	if best.NetProfit.Sign() > 0 {
		return false
	}
	if candidate.score < 2_000 {
		return true
	}
	limit := big.NewInt(0)
	if candidate.minProfit != nil && candidate.minProfit.Sign() > 0 {
		limit.Mul(candidate.minProfit, big.NewInt(25))
	}
	if current != nil && current.Sign() > 0 {
		byAmount := new(big.Int).Div(new(big.Int).Set(current), big.NewInt(100))
		if byAmount.Cmp(limit) > 0 {
			limit = byAmount
		}
	}
	if limit.Sign() <= 0 {
		limit = big.NewInt(1)
	}
	return new(big.Int).Neg(best.NetProfit).Cmp(limit) > 0
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func routeKey(route routes.Route) string {
	parts := make([]string, 0, len(route.Hops)+2)
	parts = append(parts, route.String(), nearmiss.DEXPath(route))
	for _, hop := range route.Hops {
		parts = append(parts, strings.ToLower(hop.Address))
	}
	return strings.Join(parts, "|")
}

func forcedRouteSourceBlock(route routes.Route) uint64 {
	var source uint64
	for _, pool := range route.Hops {
		if pool.LastUpdatedBlock == 0 {
			continue
		}
		if source == 0 || pool.LastUpdatedBlock < source {
			source = pool.LastUpdatedBlock
		}
	}
	return source
}

func forcedRouteCombinations(symbols []string, choices [][]pools.Pool, maximum int) []routes.Route {
	if maximum < 1 {
		return nil
	}
	var out []routes.Route
	var walk func(int, []pools.Pool)
	walk = func(index int, current []pools.Pool) {
		if len(out) >= maximum {
			return
		}
		if index == len(choices) {
			out = append(out, routes.Route{Symbols: append([]string(nil), symbols...), Hops: append([]pools.Pool(nil), current...)})
			return
		}
		for _, pool := range choices[index] {
			walk(index+1, append(current, pool))
			if len(out) >= maximum {
				return
			}
		}
	}
	walk(0, nil)
	return out
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
