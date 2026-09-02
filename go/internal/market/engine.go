// Package market connects pool discovery and bounded opportunity evaluation.
package market

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
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
	"github.com/titanarb/titanarb-go/internal/rpc"
	"github.com/titanarb/titanarb-go/internal/volatility"
)

type Engine struct {
	market             config.MarketConfig
	discoverer         *pools.Discoverer
	cache              *cache.PoolCache
	evaluator          *opportunity.Engine
	Events             <-chan opportunity.Event
	metrics            *metrics.Metrics
	discoveryWorkers   int
	amounts            map[string]*big.Int
	statsMu            sync.RWMutex
	activePools        uint64
	cycles             uint64
	volatility         *volatility.Tracker
	optimizer          optimizer.Optimizer
	liquidity          func(context.Context, string) (*big.Int, error)
	routeCache         []routes.Route
	universeAssets     []string
	dynamicAssets      []string
	universeDecisions  []string
	cyclesSinceFull    uint64
	lastMaxHops        int
	lastMaxRoutes      int
	lastStateBlock     uint64
	routeFailures      map[string]uint64
	routeMemory        *nearmiss.RouteMemory
	universeFeedback   func(string, uint64, uint64)
	pendingMu          sync.Mutex
	pendingMarket      *config.MarketConfig
	pendingPairKeys    map[string]struct{}
	pendingDynamic     map[string]struct{}
	livePairKeys       map[string]struct{}
	liveDynamic        map[string]struct{}
	executionAssets    []string
	forceFull          bool
	pairScore          func(string, string) (float64, bool)
	crossVenueShareBPS int
	recentNearMisses   []nearmiss.Record
	reconciliation     *reconcileState
	forceEvaluateNext  bool
	syncOnce           sync.Once
	syncMu             sync.Mutex
	dirtyScanBlock     uint64
	pendingDirty       map[string]struct{}
	syncRunning        bool
	syncLast           marketSyncSnapshot
}

type reconcileJob struct {
	from, to string
	dex      pools.DEX
	fee      uint32
}

type reconcileState struct {
	jobs       []reconcileJob
	next       int
	byPair     map[routes.Pair][]pools.Pool
	discovered map[string]pools.Pool
	maxHops    int
	maxRoutes  int
	batches    uint64
	failures   int
	started    time.Time
}

type refreshStats struct {
	Logs                  pools.LogQueryStats
	PoolsAttempted        uint64
	PoolsSucceeded        uint64
	PoolsFailed           uint64
	UnresolvedDirty       map[string]struct{}
	ResolvedDirty         map[string]struct{}
	MarketStateIncomplete bool
	LastFailure           string
}

type marketSyncSnapshot struct {
	LatestHead             uint64
	ScanWatermark          uint64
	StateWatermark         uint64
	LagBlocks              uint64
	ChunksAttempted        uint64
	ChunksSucceeded        uint64
	ChunksFailed           uint64
	BlocksScanned          uint64
	ProgressBlocks         uint64
	DirtyPoolsFound        uint64
	IterationDuration      time.Duration
	LimiterWait            time.Duration
	Transport              time.Duration
	CheckpointAdvances     uint64
	CheckpointBlock        uint64
	PendingDirtyPools      uint64
	MarketStateComplete    bool
	MarketStateSyncLagging bool
}

type Snapshot struct {
	ActivePools uint64
	Cycles      uint64
}

type CycleReport struct {
	StateBlock               uint64
	FullReconcile            bool
	DirtyPools               uint64
	RoutesRecomputed         uint64
	RoutesReused             uint64
	QuoteDuration            time.Duration
	OptimizerDuration        time.Duration
	OptimizerRuns            uint64
	OptimizerSamples         uint64
	RoutesEvaluated          uint64
	Duration                 time.Duration
	Routes                   []routes.Route
	UniverseAssets           []string
	DynamicAssets            []string
	UniverseDecisions        []string
	RouteCountBefore         int
	RouteCountAfter          int
	RoutesByHop              map[int]int
	DEXRoutes                map[string]int
	TopNearMisses            []map[string]any
	RouteScores              map[string]int
	OptimizerBudget          map[string]int
	RejectionReasons         map[string]int
	ExploitSelected          uint64
	ExploreSelected          uint64
	NewSelected              uint64
	MemoryRoutes             int
	AvgPreQuoteScore         int64
	CrossVenueQuoted         uint64
	SameDEXQuoted            uint64
	RoutesConsidered         uint64
	QuoteAgeBlocks           uint64
	PreQuoteRanking          bool
	RPCCallsByStage          map[string]uint64
	RPCCallsPoolRefresh      uint64
	RPCCallsInitialQuotes    uint64
	RPCCallsOptimizer        uint64
	RPCCallsEconomics        uint64
	QuoteCacheHits           uint64
	QuoteCacheMisses         uint64
	QuoteDedupHits           uint64
	QuoteCacheInvalidations  uint64
	QuoteBudget              int
	QuoteBudgetUsed          int
	QuoteBudgetDropped       uint64
	NoQuoteReason            string
	OptimizerRequested       uint64
	OptimizerSaved           uint64
	RoutesDeepOptimized      uint64
	RoutesProbeOnly          uint64
	RoutesSkippedDeep        uint64
	RoutesSkippedByPreQuote  uint64
	RPCPerEvaluatedRoute     float64
	RPCPerOptimizerRoute     float64
	ScoreDeciles             map[string]map[string]any
	ReconcilePending         bool
	ReconcileBatchSize       int
	ReconcilePairsDone       int
	ReconcilePairsTotal      int
	ReconcileUnitsDone       int
	ReconcileUnitsTotal      int
	ReconcileRPC             uint64
	ReconcileDuration        time.Duration
	ReconcileCompleted       bool
	ReconcileError           string
	ReconcileUnitsThisCycle  int
	ReconcileFailures        int
	ReconcileTotalDuration   time.Duration
	ReconcileFailedUnits     int
	ReconcileDiscoveredPools int
	ReconcileGeneratedRoutes int
	RouteCacheReady          bool
	RouteCacheSize           int
	GetLogsChunksAttempted   uint64
	GetLogsChunksSucceeded   uint64
	GetLogsChunksFailed      uint64
	GetLogsBlocksScanned     uint64
	RefreshPoolsAttempted    uint64
	RefreshPoolsSucceeded    uint64
	RefreshPoolsFailed       uint64
	UnresolvedDirtyPools     uint64
	RoutesSkippedUnresolved  uint64
	MarketStateIncomplete    bool
	DetectorStage            DetectorStageTelemetry
	SyncLatestHead           uint64
	SyncScanWatermark        uint64
	SyncStateWatermark       uint64
	SyncLagBlocks            uint64
	SyncChunksAttempted      uint64
	SyncChunksSucceeded      uint64
	SyncChunksFailed         uint64
	SyncBlocksScanned        uint64
	SyncProgressBlocks       uint64
	SyncDirtyPoolsFound      uint64
	SyncIterationDuration    time.Duration
	SyncLimiterWait          time.Duration
	SyncTransport            time.Duration
	SyncCheckpointAdvances   uint64
	SyncCheckpointBlock      uint64
	SyncPendingDirtyPools    uint64
	DecisionStateBlock       uint64
	DecisionLatestHead       uint64
	DecisionStateLagBlocks   uint64
	MarketStateComplete      bool
	MarketStateSyncLagging   bool
}

const incrementalRefreshBatchBlocks = 8
const maxOptimizerRoutesPerAsset = 2
const maxEvaluationRoutesPerAsset = 12
const optimizerSamplesPerRoute = 8

type SearchOptions struct {
	EvaluationRoutesPerAsset  int
	OptimizerRoutesPerAsset   int
	OptimizerSamplesPerRoute  int
	OptimizerSamplesPerCycle  int
	MaxQuotesPerCycle         int
	MaxOptimizedRoutes        int
	NormalOptimizerSamples    int
	FullReconcileEvery        uint64
	ReconcileBatchPairs       int
	SyncMaxChunksPerIteration int
	DisablePreQuoteRanking    bool
	ExploreRatioBPS           int
	PersistentQuoteCache      bool
	AdaptiveOptimizer         bool
	EarlyStop                 bool
	OptimizationFlagsSet      bool
}

func DefaultSearchOptions() SearchOptions {
	return SearchOptions{
		EvaluationRoutesPerAsset:  maxEvaluationRoutesPerAsset,
		OptimizerRoutesPerAsset:   maxOptimizerRoutesPerAsset,
		OptimizerSamplesPerRoute:  optimizerSamplesPerRoute,
		OptimizerSamplesPerCycle:  32,
		MaxQuotesPerCycle:         40,
		MaxOptimizedRoutes:        8,
		NormalOptimizerSamples:    3,
		FullReconcileEvery:        2_400,
		ReconcileBatchPairs:       4,
		SyncMaxChunksPerIteration: 20,
		DisablePreQuoteRanking:    false,
		ExploreRatioBPS:           2_000,
		PersistentQuoteCache:      true,
		AdaptiveOptimizer:         true,
		EarlyStop:                 true,
		OptimizationFlagsSet:      true,
	}
}

func (o SearchOptions) Normalized() SearchOptions {
	defaults := DefaultSearchOptions()
	if !o.OptimizationFlagsSet {
		o.PersistentQuoteCache = defaults.PersistentQuoteCache
		o.AdaptiveOptimizer = defaults.AdaptiveOptimizer
		o.EarlyStop = defaults.EarlyStop
		o.OptimizationFlagsSet = true
	}
	if o.EvaluationRoutesPerAsset < 1 {
		o.EvaluationRoutesPerAsset = defaults.EvaluationRoutesPerAsset
	}
	if o.OptimizerRoutesPerAsset < 1 {
		o.OptimizerRoutesPerAsset = defaults.OptimizerRoutesPerAsset
	}
	if o.OptimizerSamplesPerRoute < 2 {
		o.OptimizerSamplesPerRoute = defaults.OptimizerSamplesPerRoute
	}
	if o.OptimizerSamplesPerCycle < 2 {
		o.OptimizerSamplesPerCycle = defaults.OptimizerSamplesPerCycle
	}
	if o.OptimizerSamplesPerCycle > 512 {
		o.OptimizerSamplesPerCycle = 512
	}
	if o.MaxQuotesPerCycle < 1 {
		o.MaxQuotesPerCycle = defaults.MaxQuotesPerCycle
	}
	if o.MaxOptimizedRoutes < 1 {
		o.MaxOptimizedRoutes = defaults.MaxOptimizedRoutes
	}
	if o.NormalOptimizerSamples < 2 {
		o.NormalOptimizerSamples = defaults.NormalOptimizerSamples
	}
	if o.NormalOptimizerSamples > o.OptimizerSamplesPerRoute {
		o.NormalOptimizerSamples = o.OptimizerSamplesPerRoute
	}
	if o.FullReconcileEvery == 0 {
		o.FullReconcileEvery = defaults.FullReconcileEvery
	}
	if o.ReconcileBatchPairs < 1 {
		o.ReconcileBatchPairs = defaults.ReconcileBatchPairs
	}
	if o.ReconcileBatchPairs > 8 {
		o.ReconcileBatchPairs = 8
	}
	if o.SyncMaxChunksPerIteration < 1 {
		o.SyncMaxChunksPerIteration = defaults.SyncMaxChunksPerIteration
	}
	if o.SyncMaxChunksPerIteration > 200 {
		o.SyncMaxChunksPerIteration = 200
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
	var events <-chan opportunity.Event
	if evaluator != nil {
		events = evaluator.Events
	}
	return &Engine{market: market, discoverer: discoverer, cache: cache, evaluator: evaluator, Events: events, amounts: validAmounts, discoveryWorkers: workers, metrics: metrics, volatility: volatility.NewTracker(), optimizer: optimizer.Optimizer{Workers: workers}, liquidity: liquidity, universeAssets: market.MarketAssets(), routeFailures: make(map[string]uint64), routeMemory: nearmiss.NewRouteMemory(8192), executionAssets: append([]string(nil), market.ExecutionAssetNames...), pendingDirty: make(map[string]struct{})}
}

// StartMarketStateSynchronizer starts the single background dirty-log scanner.
// It checkpointes contiguous successful getLogs chunks separately from the
// latency-critical decision cycle. The decision cycle consumes the committed
// pending dirty set instead of rescanning long historical ranges.
func (e *Engine) StartMarketStateSynchronizer(ctx context.Context, latestBlock func() uint64, options func() SearchOptions) {
	if latestBlock == nil {
		return
	}
	e.syncOnce.Do(func() {
		e.syncMu.Lock()
		e.syncRunning = true
		if e.pendingDirty == nil {
			e.pendingDirty = make(map[string]struct{})
		}
		e.syncMu.Unlock()
		go e.marketStateSyncLoop(ctx, latestBlock, options)
	})
}

func (e *Engine) marketStateSyncLoop(ctx context.Context, latestBlock func() uint64, options func() SearchOptions) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			latest := latestBlock()
			if latest == 0 || len(e.cache.Snapshot()) == 0 {
				continue
			}
			opts := DefaultSearchOptions()
			if options != nil {
				opts = options().Normalized()
			}
			e.advanceMarketStateSync(ctx, latest, opts)
		}
	}
}

func (e *Engine) advanceMarketStateSync(ctx context.Context, latest uint64, options SearchOptions) marketSyncSnapshot {
	started := time.Now()
	chunkSize := e.discoverer.GetLogsChunkSize()
	if chunkSize == 0 {
		chunkSize = 5
	}
	maxChunks := uint64(options.Normalized().SyncMaxChunksPerIteration)
	current := e.cache.Snapshot()
	lastStateBlock := e.getLastStateBlock()
	snapshot := marketSyncSnapshot{LatestHead: latest, StateWatermark: lastStateBlock, MarketStateComplete: true}
	if len(current) == 0 {
		e.storeMarketSyncSnapshot(snapshot)
		return snapshot
	}
	addresses := make([]string, 0, len(current))
	for _, pool := range current {
		addresses = append(addresses, pool.Address)
	}
	snapshot.StateWatermark = lastStateBlock
	e.syncMu.Lock()
	if e.pendingDirty == nil {
		e.pendingDirty = make(map[string]struct{})
	}
	if e.dirtyScanBlock == 0 {
		if lastStateBlock > 0 {
			e.dirtyScanBlock = lastStateBlock
		} else {
			e.dirtyScanBlock = latest
		}
	}
	from := e.dirtyScanBlock + 1
	pending := uint64(len(e.pendingDirty))
	e.syncMu.Unlock()
	if from == 0 || from > latest {
		snapshot.ScanWatermark = e.currentDirtyScanBlock()
		snapshot.StateWatermark = e.getLastStateBlock()
		snapshot.PendingDirtyPools = pending
		snapshot.MarketStateComplete = snapshot.ScanWatermark >= latest
		if latest > snapshot.ScanWatermark {
			snapshot.LagBlocks = latest - snapshot.ScanWatermark
			snapshot.MarketStateSyncLagging = true
		}
		e.storeMarketSyncSnapshot(snapshot)
		return snapshot
	}
	maxBlocks := maxChunks * chunkSize
	to := latest
	if maxBlocks > 0 && to-from+1 > maxBlocks {
		to = from + maxBlocks - 1
	}
	syncCtx := rpc.WithRequestMetadata(rpc.WithRequestClass(ctx, rpc.Background), "market_state_sync", latest)
	iterCtx, cancel := context.WithTimeout(syncCtx, 30*time.Second)
	defer cancel()
	tracker := newDetectorStageTracker(latest, lastStateBlock, nil)
	iterCtx = rpc.WithTimingObserver(iterCtx, tracker)
	dirty, stats, err := e.discoverer.ChangedPoolAddressesAtWithStats(iterCtx, addresses, from, to)
	telemetry := tracker.Snapshot()
	successBlocks := stats.ChunksSucceeded * chunkSize
	totalBlocks := to - from + 1
	if successBlocks > totalBlocks {
		successBlocks = totalBlocks
	}
	var checkpoint uint64
	if successBlocks > 0 {
		checkpoint = from + successBlocks - 1
	}
	e.syncMu.Lock()
	if e.pendingDirty == nil {
		e.pendingDirty = make(map[string]struct{})
	}
	for address := range dirty {
		e.pendingDirty[strings.ToLower(address)] = struct{}{}
	}
	if checkpoint > e.dirtyScanBlock {
		e.dirtyScanBlock = checkpoint
		snapshot.CheckpointAdvances = 1
	}
	snapshot.LatestHead = latest
	snapshot.ScanWatermark = e.dirtyScanBlock
	snapshot.StateWatermark = lastStateBlock
	snapshot.PendingDirtyPools = uint64(len(e.pendingDirty))
	e.syncMu.Unlock()
	snapshot.ChunksAttempted = stats.ChunksAttempted
	snapshot.ChunksSucceeded = stats.ChunksSucceeded
	snapshot.ChunksFailed = stats.ChunksFailed
	snapshot.BlocksScanned = stats.BlocksScanned
	snapshot.ProgressBlocks = successBlocks
	snapshot.DirtyPoolsFound = uint64(len(dirty))
	snapshot.IterationDuration = time.Since(started)
	snapshot.LimiterWait = telemetry.RPCLimiterWait
	snapshot.Transport = telemetry.RPCTransport
	snapshot.CheckpointBlock = checkpoint
	if latest > snapshot.ScanWatermark {
		snapshot.LagBlocks = latest - snapshot.ScanWatermark
		snapshot.MarketStateSyncLagging = true
	}
	snapshot.MarketStateComplete = !snapshot.MarketStateSyncLagging && err == nil
	e.storeMarketSyncSnapshot(snapshot)
	return snapshot
}

func (e *Engine) storeMarketSyncSnapshot(snapshot marketSyncSnapshot) {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	e.syncLast = snapshot
}

func (e *Engine) currentDirtyScanBlock() uint64 {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	return e.dirtyScanBlock
}

func (e *Engine) marketSyncEnabled() bool {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	return e.syncRunning
}

func (e *Engine) marketSyncSnapshot(latest uint64) marketSyncSnapshot {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	s := e.syncLast
	s.LatestHead = latest
	s.ScanWatermark = e.dirtyScanBlock
	s.StateWatermark = e.lastStateBlock
	s.PendingDirtyPools = uint64(len(e.pendingDirty))
	s.MarketStateComplete = s.ScanWatermark >= latest
	if latest > s.ScanWatermark {
		s.LagBlocks = latest - s.ScanWatermark
		s.MarketStateSyncLagging = true
	} else {
		s.LagBlocks = 0
		s.MarketStateSyncLagging = false
	}
	return s
}

func (e *Engine) getLastStateBlock() uint64 {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	return e.lastStateBlock
}

func (e *Engine) setLastStateBlock(block uint64) {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	if block > e.lastStateBlock {
		e.lastStateBlock = block
	}
}

func (e *Engine) pendingDirtySnapshot() map[string]struct{} {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	out := make(map[string]struct{}, len(e.pendingDirty))
	for address := range e.pendingDirty {
		out[address] = struct{}{}
	}
	return out
}

func (e *Engine) ackPendingDirty(attempted, unresolved map[string]struct{}) {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	for address := range attempted {
		key := strings.ToLower(address)
		if _, keep := unresolved[key]; keep {
			continue
		}
		delete(e.pendingDirty, key)
	}
}

func (e *Engine) SetUniverseTelemetry(active, dynamic, decisions []string) {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	e.universeAssets = append([]string(nil), active...)
	e.dynamicAssets = append([]string(nil), dynamic...)
	e.universeDecisions = append([]string(nil), decisions...)
}

// SetUniverseFeedback connects read-only market usefulness observations to the
// universe manager. It cannot mutate the current graph or execution boundary.
func (e *Engine) SetUniverseFeedback(feedback func(string, uint64, uint64)) {
	e.universeFeedback = feedback
}

// QueueMarketConfig atomically stages a market-only universe for the next
// cycle. ExecutionAssets and loan amounts are deliberately retained from the
// original engine configuration.
func (e *Engine) QueueMarketConfig(next config.MarketConfig) {
	e.QueueLiveMarket(next, nil, nil)
}

// QueueLiveMarket stages one immutable market snapshot. Only explicitly
// admitted dynamic pair edges are discoverable; execution assets remain the
// immutable startup allow-list.
func (e *Engine) QueueLiveMarket(next config.MarketConfig, pairKeys, dynamicSymbols []string) {
	next.ExecutionAssetNames = append([]string(nil), e.executionAssets...)
	allowed := make(map[string]struct{}, len(pairKeys))
	for _, key := range pairKeys {
		allowed[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	dynamic := make(map[string]struct{}, len(dynamicSymbols))
	for _, symbol := range dynamicSymbols {
		dynamic[symbol] = struct{}{}
	}
	e.pendingMu.Lock()
	e.pendingMarket = &next
	e.pendingPairKeys = allowed
	e.pendingDynamic = dynamic
	e.pendingMu.Unlock()
}
func (e *Engine) SetPairRouting(score func(string, string) (float64, bool), crossVenueShareBPS int) {
	e.pairScore = score
	if crossVenueShareBPS < 0 {
		crossVenueShareBPS = 0
	}
	if crossVenueShareBPS > 10000 {
		crossVenueShareBPS = 10000
	}
	e.crossVenueShareBPS = crossVenueShareBPS
}

func (e *Engine) applyPendingMarket() bool {
	e.pendingMu.Lock()
	next := e.pendingMarket
	pairKeys := e.pendingPairKeys
	dynamic := e.pendingDynamic
	e.pendingMarket = nil
	e.pendingPairKeys = nil
	e.pendingDynamic = nil
	e.pendingMu.Unlock()
	if next == nil {
		return false
	}
	if reflect.DeepEqual(e.market, *next) && stringSetEqual(e.livePairKeys, pairKeys) && stringSetEqual(e.liveDynamic, dynamic) {
		return false
	}
	e.market = *next
	e.livePairKeys = pairKeys
	e.liveDynamic = dynamic
	if e.evaluator != nil {
		e.evaluator.SetMarketConfig(*next)
	}
	e.statsMu.Lock()
	e.universeAssets = append([]string(nil), next.MarketAssets()...)
	e.statsMu.Unlock()
	e.forceFull = true
	return true
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

// RequiresFullReconcile reports whether a cycle needs an uninterruptible full
// scan. Reconciliation is checkpointed and interruptible, so it never blocks
// stale-work cancellation in the scheduler.
func (e *Engine) RequiresFullReconcile(maxHops, maxRoutes int, options SearchOptions) bool {
	return false
}

// CycleAtWithSearchOptions lets runtime risk profiles widen or narrow the
// read-only market search envelope without touching execution safety gates.
func (e *Engine) CycleAtWithSearchOptions(ctx context.Context, stateBlock uint64, maxHops, maxRoutes int, volatilityWeight float64, options SearchOptions) (report CycleReport, err error) {
	lastStateAtStart := e.getLastStateBlock()
	tracker := newDetectorStageTracker(stateBlock, lastStateAtStart, nil)
	defer func() {
		tracker.SetLastStateBlock(e.getLastStateBlock())
		tracker.SetReconciliation(e.reconciliation != nil, reconciliationUnitsDone(e.reconciliation), reconciliationUnitsTotal(e.reconciliation))
		report.DetectorStage = tracker.Snapshot()
	}()
	ctx = rpc.WithTimingObserver(ctx, tracker)
	tracker.Stage(stageApplyPendingMarket)
	marketChanged := e.applyPendingMarket()
	tracker.Stage(stageReconcileCheck)
	options = options.Normalized()
	started := time.Now()
	report = CycleReport{StateBlock: stateBlock}
	report.RouteCountBefore = len(e.routeCache)
	report.RouteCacheReady = len(e.routeCache) > 0
	report.RouteCacheSize = len(e.routeCache)
	e.applySyncTelemetry(&report, e.marketSyncSnapshot(stateBlock), stateBlock)
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
	hadRoutes := len(e.routeCache) > 0
	searchEnvelopeChanged := maxHops != e.lastMaxHops || maxRoutes != e.lastMaxRoutes || marketChanged || e.forceFull
	needsReconcile := !hadRoutes || e.cyclesSinceFull >= options.FullReconcileEvery || searchEnvelopeChanged
	if needsReconcile && (e.reconciliation == nil || e.reconciliation.maxHops != maxHops || e.reconciliation.maxRoutes != maxRoutes || marketChanged) {
		e.startReconciliation(maxHops, maxRoutes)
	}
	if !hadRoutes {
		tracker.Stage(stageStartupReconciliation)
		e.advanceReconciliationBatch(ctx, stateBlock, options.ReconcileBatchPairs, &report)
		report.Duration = time.Since(started)
		report.Routes = append([]routes.Route(nil), e.routeCache...)
		report.RouteCountAfter = len(report.Routes)
		report.RouteCacheReady = len(e.routeCache) > 0
		report.RouteCacheSize = len(e.routeCache)
		report.RoutesByHop = routesByHop(report.Routes)
		report.DEXRoutes = routeDEXDiversity(report.Routes)
		if len(report.Routes) == 0 {
			report.NoQuoteReason = "reconciliation_in_progress"
		}
		if stateBlock > 0 {
			e.setLastStateBlock(stateBlock)
		}
		return report, nil
	}
	tracker.Stage(stageIncrementalRefreshGate)
	lastStateBlock := e.getLastStateBlock()
	pendingSyncedDirty := e.marketSyncEnabled() && len(e.pendingDirtySnapshot()) > 0
	if !pendingSyncedDirty && shouldDeferIncrementalRefresh(lastStateBlock, stateBlock, incrementalRefreshBatchBlocks) {
		routesFound := refreshRoutes(e.routeCache, liquidPools(e.cache.Snapshot()))
		report.RoutesReused = uint64(len(routesFound))
		e.describeReconciliation(&report)
		report.Duration = time.Since(started)
		report.Routes = routesFound
		report.RouteCountAfter = len(routesFound)
		report.RoutesByHop = routesByHop(routesFound)
		report.DEXRoutes = routeDEXDiversity(routesFound)
		report.NoQuoteReason = "incremental_refresh_deferred"
		e.statsMu.Lock()
		e.activePools = uint64(len(e.cache.Snapshot()))
		e.cycles = uint64(len(routesFound))
		e.statsMu.Unlock()
		return report, nil
	}
	var dirty map[string]struct{}
	var refresh refreshStats
	rpcAtStart := e.rpcCalls()
	forceAllEvaluation := e.forceEvaluateNext
	e.forceEvaluateNext = false
	refreshCtx := rpc.WithRequestMetadata(rpc.WithRequestClass(ctx, rpc.HotPath), "pool_refresh", stateBlock)
	var syncedDirty map[string]struct{}
	if e.marketSyncEnabled() {
		syncedDirty = e.pendingDirtySnapshot()
		dirty = copyStringSet(syncedDirty)
		refresh = e.refreshKnownDirty(refreshCtx, dirty, stateBlock, tracker)
		e.applySyncTelemetry(&report, e.marketSyncSnapshot(stateBlock), stateBlock)
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		e.ackPendingDirty(refresh.ResolvedDirty, refresh.UnresolvedDirty)
	} else {
		fromBlock := stateBlock
		if lastStateBlock > 0 && lastStateBlock < stateBlock {
			fromBlock = lastStateBlock + 1
		}
		tracker.Stage(stageGetLogsDirtyDetection)
		tracker.StartGetLogs(fromBlock, stateBlock, e.discoverer.GetLogsChunkSize())
		dirty, refresh, err = e.incrementalRefresh(refreshCtx, fromBlock, stateBlock, tracker)
		tracker.FinishGetLogs(refresh)
	}
	tracker.FinishPoolRefresh(refresh)
	report.GetLogsChunksAttempted = refresh.Logs.ChunksAttempted
	report.GetLogsChunksSucceeded = refresh.Logs.ChunksSucceeded
	report.GetLogsChunksFailed = refresh.Logs.ChunksFailed
	report.GetLogsBlocksScanned = refresh.Logs.BlocksScanned
	report.RefreshPoolsAttempted = refresh.PoolsAttempted
	report.RefreshPoolsSucceeded = refresh.PoolsSucceeded
	report.RefreshPoolsFailed = refresh.PoolsFailed
	report.UnresolvedDirtyPools = uint64(len(refresh.UnresolvedDirty))
	report.MarketStateIncomplete = refresh.MarketStateIncomplete
	if err != nil {
		return report, err
	}
	tracker.SetDirtyPools(uint64(len(dirty)))
	if shouldAdvanceFullReconcileCounter(dirty) {
		e.cyclesSinceFull++
	}
	rpcAfterRefresh := e.rpcCalls()
	report.DirtyPools = uint64(len(dirty))
	active := liquidPools(e.cache.Snapshot())
	signals := map[string]volatility.Signal{}
	if e.volatility != nil {
		signals = e.volatility.Observe(active)
	}
	routesFound := refreshRoutes(e.routeCache, active)
	tracker.Stage(stageDirtyRouteMapping)
	sort.SliceStable(routesFound, func(i, j int) bool {
		left := routeVolatility(routesFound[i], signals, volatilityWeight)
		right := routeVolatility(routesFound[j], signals, volatilityWeight)
		if left != right {
			return left > right
		}
		return routesFound[i].String() < routesFound[j].String()
	})
	affected := routesAffectedBy(routesFound, dirty, forceAllEvaluation)
	if len(refresh.UnresolvedDirty) > 0 {
		before := len(affected)
		affected = routesWithoutPools(affected, refresh.UnresolvedDirty)
		report.RoutesSkippedUnresolved = uint64(before - len(affected))
	}
	e.reviveDirtyRouteMemory(affected, stateBlock)
	tracker.SetAffectedRoutes(uint64(len(affected)))
	report.RoutesRecomputed = uint64(len(affected))
	if len(routesFound) > len(affected) {
		report.RoutesReused = uint64(len(routesFound) - len(affected))
	}
	e.statsMu.Lock()
	e.activePools = uint64(len(active))
	e.cycles = uint64(len(routesFound))
	e.statsMu.Unlock()
	tracker.Stage(stagePreQuote)
	quoteStarted := time.Now()
	if e.evaluator != nil {
		e.evaluator.PrepareQuoteCache(stateBlock, dirty, options.PersistentQuoteCache)
		e.evaluator.SetQuoteBudget(options.MaxQuotesPerCycle)
	}
	evaluation := e.evaluate(ctx, affected, options, signals, volatilityWeight, stateBlock, tracker)
	if err := ctx.Err(); err != nil {
		return report, err
	}
	rpcAfterEvaluation := e.rpcCalls()
	report.QuoteDuration = time.Since(quoteStarted)
	report.OptimizerDuration = evaluation.OptimizerDuration
	report.OptimizerRuns = evaluation.OptimizerRuns
	report.OptimizerSamples = evaluation.OptimizerSamples
	report.RoutesEvaluated = evaluation.RoutesEvaluated
	e.recentNearMisses = mergeNearMisses(e.recentNearMisses, evaluation.NearMisses, 100)
	report.TopNearMisses = nearmiss.Snapshot(e.recentNearMisses, 10)
	report.RouteScores = nearmiss.Distribution(evaluation.NearMisses)
	report.OptimizerBudget = evaluation.OptimizerBudget
	report.RejectionReasons = nearmiss.RejectionReasons(evaluation.NearMisses)
	report.ExploitSelected = evaluation.ExploitSelected
	report.ExploreSelected = evaluation.ExploreSelected
	report.NewSelected = evaluation.NewSelected
	report.MemoryRoutes = evaluation.MemoryRoutes
	report.AvgPreQuoteScore = evaluation.AvgPreQuoteScore
	report.CrossVenueQuoted = evaluation.CrossVenueQuoted
	report.SameDEXQuoted = evaluation.SameDEXQuoted
	report.RoutesConsidered = evaluation.RoutesConsidered
	report.QuoteAgeBlocks = evaluation.QuoteAgeBlocks
	report.PreQuoteRanking = !options.DisablePreQuoteRanking
	report.RPCCallsByStage = map[string]uint64{
		"pool_refresh":   safeUint64Delta(rpcAfterRefresh, rpcAtStart),
		"initial_quotes": evaluation.RPCCallsInitialQuotes,
		"optimizer":      evaluation.RPCCallsOptimizer,
		"economics":      evaluation.RPCCallsEconomics,
	}
	report.RPCCallsPoolRefresh = safeUint64Delta(rpcAfterRefresh, rpcAtStart)
	report.RPCCallsInitialQuotes = evaluation.RPCCallsInitialQuotes
	report.RPCCallsOptimizer = evaluation.RPCCallsOptimizer
	report.RPCCallsEconomics = evaluation.RPCCallsEconomics
	report.QuoteCacheMisses = evaluation.QuoteCacheMisses
	report.QuoteCacheInvalidations = evaluation.QuoteCacheInvalidations
	report.QuoteBudget = evaluation.QuoteBudget
	report.QuoteBudgetUsed = evaluation.QuoteBudgetUsed
	report.QuoteBudgetDropped = evaluation.QuoteBudgetDropped
	if len(affected) == 0 {
		if report.MarketStateSyncLagging {
			report.NoQuoteReason = "market_state_sync_lagging"
		} else {
			report.NoQuoteReason = "no_dirty_routes"
		}
	} else if evaluation.RoutesEvaluated == 0 {
		report.NoQuoteReason = "prequote_or_quote_budget"
	} else if len(evaluation.NearMisses) == 0 {
		report.NoQuoteReason = "no_complete_economic_evaluation"
	}
	report.OptimizerRequested = evaluation.OptimizerRequested
	report.RoutesDeepOptimized = evaluation.RoutesDeepOptimized
	report.RoutesProbeOnly = evaluation.RoutesProbeOnly
	report.RoutesSkippedDeep = evaluation.RoutesSkippedDeep
	report.RPCPerEvaluatedRoute = ratio(safeUint64Delta(rpcAfterEvaluation, rpcAfterRefresh), evaluation.RoutesEvaluated)
	report.RPCPerOptimizerRoute = ratio(evaluation.RPCCallsOptimizer, evaluation.OptimizerRuns)
	report.ScoreDeciles = evaluation.ScoreDeciles
	report.QuoteCacheHits = evaluation.QuoteCacheHits
	report.QuoteDedupHits = evaluation.QuoteDedupHits
	report.OptimizerSaved = evaluation.OptimizerSaved
	report.RoutesSkippedByPreQuote = evaluation.RoutesSkippedByPreQuote
	// Dirty/live market work owns the cycle. Background reconciliation advances
	// only on an otherwise idle head so it cannot add latency to fresh quotes.
	if len(dirty) == 0 {
		tracker.Stage(stageBackgroundReconciliation)
		e.advanceBackgroundReconciliationBatch(ctx, stateBlock, options.ReconcileBatchPairs, &report)
	}
	report.RPCCallsByStage["reconcile"] = report.ReconcileRPC
	report.Duration = time.Since(started)
	report.Routes = routesFound
	report.RouteCountAfter = len(routesFound)
	report.RouteCacheReady = len(e.routeCache) > 0
	report.RouteCacheSize = len(e.routeCache)
	report.RoutesByHop = routesByHop(routesFound)
	report.DEXRoutes = routeDEXDiversity(routesFound)
	tracker.Stage(stageCycleCommit)
	if stateBlock > 0 && !refresh.MarketStateIncomplete {
		commitBlock := stateBlock
		if e.marketSyncEnabled() {
			if scan := e.currentDirtyScanBlock(); scan > 0 && scan < commitBlock {
				commitBlock = scan
			}
		}
		e.setLastStateBlock(commitBlock)
	}
	tracker.Stage(stageCycleComplete)
	return report, nil
}

func reconciliationUnitsDone(state *reconcileState) int {
	if state == nil {
		return 0
	}
	return state.next
}

func reconciliationUnitsTotal(state *reconcileState) int {
	if state == nil {
		return 0
	}
	return len(state.jobs)
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

func (e *Engine) applySyncTelemetry(report *CycleReport, snapshot marketSyncSnapshot, decisionBlock uint64) {
	if report == nil {
		return
	}
	report.SyncLatestHead = snapshot.LatestHead
	report.SyncScanWatermark = snapshot.ScanWatermark
	report.SyncStateWatermark = snapshot.StateWatermark
	report.SyncLagBlocks = snapshot.LagBlocks
	report.SyncChunksAttempted = snapshot.ChunksAttempted
	report.SyncChunksSucceeded = snapshot.ChunksSucceeded
	report.SyncChunksFailed = snapshot.ChunksFailed
	report.SyncBlocksScanned = snapshot.BlocksScanned
	report.SyncProgressBlocks = snapshot.ProgressBlocks
	report.SyncDirtyPoolsFound = snapshot.DirtyPoolsFound
	report.SyncIterationDuration = snapshot.IterationDuration
	report.SyncLimiterWait = snapshot.LimiterWait
	report.SyncTransport = snapshot.Transport
	report.SyncCheckpointAdvances = snapshot.CheckpointAdvances
	report.SyncCheckpointBlock = snapshot.CheckpointBlock
	report.SyncPendingDirtyPools = snapshot.PendingDirtyPools
	report.DecisionStateBlock = snapshot.ScanWatermark
	report.DecisionLatestHead = decisionBlock
	report.MarketStateComplete = snapshot.MarketStateComplete
	report.MarketStateSyncLagging = snapshot.MarketStateSyncLagging
	if decisionBlock > snapshot.ScanWatermark {
		report.DecisionStateLagBlocks = decisionBlock - snapshot.ScanWatermark
	}
}

func copyStringSet(input map[string]struct{}) map[string]struct{} {
	output := make(map[string]struct{}, len(input))
	for key := range input {
		output[key] = struct{}{}
	}
	return output
}

func (e *Engine) startReconciliation(maxHops, maxRoutes int) {
	symbols := e.market.MarketAssets()
	state := &reconcileState{
		byPair:     make(map[routes.Pair][]pools.Pool),
		discovered: make(map[string]pools.Pool),
		maxHops:    maxHops,
		maxRoutes:  maxRoutes,
		started:    time.Now(),
	}
	for i, from := range symbols {
		for _, to := range symbols[i+1:] {
			if e.marketPairAllowed(from, to) {
				for _, fee := range e.discoverer.DiscoveryFeeTiers() {
					state.jobs = append(state.jobs, reconcileJob{from: from, to: to, dex: pools.UniswapV3, fee: fee})
				}
				state.jobs = append(state.jobs, reconcileJob{from: from, to: to, dex: pools.CamelotV3})
			}
		}
	}
	e.reconciliation = state
}

// advanceReconciliationBatch performs a small resumable unit of discovery.
// Existing routes/cache remain live until the final atomic commit.
func (e *Engine) describeReconciliation(report *CycleReport) {
	state := e.reconciliation
	if state == nil || report == nil {
		return
	}
	report.ReconcilePending = true
	report.ReconcilePairsDone = state.next
	report.ReconcilePairsTotal = len(state.jobs)
	report.ReconcileUnitsDone = state.next
	report.ReconcileUnitsTotal = len(state.jobs)
	report.ReconcileFailures = state.failures
}

func (e *Engine) advanceReconciliationBatch(ctx context.Context, stateBlock uint64, batchSize int, report *CycleReport) {
	e.advanceReconciliationBatchWithin(ctx, stateBlock, batchSize, 2500*time.Millisecond, report)
}

func (e *Engine) advanceBackgroundReconciliationBatch(ctx context.Context, stateBlock uint64, batchSize int, report *CycleReport) {
	if batchSize > 1 {
		batchSize = 1
	}
	e.advanceReconciliationBatchWithin(ctx, stateBlock, batchSize, 1500*time.Millisecond, report)
}

func (e *Engine) advanceReconciliationBatchWithin(ctx context.Context, stateBlock uint64, batchSize int, timeout time.Duration, report *CycleReport) {
	state := e.reconciliation
	if state == nil || report == nil {
		return
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if timeout <= 0 {
		timeout = 2500 * time.Millisecond
	}
	report.ReconcilePending = true
	report.ReconcileBatchSize = batchSize
	report.ReconcilePairsDone = state.next
	report.ReconcilePairsTotal = len(state.jobs)
	report.ReconcileUnitsDone = state.next
	report.ReconcileUnitsTotal = len(state.jobs)
	started := time.Now()
	rpcBefore := e.rpcCalls()
	// A superseded market cycle must not strand the current small checkpoint
	// forever. Finish at most this one bounded batch, then the scheduler resumes
	// the remaining jobs against its newest head.
	reconcileCtx := rpc.WithRequestMetadata(rpc.WithRequestClass(context.WithoutCancel(ctx), rpc.Background), "reconcile", stateBlock)
	batchCtx, cancel := context.WithTimeout(reconcileCtx, timeout)
	defer cancel()
	processed := 0
	for state.next < len(state.jobs) && processed < batchSize {
		job := state.jobs[state.next]
		a, aOK := e.market.Tokens[job.from]
		b, bOK := e.market.Tokens[job.to]
		if !aOK || !bOK {
			state.next++
			processed++
			continue
		}
		var found []pools.Pool
		var err error
		if job.dex == pools.UniswapV3 {
			found, err = e.discoverer.DiscoverUniswapFeeAt(batchCtx, a.Address, b.Address, job.fee, stateBlock)
		} else {
			found, err = e.discoverer.DiscoverCamelotPairAt(batchCtx, a.Address, b.Address, stateBlock)
		}
		if err != nil {
			report.ReconcileError = fmt.Sprintf("%T", err)
			if batchCtx.Err() != nil {
				break
			}
			state.failures++
			// Preserve the existing cache entry and let the next periodic
			// reconciliation retry this pair; one unavailable pair/provider must
			// not permanently block checkpoint progress.
			state.next++
			processed++
			continue
		}
		pair := routes.Pair{From: job.from, To: job.to}
		state.byPair[pair] = append(state.byPair[pair], found...)
		reverse := routes.Pair{From: job.to, To: job.from}
		state.byPair[reverse] = append(state.byPair[reverse], found...)
		for _, pool := range found {
			state.discovered[strings.ToLower(pool.Address)] = pool
			if e.metrics != nil {
				e.metrics.IncPoolsDiscovered()
				if pool.DEX == pools.UniswapV3 {
					e.metrics.IncUniswapPools()
				} else if pool.DEX == pools.CamelotV3 {
					e.metrics.IncCamelotPools()
				}
			}
		}
		state.next++
		processed++
	}
	state.batches++
	report.ReconcileBatchSize = processed
	report.ReconcilePairsDone = state.next
	report.ReconcilePairsTotal = len(state.jobs)
	report.ReconcileUnitsDone = state.next
	report.ReconcileUnitsTotal = len(state.jobs)
	report.ReconcileRPC = safeUint64Delta(e.rpcCalls(), rpcBefore)
	report.ReconcileDuration = time.Since(started)
	report.ReconcileUnitsThisCycle = processed
	report.ReconcileFailures = state.failures
	if state.next < len(state.jobs) {
		return
	}

	current := make(map[string]pools.Pool)
	for _, pool := range e.cache.Snapshot() {
		current[strings.ToLower(pool.Address)] = pool
	}
	discovered := make([]pools.Pool, 0, len(state.discovered))
	for address, pool := range state.discovered {
		if newer, ok := current[address]; ok && newer.LastUpdatedBlock > pool.LastUpdatedBlock {
			pool = newer
		}
		discovered = append(discovered, pool)
	}
	sort.Slice(discovered, func(i, j int) bool {
		return strings.ToLower(discovered[i].Address) < strings.ToLower(discovered[j].Address)
	})
	symbols := e.market.MarketAssets()
	routesFound := routes.BuildForStartsWithCrossVenueShare(e.loanAssets(), symbols, state.byPair, state.maxRoutes, e.crossVenueShareBPS)
	filtered := routesFound[:0]
	for _, route := range routesFound {
		if len(route.Hops) <= state.maxHops {
			filtered = append(filtered, route)
		}
	}
	e.cache.Replace(discovered)
	e.routeCache = append([]routes.Route(nil), filtered...)
	e.cyclesSinceFull = 0
	e.lastMaxHops, e.lastMaxRoutes = state.maxHops, state.maxRoutes
	e.forceFull = false
	e.forceEvaluateNext = true
	e.reconciliation = nil
	report.ReconcilePending = false
	report.ReconcileCompleted = true
	report.FullReconcile = true
	report.ReconcileTotalDuration = time.Since(state.started)
	report.ReconcileFailedUnits = state.failures
	report.ReconcileDiscoveredPools = len(discovered)
	report.ReconcileGeneratedRoutes = len(filtered)
	report.RouteCacheReady = len(e.routeCache) > 0
	report.RouteCacheSize = len(e.routeCache)
}

func (e *Engine) fullReconcile(ctx context.Context, stateBlock uint64, maxHops, maxRoutes int) ([]routes.Route, error) {
	previous := make(map[string]pools.Pool)
	for _, pool := range e.cache.Snapshot() {
		previous[strings.ToLower(pool.Address)] = pool
	}
	symbols := e.market.MarketAssets()
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
				if !e.marketPairAllowed(from, to) {
					continue
				}
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
	for index := range discovered {
		old, ok := previous[strings.ToLower(discovered[index].Address)]
		if ok && !poolChanged(old, discovered[index]) {
			discovered[index].LastUpdatedBlock = old.LastUpdatedBlock
		}
	}
	e.cache.Replace(discovered)

	routesFound := routes.BuildForStartsWithCrossVenueShare(e.loanAssets(), symbols, byPair, maxRoutes, e.crossVenueShareBPS)

	filtered := routesFound[:0]
	for _, route := range routesFound {
		if len(route.Hops) <= maxHops {
			filtered = append(filtered, route)
		}
	}
	routesFound = filtered
	return routesFound, nil
}

func (e *Engine) marketPairAllowed(from, to string) bool {
	_, fromDynamic := e.liveDynamic[from]
	_, toDynamic := e.liveDynamic[to]
	if !fromDynamic && !toDynamic {
		return true
	}
	a, aOK := e.market.Tokens[from]
	b, bOK := e.market.Tokens[to]
	if !aOK || !bOK {
		return false
	}
	_, ok := e.livePairKeys[canonicalAddressPair(a.Address, b.Address)]
	return ok
}

func canonicalAddressPair(a, b string) string {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	if b < a {
		a, b = b, a
	}
	return a + ":" + b
}

func (e *Engine) incrementalRefresh(ctx context.Context, fromBlock, stateBlock uint64, tracker *detectorStageTracker) (map[string]struct{}, refreshStats, error) {
	stats := refreshStats{UnresolvedDirty: make(map[string]struct{})}
	current := e.cache.Snapshot()
	if len(current) == 0 {
		return map[string]struct{}{}, stats, nil
	}
	addresses := make([]string, 0, len(current))
	for _, pool := range current {
		addresses = append(addresses, pool.Address)
	}
	dirty, logStats, logErr := e.discoverer.ChangedPoolAddressesAtWithStats(ctx, addresses, fromBlock, stateBlock)
	stats.Logs = logStats
	tracker.FinishGetLogs(stats)
	refresh := current
	if logErr == nil {
		refresh = poolsByAddress(current, dirty)
	} else {
		if ctx.Err() != nil {
			return nil, stats, ctx.Err()
		}
		stats.MarketStateIncomplete = true
		stats.LastFailure = fmt.Sprintf("%T", logErr)
		// Correctness fallback: if log-based invalidation is unavailable, refresh
		// and recompute every cached pool rather than risk a stale candidate.
		dirty = make(map[string]struct{}, len(current))
		for _, pool := range current {
			dirty[strings.ToLower(pool.Address)] = struct{}{}
		}
	}
	if len(refresh) == 0 {
		return dirty, stats, nil
	}
	tracker.Stage(stagePoolRefresh)
	tracker.StartPoolRefresh(uint64(len(refresh)))
	type result struct {
		before, after pools.Pool
		err           error
		duration      time.Duration
	}
	jobs := make(chan pools.Pool)
	results := make(chan result, len(refresh))
	var wg sync.WaitGroup
	for i := 0; i < e.discoveryWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pool := range jobs {
				started := time.Now()
				updated, err := e.discoverer.RefreshPoolAt(ctx, pool, stateBlock)
				results <- result{before: pool, after: updated, err: err, duration: time.Since(started)}
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
		stats.PoolsAttempted++
		if result.err != nil {
			tracker.PoolRefreshResult(false, result.duration)
			if ctx.Err() != nil {
				return nil, stats, ctx.Err()
			}
			stats.PoolsFailed++
			stats.MarketStateIncomplete = true
			stats.LastFailure = fmt.Sprintf("%T", result.err)
			address := strings.ToLower(result.before.Address)
			if address != "" {
				stats.UnresolvedDirty[address] = struct{}{}
				dirty[address] = struct{}{}
			}
			continue
		}
		tracker.PoolRefreshResult(true, result.duration)
		stats.PoolsSucceeded++
		address := strings.ToLower(result.after.Address)
		changed := poolChanged(result.before, result.after)
		if !changed {
			result.after.LastUpdatedBlock = result.before.LastUpdatedBlock
		}
		e.cache.Put(result.after)
		if logErr != nil && changed {
			dirty[address] = struct{}{}
		}
		if logErr == nil && !changed {
			delete(dirty, address)
		}
	}
	return dirty, stats, nil
}

func (e *Engine) refreshKnownDirty(ctx context.Context, dirty map[string]struct{}, stateBlock uint64, tracker *detectorStageTracker) refreshStats {
	stats := refreshStats{UnresolvedDirty: make(map[string]struct{}), ResolvedDirty: make(map[string]struct{})}
	current := e.cache.Snapshot()
	if len(current) == 0 || len(dirty) == 0 {
		return stats
	}
	refresh := poolsByAddress(current, dirty)
	if len(refresh) == 0 {
		return stats
	}
	tracker.Stage(stagePoolRefresh)
	tracker.StartPoolRefresh(uint64(len(refresh)))
	type result struct {
		before, after pools.Pool
		err           error
		duration      time.Duration
	}
	jobs := make(chan pools.Pool)
	results := make(chan result, len(refresh))
	var wg sync.WaitGroup
	for i := 0; i < e.discoveryWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pool := range jobs {
				started := time.Now()
				updated, err := e.discoverer.RefreshPoolAt(ctx, pool, stateBlock)
				results <- result{before: pool, after: updated, err: err, duration: time.Since(started)}
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
		stats.PoolsAttempted++
		address := strings.ToLower(result.before.Address)
		if result.err != nil {
			tracker.PoolRefreshResult(false, result.duration)
			if ctx.Err() != nil {
				stats.MarketStateIncomplete = true
				stats.LastFailure = fmt.Sprintf("%T", ctx.Err())
				if address != "" {
					stats.UnresolvedDirty[address] = struct{}{}
				}
				continue
			}
			stats.PoolsFailed++
			stats.MarketStateIncomplete = true
			stats.LastFailure = fmt.Sprintf("%T", result.err)
			if address != "" {
				stats.UnresolvedDirty[address] = struct{}{}
			}
			continue
		}
		tracker.PoolRefreshResult(true, result.duration)
		stats.PoolsSucceeded++
		afterAddress := strings.ToLower(result.after.Address)
		if afterAddress == "" {
			afterAddress = address
		}
		changed := poolChanged(result.before, result.after)
		if !changed {
			result.after.LastUpdatedBlock = result.before.LastUpdatedBlock
			delete(dirty, afterAddress)
		} else {
			dirty[afterAddress] = struct{}{}
		}
		e.cache.Put(result.after)
		if afterAddress != "" {
			stats.ResolvedDirty[afterAddress] = struct{}{}
		}
	}
	return stats
}

func changedPoolAddresses(before, after []pools.Pool) map[string]struct{} {
	previous := make(map[string]pools.Pool, len(before))
	for _, pool := range before {
		previous[strings.ToLower(pool.Address)] = pool
	}
	changed := make(map[string]struct{})
	for _, pool := range after {
		key := strings.ToLower(pool.Address)
		old, ok := previous[key]
		if !ok || poolChanged(old, pool) {
			changed[key] = struct{}{}
		}
		delete(previous, key)
	}
	// Removed pools invalidate historical cache entries even though routes that
	// referenced them disappear from the active graph.
	for key := range previous {
		changed[key] = struct{}{}
	}
	return changed
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

func routesWithoutPools(all []routes.Route, excluded map[string]struct{}) []routes.Route {
	if len(excluded) == 0 {
		return all
	}
	out := make([]routes.Route, 0, len(all))
	for _, route := range all {
		blocked := false
		for _, pool := range route.Hops {
			if _, ok := excluded[strings.ToLower(pool.Address)]; ok {
				blocked = true
				break
			}
		}
		if !blocked {
			out = append(out, route)
		}
	}
	return out
}

func stringSetEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for key := range a {
		if _, ok := b[key]; !ok {
			return false
		}
	}
	return true
}

type evaluationReport struct {
	OptimizerRuns           uint64
	OptimizerRequested      uint64
	OptimizerSamples        uint64
	RoutesEvaluated         uint64
	OptimizerDuration       time.Duration
	NearMisses              []nearmiss.Record
	OptimizerBudget         map[string]int
	ExploitSelected         uint64
	ExploreSelected         uint64
	NewSelected             uint64
	MemoryRoutes            int
	AvgPreQuoteScore        int64
	CrossVenueQuoted        uint64
	SameDEXQuoted           uint64
	RoutesConsidered        uint64
	QuoteAgeBlocks          uint64
	QuoteCacheHits          uint64
	QuoteCacheMisses        uint64
	QuoteDedupHits          uint64
	QuoteCacheInvalidations uint64
	QuoteBudget             int
	QuoteBudgetUsed         int
	QuoteBudgetDropped      uint64
	OptimizerSaved          uint64
	RoutesDeepOptimized     uint64
	RoutesProbeOnly         uint64
	RoutesSkippedDeep       uint64
	RoutesSkippedByPreQuote uint64
	RPCCallsInitialQuotes   uint64
	RPCCallsOptimizer       uint64
	RPCCallsEconomics       uint64
	ScoreDeciles            map[string]map[string]any
}

func (e *Engine) evaluate(ctx context.Context, candidates []routes.Route, options SearchOptions, signals map[string]volatility.Signal, volatilityWeight float64, stateBlock uint64, tracker *detectorStageTracker) evaluationReport {
	report := evaluationReport{}
	byAsset := make(map[string][]routes.Route)
	for _, route := range candidates {
		byAsset[route.Symbols[0]] = append(byAsset[route.Symbols[0]], route)
	}
	assets := make([]string, 0, len(byAsset))
	for asset := range byAsset {
		assets = append(assets, asset)
	}
	sort.Strings(assets)
	type pendingOptimization struct {
		asset      string
		amount     *big.Int
		candidates []optimizerCandidate
	}
	pending := make([]pendingOptimization, 0, len(assets))
	allEvaluated := make([]evaluatedRoute, 0)
	rpcBeforeInitial := e.rpcCalls()
	// Reserve most of the hard quote ceiling for the small set of routes that
	// prove promising enough for optimizer work. The initial detector probe is
	// deliberately cheap and entirely local-ranked.
	remainingQuotes := minInt(options.MaxQuotesPerCycle, maxInt(8, options.MaxQuotesPerCycle*3/10))
	for assetIndex, asset := range assets {
		assetCandidates := byAsset[asset]
		amount, ok := e.amounts[asset]
		if !ok {
			// Discovery remains asset-agnostic, but economics are unsafe without
			// an asset-specific notional. Skip rather than reuse another token's
			// raw units or manufacture a USD conversion.
			continue
		}
		assetOptions := options
		assetsRemaining := len(assets) - assetIndex
		assetQuoteBudget := remainingQuotes
		if assetsRemaining > 0 {
			assetQuoteBudget = (remainingQuotes + assetsRemaining - 1) / assetsRemaining
		}
		tracker.Stage(stageRouteSelection)
		evaluationCandidates, selection := e.selectEvaluationCandidates(assetCandidates, assetOptions, signals, volatilityWeight, stateBlock)
		evaluationCandidates, usedQuotes := boundRoutesByQuoteCost(evaluationCandidates, assetQuoteBudget)
		tracker.AddRoutesSelected(len(evaluationCandidates))
		selection = summarizeBoundedSelection(evaluationCandidates, selection)
		if usedQuotes > remainingQuotes {
			usedQuotes = remainingQuotes
		}
		remainingQuotes -= usedQuotes
		report.RoutesConsidered += uint64(len(assetCandidates))
		if len(assetCandidates) > len(evaluationCandidates) {
			report.RoutesSkippedByPreQuote += uint64(len(assetCandidates) - len(evaluationCandidates))
		}
		report.ExploitSelected += uint64(selection.Exploit)
		report.ExploreSelected += uint64(selection.Explore)
		report.NewSelected += uint64(selection.New)
		report.CrossVenueQuoted += uint64(selection.CrossVenue)
		report.SameDEXQuoted += uint64(selection.SameDEX)
		report.AvgPreQuoteScore = combineAverages(report.AvgPreQuoteScore, report.RoutesEvaluated, selection.AvgScore, uint64(len(evaluationCandidates)))
		report.RoutesEvaluated += uint64(len(evaluationCandidates))
		evaluated := e.evaluateCurrent(ctx, evaluationCandidates, amount, stateBlock, selection.Scores, tracker)
		allEvaluated = append(allEvaluated, evaluated...)
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

		optimizerLimit := options.OptimizerRoutesPerAsset
		alreadyPending := 0
		for _, p := range pending {
			alreadyPending += len(p.candidates)
		}
		if left := options.MaxOptimizedRoutes - alreadyPending; optimizerLimit > left {
			optimizerLimit = left
		}
		tracker.Stage(stageOptimizerSelection)
		optimizerCandidates := selectOptimizerCandidates(append([]evaluatedRoute(nil), evaluated...), optimizerLimit)
		if len(optimizerCandidates) > 0 {
			pending = append(pending, pendingOptimization{asset: asset, amount: amount, candidates: optimizerCandidates})
		}
	}
	rpcAfterInitial := e.rpcCalls()
	if e.evaluator != nil {
		report.RPCCallsInitialQuotes = e.evaluator.QuoteCacheStats().Misses
	}
	initialTotal := safeUint64Delta(rpcAfterInitial, rpcBeforeInitial)
	report.RPCCallsEconomics = safeUint64Delta(initialTotal, report.RPCCallsInitialQuotes)

	remainingBudget := options.OptimizerSamplesPerCycle
	rpcBeforeOptimizer := e.rpcCalls()
	for index, item := range pending {
		assetBudget := 0
		if options.AdaptiveOptimizer {
			assetsRemaining := len(pending) - index
			if remainingBudget < 2 || assetsRemaining < 1 {
				for _, candidate := range item.candidates {
					report.OptimizerRequested += uint64(samplesForScore(candidate.score, options.OptimizerSamplesPerRoute, options.NormalOptimizerSamples))
					report.RoutesSkippedDeep++
				}
				continue
			}
			assetBudget = remainingBudget / assetsRemaining
			if assetBudget < 2 {
				assetBudget = 2
			}
		}
		optimizerStarted := time.Now()
		tracker.Stage(stageOptimizer)
		optimized := e.optimizeRoutes(ctx, item.asset, item.candidates, item.amount, options.OptimizerSamplesPerRoute, options.NormalOptimizerSamples, assetBudget, options.AdaptiveOptimizer, options.EarlyStop)
		report.OptimizerDuration += time.Since(optimizerStarted)
		report.OptimizerRuns += optimized.OptimizerRuns
		report.OptimizerRequested += optimized.OptimizerRequested
		report.OptimizerSamples += optimized.OptimizerSamples
		report.OptimizerSaved += optimized.OptimizerSaved
		report.RoutesDeepOptimized += optimized.RoutesDeepOptimized
		report.RoutesProbeOnly += optimized.RoutesProbeOnly
		report.RoutesSkippedDeep += optimized.RoutesSkippedDeep
		if options.AdaptiveOptimizer {
			used := int(optimized.OptimizerSamples)
			if used > remainingBudget {
				remainingBudget = 0
			} else {
				remainingBudget -= used
			}
		}
		if report.OptimizerBudget == nil {
			report.OptimizerBudget = make(map[string]int)
		}
		for key, value := range optimized.OptimizerBudget {
			report.OptimizerBudget[key] += value
		}
	}
	if report.OptimizerRequested > report.OptimizerSamples {
		report.OptimizerSaved = report.OptimizerRequested - report.OptimizerSamples
	}
	rpcAfterOptimizer := e.rpcCalls()
	report.RPCCallsOptimizer = safeUint64Delta(rpcAfterOptimizer, rpcBeforeOptimizer)
	report.ScoreDeciles = scoreDecileQuality(allEvaluated)
	if e.routeMemory != nil {
		report.MemoryRoutes = e.routeMemory.Len()
	}
	if e.evaluator != nil {
		stats := e.evaluator.QuoteCacheStats()
		report.QuoteCacheHits = stats.Hits
		report.QuoteDedupHits = stats.DedupHits
		report.QuoteCacheMisses = stats.Misses
		report.QuoteCacheInvalidations = stats.Invalidations
		report.QuoteBudget = stats.Budget
		report.QuoteBudgetUsed = stats.BudgetUsed
		report.QuoteBudgetDropped = stats.BudgetDropped
	}
	e.publishUniverseFeedback(allEvaluated)
	return report
}

func (e *Engine) publishUniverseFeedback(evaluated []evaluatedRoute) {
	if e.universeFeedback == nil || len(evaluated) == 0 {
		return
	}
	execution := make(map[string]struct{})
	for _, a := range e.market.ExecutionAssets() {
		execution[a] = struct{}{}
	}
	type counts struct{ evaluations, useful uint64 }
	byAsset := make(map[string]counts)
	for _, item := range evaluated {
		seen := make(map[string]struct{})
		for _, asset := range item.route.Symbols {
			if _, core := execution[asset]; core {
				continue
			}
			if _, ok := seen[asset]; ok {
				continue
			}
			seen[asset] = struct{}{}
			c := byAsset[asset]
			c.evaluations++
			if item.quoteSuccessful && item.nearMiss != nil && item.nearMiss.Score >= 2000 {
				c.useful++
			}
			byAsset[asset] = c
		}
	}
	for asset, c := range byAsset {
		e.universeFeedback(asset, c.evaluations, c.useful)
	}
}

type evaluatedRoute struct {
	route           routes.Route
	score           *big.Int
	nearMiss        *nearmiss.Record
	routeScore      int64
	preQuoteScore   int64
	quoteSuccessful bool
	quoteAgeBlocks  uint64
}

func (e *Engine) evaluateCurrent(ctx context.Context, candidates []routes.Route, amount *big.Int, stateBlock uint64, preQuoteScores map[string]int64, tracker *detectorStageTracker) []evaluatedRoute {
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
				tracker.Stage(stageQuote)
				tracker.RouteEvaluationStarted()
				quoteCtx := rpc.WithRequestMetadata(rpc.WithRequestClass(ctx, rpc.HotPath), "quote", stateBlock)
				op, err := e.evaluator.Evaluate(quoteCtx, route, amount)
				if err != nil || op == nil || op.ExpectedProfit == nil {
					tracker.RouteEvaluationFinished(false)
					results <- evaluatedRoute{route: route, preQuoteScore: preQuoteScores[routeKey(route)]}
					continue
				}
				tracker.Stage(stageEconomicEvaluation)
				reason := ""
				if op.MinProfit != nil && op.ExpectedProfit.Cmp(op.MinProfit) <= 0 {
					reason = "profitability threshold not met"
				}
				record := nearmiss.FromOpportunity(op, reason, time.Since(started), failures[routeKey(route)])
				tracker.RouteEvaluationFinished(true)
				results <- evaluatedRoute{route: route, score: new(big.Int).Set(op.ExpectedProfit), nearMiss: &record, routeScore: record.Score, preQuoteScore: preQuoteScores[routeKey(route)], quoteSuccessful: true, quoteAgeBlocks: quoteAgeBlocks(stateBlock, forcedRouteSourceBlock(route))}
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
	New        int
	CrossVenue int
	SameDEX    int
	AvgScore   int64
	Scores     map[string]int64
	Kinds      map[string]string
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
		if cross && len(route.Hops) == 2 {
			score += 400
		}
		score += int64(e.routePairQuality(route) * 50)
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
	newSlots := limit / 10
	if newSlots == 0 && limit >= 3 {
		newSlots = 1
	}
	if newSlots+exploreSlots > limit {
		newSlots = 0
	}
	exploitSlots := limit - exploreSlots - newSlots
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].key < scored[j].key
	})
	selected := make([]routes.Route, 0, limit)
	selectedKeys := make(map[string]struct{}, limit)
	selection := evaluationSelection{Scores: make(map[string]int64, limit), Kinds: make(map[string]string, limit)}
	add := func(item scoredRoute, kind string) bool {
		if len(selected) >= limit {
			return false
		}
		if _, ok := selectedKeys[item.key]; ok {
			return false
		}
		selectedKeys[item.key] = struct{}{}
		selected = append(selected, item.route)
		selection.Kinds[item.key] = kind
		return true
	}
	for _, item := range scored {
		if selection.Exploit >= exploitSlots {
			break
		}
		if add(item, "exploit") {
			selection.Exploit++
		}
	}
	for _, item := range scored {
		if selection.New >= newSlots {
			break
		}
		if item.observations == 0 && add(item, "new") {
			selection.New++
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
		if add(item, "explore") {
			selection.Explore++
		}
	}
	for _, item := range scored {
		if len(selected) >= limit {
			break
		}
		if add(item, "exploit") {
			selection.Exploit++
		}
	}
	var total int64
	for _, route := range selected {
		key := routeKey(route)
		for _, item := range scored {
			if item.key == key {
				total += item.score
				selection.Scores[key] = item.score
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

func (e *Engine) routePairQuality(route routes.Route) float64 {
	if e.pairScore == nil || len(route.Symbols) < 2 {
		return 0
	}
	weak, denom := 100.0, 0.0
	for i := 0; i < len(route.Symbols)-1; i++ {
		a, oka := e.market.Tokens[route.Symbols[i]]
		b, okb := e.market.Tokens[route.Symbols[i+1]]
		if !oka || !okb {
			return 0
		}
		s, ok := e.pairScore(a.Address, b.Address)
		if !ok || s <= 0 {
			return 0
		}
		if s < weak {
			weak = s
		}
		denom += 1 / s
	}
	harm := float64(len(route.Symbols)-1) / denom
	return .6*weak + .4*harm
}

func boundRoutesByQuoteCost(input []routes.Route, budget int) ([]routes.Route, int) {
	if budget < 1 {
		return nil, 0
	}
	out := make([]routes.Route, 0, len(input))
	used := 0
	for _, route := range input {
		cost := len(route.Hops)
		if cost < 1 {
			cost = 1
		}
		if used+cost > budget {
			continue
		}
		out = append(out, route)
		used += cost
	}
	return out, used
}

func summarizeBoundedSelection(selected []routes.Route, original evaluationSelection) evaluationSelection {
	result := evaluationSelection{Scores: make(map[string]int64, len(selected)), Kinds: make(map[string]string, len(selected))}
	var total int64
	for _, route := range selected {
		key := routeKey(route)
		score := original.Scores[key]
		result.Scores[key] = score
		result.Kinds[key] = original.Kinds[key]
		total += score
		switch original.Kinds[key] {
		case "new":
			result.New++
		case "explore":
			result.Explore++
		default:
			result.Exploit++
		}
		if nearmiss.CrossVenue(route) {
			result.CrossVenue++
		} else {
			result.SameDEX++
		}
	}
	if len(selected) > 0 {
		result.AvgScore = total / int64(len(selected))
	}
	return result
}

func mergeNearMisses(previous, current []nearmiss.Record, limit int) []nearmiss.Record {
	if limit < 1 {
		return nil
	}
	byKey := make(map[string]nearmiss.Record, len(previous)+len(current))
	for _, record := range append(append([]nearmiss.Record(nil), previous...), current...) {
		amount := ""
		if record.AmountIn != nil {
			amount = record.AmountIn.String()
		}
		key := record.Route + "|" + record.DEXPath + "|" + amount
		if old, ok := byKey[key]; !ok || record.Timestamp.After(old.Timestamp) {
			byKey[key] = record
		}
	}
	result := make([]nearmiss.Record, 0, len(byKey))
	for _, record := range byKey {
		result = append(result, record)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if !result[i].Timestamp.Equal(result[j].Timestamp) {
			return result[i].Timestamp.After(result[j].Timestamp)
		}
		return result[i].Route < result[j].Route
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

func summarizeLegacySelection(selected []routes.Route) evaluationSelection {
	selection := evaluationSelection{Exploit: len(selected), Scores: make(map[string]int64, len(selected))}
	for _, route := range selected {
		selection.Scores[routeKey(route)] = 0
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

type optimizerMemoEntry struct {
	value optimizer.Evaluation
	err   error
}

type memoizingOptimizerAdapter struct {
	base optimizerAdapter
	mu   sync.Mutex
	seen map[string]optimizerMemoEntry
}

func newMemoizingOptimizerAdapter(evaluator *opportunity.Engine) *memoizingOptimizerAdapter {
	return &memoizingOptimizerAdapter{base: optimizerAdapter{evaluator: evaluator}, seen: make(map[string]optimizerMemoEntry)}
}

func (a *memoizingOptimizerAdapter) Evaluate(ctx context.Context, asset string, route graph.Route, amount *big.Int) (optimizer.Evaluation, error) {
	key := amount.String()
	a.mu.Lock()
	entry, ok := a.seen[key]
	a.mu.Unlock()
	if ok {
		return cloneOptimizerEvaluation(entry.value), entry.err
	}
	value, err := a.base.Evaluate(ctx, asset, route, amount)
	a.mu.Lock()
	a.seen[key] = optimizerMemoEntry{value: cloneOptimizerEvaluation(value), err: err}
	a.mu.Unlock()
	return value, err
}

func cloneOptimizerEvaluation(value optimizer.Evaluation) optimizer.Evaluation {
	if value.Amount != nil {
		value.Amount = new(big.Int).Set(value.Amount)
	}
	if value.NetProfit != nil {
		value.NetProfit = new(big.Int).Set(value.NetProfit)
	}
	return value
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

func ratio(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func scoreDecileQuality(input []evaluatedRoute) map[string]map[string]any {
	if len(input) == 0 {
		return nil
	}
	items := append([]evaluatedRoute(nil), input...)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].preQuoteScore != items[j].preQuoteScore {
			return items[i].preQuoteScore > items[j].preQuoteScore
		}
		return routeKey(items[i].route) < routeKey(items[j].route)
	})
	type accumulator struct {
		count, successful, nearMiss, optimizerEligible int
		gapTotal, bestGap                              float64
	}
	accumulators := make(map[string]*accumulator)
	for index, item := range items {
		decile := (index*10)/len(items) + 1
		if decile > 10 {
			decile = 10
		}
		key := fmt.Sprintf("d%d", decile)
		bucket := accumulators[key]
		if bucket == nil {
			bucket = &accumulator{bestGap: -1}
			accumulators[key] = bucket
		}
		bucket.count++
		if item.quoteSuccessful {
			bucket.successful++
		}
		if optimizerEligible(item) {
			bucket.optimizerEligible++
		}
		if item.nearMiss == nil || item.nearMiss.GapToProfit == nil || item.nearMiss.MinProfit == nil || item.nearMiss.MinProfit.Sign() <= 0 {
			continue
		}
		bucket.nearMiss++
		gapRatio, _ := new(big.Rat).SetFrac(item.nearMiss.GapToProfit, item.nearMiss.MinProfit).Float64()
		gapBPS := gapRatio * 10_000
		bucket.gapTotal += gapBPS
		if bucket.bestGap < 0 || gapBPS < bucket.bestGap {
			bucket.bestGap = gapBPS
		}
	}
	result := make(map[string]map[string]any, len(accumulators))
	for key, bucket := range accumulators {
		averageGap := 0.0
		bestGap := 0.0
		if bucket.nearMiss > 0 {
			averageGap = bucket.gapTotal / float64(bucket.nearMiss)
			bestGap = bucket.bestGap
		}
		result[key] = map[string]any{
			"routes":                    bucket.count,
			"quote_success_pct":         100 * float64(bucket.successful) / float64(bucket.count),
			"near_miss_rate_pct":        100 * float64(bucket.nearMiss) / float64(bucket.count),
			"avg_gap_to_threshold_bps":  averageGap,
			"best_gap_to_threshold_bps": bestGap,
			"optimizer_eligible_routes": bucket.optimizerEligible,
		}
	}
	return result
}

func (e *Engine) optimizeRoutes(ctx context.Context, asset string, candidates []optimizerCandidate, current *big.Int, samples, normalSamples, sampleBudget int, adaptive, earlyStop bool) evaluationReport {
	ctx = rpc.WithRequestMetadata(rpc.WithRequestClass(ctx, rpc.HotPath), "optimizer", e.getLastStateBlock())
	if !adaptive {
		return e.optimizeRoutesLegacy(ctx, asset, candidates, current, samples)
	}
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

	remaining := sampleBudget
	for _, candidate := range candidates {
		route := candidate.route
		requestedSamples := samplesForScore(candidate.score, samples, normalSamples)
		report.OptimizerRequested += uint64(requestedSamples)
		if remaining < 2 {
			report.RoutesSkippedDeep++
			continue
		}
		allocatedSamples := minInt(requestedSamples, remaining)
		report.OptimizerBudget[routeKey(route)] = allocatedSamples
		graphRoute := graph.Route{
			Assets: append([]string(nil), route.Symbols...),
			Hops:   append([]pools.Pool(nil), route.Hops...),
		}

		probeSamples := minInt(allocatedSamples, 3)
		report.OptimizerRuns++
		adapter := newMemoizingOptimizerAdapter(e.evaluator)
		minimum := new(big.Int).Div(new(big.Int).Set(current), big.NewInt(10))
		if minimum.Sign() <= 0 {
			minimum = new(big.Int).Set(current)
		}
		best, err := e.optimizer.Optimize(
			ctx,
			adapter,
			asset,
			graphRoute,
			optimizer.Range{Min: minimum, Max: max, Samples: probeSamples},
		)
		report.OptimizerSamples += uint64(best.Evaluated)
		remaining -= minInt(best.Evaluated, remaining)

		if err != nil {
			report.RoutesProbeOnly++
			report.RoutesSkippedDeep++
			continue
		}

		deepSamples := allocatedSamples - best.Evaluated
		if (earlyStop && shouldStopOptimizerAfterProbe(candidate, best, current)) || deepSamples < 2 {
			report.RoutesProbeOnly++
			report.RoutesSkippedDeep++
			if best.Amount != nil && best.NetProfit != nil && best.NetProfit.Sign() > 0 {
				optimizedRoute := routes.Route{
					Symbols: append([]string(nil), route.Symbols...),
					Hops:    append([]pools.Pool(nil), route.Hops...),
				}
				_, _ = e.evaluator.Evaluate(ctx, optimizedRoute, best.Amount)
			}
			continue
		}

		refinedMin, refinedMax := refineOptimizerRange(minimum, max, best.Amount)
		deep, err := e.optimizer.Optimize(
			ctx,
			adapter,
			asset,
			graphRoute,
			optimizer.Range{Min: refinedMin, Max: refinedMax, Samples: deepSamples},
		)
		report.OptimizerSamples += uint64(deep.Evaluated)
		remaining -= minInt(deep.Evaluated, remaining)
		report.RoutesDeepOptimized++

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
	if report.OptimizerRequested > report.OptimizerSamples {
		report.OptimizerSaved = report.OptimizerRequested - report.OptimizerSamples
	}
	return report
}

func (e *Engine) optimizeRoutesLegacy(ctx context.Context, asset string, candidates []optimizerCandidate, current *big.Int, samples int) evaluationReport {
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
		allocated := samplesForScore(candidate.score, samples, maxInt(2, samples/2))
		report.OptimizerBudget[routeKey(candidate.route)] = allocated
		report.OptimizerRequested += uint64(allocated)
		graphRoute := graph.Route{Assets: append([]string(nil), candidate.route.Symbols...), Hops: append([]pools.Pool(nil), candidate.route.Hops...)}
		minimum := new(big.Int).Div(new(big.Int).Set(current), big.NewInt(10))
		if minimum.Sign() <= 0 {
			minimum = new(big.Int).Set(current)
		}
		report.OptimizerRuns++
		best, err := e.optimizer.Optimize(ctx, optimizerAdapter{evaluator: e.evaluator}, asset, graphRoute, optimizer.Range{Min: minimum, Max: max, Samples: minInt(allocated, 3)})
		report.OptimizerSamples += uint64(best.Evaluated)
		if err == nil && minInt(allocated, 3) < allocated {
			deep, deepErr := e.optimizer.Optimize(ctx, optimizerAdapter{evaluator: e.evaluator}, asset, graphRoute, optimizer.Range{Min: minimum, Max: max, Samples: allocated})
			report.OptimizerSamples += uint64(deep.Evaluated)
			if deepErr == nil && deep.NetProfit != nil && (best.NetProfit == nil || deep.NetProfit.Cmp(best.NetProfit) > 0) {
				best = deep
			}
			report.RoutesDeepOptimized++
		} else {
			report.RoutesProbeOnly++
		}
		if best.Amount != nil && best.NetProfit != nil && best.NetProfit.Sign() > 0 {
			_, _ = e.evaluator.Evaluate(ctx, candidate.route, best.Amount)
		}
	}
	return report
}

func refineOptimizerRange(minimum, maximum, best *big.Int) (*big.Int, *big.Int) {
	if minimum == nil || maximum == nil {
		return big.NewInt(0), big.NewInt(0)
	}
	if best == nil || maximum.Cmp(minimum) <= 0 {
		return new(big.Int).Set(minimum), new(big.Int).Set(maximum)
	}
	quarter := new(big.Int).Div(new(big.Int).Sub(maximum, minimum), big.NewInt(4))
	if quarter.Sign() <= 0 {
		return new(big.Int).Set(minimum), new(big.Int).Set(maximum)
	}
	left := new(big.Int).Sub(best, quarter)
	if left.Cmp(minimum) < 0 {
		left.Set(minimum)
	}
	right := new(big.Int).Add(best, quarter)
	if right.Cmp(maximum) > 0 {
		right.Set(maximum)
	}
	if right.Cmp(left) <= 0 {
		return new(big.Int).Set(minimum), new(big.Int).Set(maximum)
	}
	return left, right
}

func samplesForScore(score int64, high, normal int) int {
	if high < 2 {
		high = 2
	}
	if normal < 2 {
		normal = 2
	}
	if normal > high {
		normal = high
	}
	switch {
	case score >= 4_000:
		return high
	case score < 2_000:
		return 2
	default:
		return normal
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
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
