package market

import (
	"sync"
	"time"
)

const (
	stageCycleStart               = "cycle_start"
	stageApplyPendingMarket       = "apply_pending_market"
	stageReconcileCheck           = "reconcile_check"
	stageStartupReconciliation    = "startup_reconciliation"
	stageIncrementalRefreshGate   = "incremental_refresh_gate"
	stageGetLogsDirtyDetection    = "getlogs_dirty_detection"
	stagePoolRefresh              = "pool_refresh"
	stageDirtyRouteMapping        = "dirty_route_mapping"
	stageRouteSelection           = "route_selection"
	stagePreQuote                 = "prequote"
	stageQuote                    = "quote"
	stageEconomicEvaluation       = "economic_evaluation"
	stageOptimizerSelection       = "optimizer_selection"
	stageOptimizer                = "optimizer"
	stageBackgroundReconciliation = "background_reconciliation"
	stageCycleCommit              = "cycle_commit"
	stageCycleComplete            = "cycle_complete"
)

type DetectorStageTelemetry struct {
	CurrentStage               string
	StageStartedAt             time.Time
	CycleStartedAt             time.Time
	SourceBlock                uint64
	LatestBlockAtStageStart    uint64
	CycleElapsed               time.Duration
	ActiveStageElapsed         time.Duration
	LastCompletedStage         string
	LastCompletedStageDuration time.Duration
	LastStateBlockBeforeCycle  uint64
	LastStateBlockAtCancel     uint64

	ReconciliationPending    bool
	ReconciliationUnitsDone  int
	ReconciliationUnitsTotal int

	GetLogsFromBlock       uint64
	GetLogsToBlock         uint64
	GetLogsBlocksToScan    uint64
	GetLogsChunksPlanned   uint64
	GetLogsChunksAttempted uint64
	GetLogsChunksSucceeded uint64
	GetLogsChunksFailed    uint64
	GetLogsBlocksScanned   uint64
	GetLogsDuration        time.Duration

	PoolRefreshRequested uint64
	PoolRefreshAttempted uint64
	PoolRefreshSucceeded uint64
	PoolRefreshFailed    uint64
	PoolRefreshDuration  time.Duration
	MaxSinglePoolRefresh time.Duration

	DirtyPoolsFound         uint64
	AffectedRoutesFound     uint64
	RoutesSelected          uint64
	RoutesEvaluationStarted uint64
	RoutesEvaluated         uint64
	QuoteAttempts           uint64
	QuoteSuccesses          uint64
	QuoteFailures           uint64

	RPCCallsStarted   uint64
	RPCCallsCompleted uint64
	RPCLimiterWait    time.Duration
	RPCTransport      time.Duration
	RateLimiterWait   time.Duration
}

type detectorStageTracker struct {
	mu sync.Mutex

	currentStage               string
	stageStartedAt             time.Time
	cycleStartedAt             time.Time
	sourceBlock                uint64
	latest                     func() uint64
	latestBlockAtStageStart    uint64
	lastCompletedStage         string
	lastCompletedStageDuration time.Duration
	lastStateBlockBeforeCycle  uint64
	lastStateBlockAtCancel     uint64

	reconciliationPending    bool
	reconciliationUnitsDone  int
	reconciliationUnitsTotal int

	getLogsFromBlock       uint64
	getLogsToBlock         uint64
	getLogsBlocksToScan    uint64
	getLogsChunksPlanned   uint64
	getLogsChunksAttempted uint64
	getLogsChunksSucceeded uint64
	getLogsChunksFailed    uint64
	getLogsBlocksScanned   uint64
	getLogsStartedAt       time.Time
	getLogsDuration        time.Duration

	poolRefreshRequested uint64
	poolRefreshAttempted uint64
	poolRefreshSucceeded uint64
	poolRefreshFailed    uint64
	poolRefreshStartedAt time.Time
	poolRefreshDuration  time.Duration
	maxSinglePoolRefresh time.Duration

	dirtyPoolsFound         uint64
	affectedRoutesFound     uint64
	routesSelected          uint64
	routesEvaluationStarted uint64
	routesEvaluated         uint64
	quoteAttempts           uint64
	quoteSuccesses          uint64
	quoteFailures           uint64

	rpcCallsStarted   uint64
	rpcCallsCompleted uint64
	rpcLimiterWait    time.Duration
	rpcTransport      time.Duration
}

func newDetectorStageTracker(sourceBlock, lastStateBlock uint64, latest func() uint64) *detectorStageTracker {
	now := time.Now()
	return &detectorStageTracker{
		currentStage:              stageCycleStart,
		stageStartedAt:            now,
		cycleStartedAt:            now,
		sourceBlock:               sourceBlock,
		latest:                    latest,
		latestBlockAtStageStart:   latestBlock(latest),
		lastStateBlockBeforeCycle: lastStateBlock,
		lastStateBlockAtCancel:    lastStateBlock,
	}
}

func latestBlock(latest func() uint64) uint64 {
	if latest == nil {
		return 0
	}
	return latest()
}

func (t *detectorStageTracker) Stage(stage string) {
	if t == nil || stage == "" {
		return
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.currentStage != "" {
		t.lastCompletedStage = t.currentStage
		t.lastCompletedStageDuration = now.Sub(t.stageStartedAt)
	}
	t.currentStage = stage
	t.stageStartedAt = now
	t.latestBlockAtStageStart = latestBlock(t.latest)
}

func (t *detectorStageTracker) SetLastStateBlock(block uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.lastStateBlockAtCancel = block
	t.mu.Unlock()
}

func (t *detectorStageTracker) SetReconciliation(pending bool, done, total int) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.reconciliationPending = pending
	t.reconciliationUnitsDone = done
	t.reconciliationUnitsTotal = total
	t.mu.Unlock()
}

func (t *detectorStageTracker) StartGetLogs(from, to, chunkSize uint64) {
	if t == nil {
		return
	}
	if from == 0 || to == 0 || from > to {
		from = to
	}
	blocks := uint64(0)
	chunks := uint64(0)
	if to >= from && to > 0 {
		blocks = to - from + 1
		if chunkSize > 0 {
			chunks = (blocks + chunkSize - 1) / chunkSize
		}
	}
	t.mu.Lock()
	t.getLogsFromBlock = from
	t.getLogsToBlock = to
	t.getLogsBlocksToScan = blocks
	t.getLogsChunksPlanned = chunks
	t.getLogsStartedAt = time.Now()
	t.mu.Unlock()
}

func (t *detectorStageTracker) FinishGetLogs(stats refreshStats) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.getLogsChunksAttempted = stats.Logs.ChunksAttempted
	t.getLogsChunksSucceeded = stats.Logs.ChunksSucceeded
	t.getLogsChunksFailed = stats.Logs.ChunksFailed
	t.getLogsBlocksScanned = stats.Logs.BlocksScanned
	if !t.getLogsStartedAt.IsZero() {
		t.getLogsDuration += time.Since(t.getLogsStartedAt)
		t.getLogsStartedAt = time.Time{}
	}
	t.mu.Unlock()
}

func (t *detectorStageTracker) StartPoolRefresh(requested uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.poolRefreshRequested = requested
	t.poolRefreshStartedAt = time.Now()
	t.mu.Unlock()
}

func (t *detectorStageTracker) PoolRefreshResult(success bool, elapsed time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.poolRefreshAttempted++
	if success {
		t.poolRefreshSucceeded++
	} else {
		t.poolRefreshFailed++
	}
	if elapsed > t.maxSinglePoolRefresh {
		t.maxSinglePoolRefresh = elapsed
	}
	t.mu.Unlock()
}

func (t *detectorStageTracker) FinishPoolRefresh(stats refreshStats) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.poolRefreshAttempted = maxUint64(t.poolRefreshAttempted, stats.PoolsAttempted)
	t.poolRefreshSucceeded = maxUint64(t.poolRefreshSucceeded, stats.PoolsSucceeded)
	t.poolRefreshFailed = maxUint64(t.poolRefreshFailed, stats.PoolsFailed)
	if !t.poolRefreshStartedAt.IsZero() {
		t.poolRefreshDuration += time.Since(t.poolRefreshStartedAt)
		t.poolRefreshStartedAt = time.Time{}
	}
	t.mu.Unlock()
}

func (t *detectorStageTracker) SetDirtyPools(count uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.dirtyPoolsFound = count
	t.mu.Unlock()
}

func (t *detectorStageTracker) SetAffectedRoutes(count uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.affectedRoutesFound = count
	t.mu.Unlock()
}

func (t *detectorStageTracker) AddRoutesSelected(count int) {
	if t == nil || count <= 0 {
		return
	}
	t.mu.Lock()
	t.routesSelected += uint64(count)
	t.mu.Unlock()
}

func (t *detectorStageTracker) RouteEvaluationStarted() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.routesEvaluationStarted++
	t.quoteAttempts++
	t.mu.Unlock()
}

func (t *detectorStageTracker) RouteEvaluationFinished(success bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.routesEvaluated++
	if success {
		t.quoteSuccesses++
	} else {
		t.quoteFailures++
	}
	t.mu.Unlock()
}

func (t *detectorStageTracker) ObserveRPCWait(wait time.Duration) {
	if t == nil || wait <= 0 {
		return
	}
	t.mu.Lock()
	t.rpcLimiterWait += wait
	t.mu.Unlock()
}

func (t *detectorStageTracker) ObserveRPCTransport(elapsed time.Duration, completed bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.rpcCallsStarted++
	if completed {
		t.rpcCallsCompleted++
	}
	if elapsed > 0 {
		t.rpcTransport += elapsed
	}
	t.mu.Unlock()
}

func (t *detectorStageTracker) Snapshot() DetectorStageTelemetry {
	if t == nil {
		return DetectorStageTelemetry{}
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	getLogsDuration := t.getLogsDuration
	if !t.getLogsStartedAt.IsZero() {
		getLogsDuration += now.Sub(t.getLogsStartedAt)
	}
	poolRefreshDuration := t.poolRefreshDuration
	if !t.poolRefreshStartedAt.IsZero() {
		poolRefreshDuration += now.Sub(t.poolRefreshStartedAt)
	}
	return DetectorStageTelemetry{
		CurrentStage:               t.currentStage,
		StageStartedAt:             t.stageStartedAt,
		CycleStartedAt:             t.cycleStartedAt,
		SourceBlock:                t.sourceBlock,
		LatestBlockAtStageStart:    t.latestBlockAtStageStart,
		CycleElapsed:               now.Sub(t.cycleStartedAt),
		ActiveStageElapsed:         now.Sub(t.stageStartedAt),
		LastCompletedStage:         t.lastCompletedStage,
		LastCompletedStageDuration: t.lastCompletedStageDuration,
		LastStateBlockBeforeCycle:  t.lastStateBlockBeforeCycle,
		LastStateBlockAtCancel:     t.lastStateBlockAtCancel,
		ReconciliationPending:      t.reconciliationPending,
		ReconciliationUnitsDone:    t.reconciliationUnitsDone,
		ReconciliationUnitsTotal:   t.reconciliationUnitsTotal,
		GetLogsFromBlock:           t.getLogsFromBlock,
		GetLogsToBlock:             t.getLogsToBlock,
		GetLogsBlocksToScan:        t.getLogsBlocksToScan,
		GetLogsChunksPlanned:       t.getLogsChunksPlanned,
		GetLogsChunksAttempted:     t.getLogsChunksAttempted,
		GetLogsChunksSucceeded:     t.getLogsChunksSucceeded,
		GetLogsChunksFailed:        t.getLogsChunksFailed,
		GetLogsBlocksScanned:       t.getLogsBlocksScanned,
		GetLogsDuration:            getLogsDuration,
		PoolRefreshRequested:       t.poolRefreshRequested,
		PoolRefreshAttempted:       t.poolRefreshAttempted,
		PoolRefreshSucceeded:       t.poolRefreshSucceeded,
		PoolRefreshFailed:          t.poolRefreshFailed,
		PoolRefreshDuration:        poolRefreshDuration,
		MaxSinglePoolRefresh:       t.maxSinglePoolRefresh,
		DirtyPoolsFound:            t.dirtyPoolsFound,
		AffectedRoutesFound:        t.affectedRoutesFound,
		RoutesSelected:             t.routesSelected,
		RoutesEvaluationStarted:    t.routesEvaluationStarted,
		RoutesEvaluated:            t.routesEvaluated,
		QuoteAttempts:              t.quoteAttempts,
		QuoteSuccesses:             t.quoteSuccesses,
		QuoteFailures:              t.quoteFailures,
		RPCCallsStarted:            t.rpcCallsStarted,
		RPCCallsCompleted:          t.rpcCallsCompleted,
		RPCLimiterWait:             t.rpcLimiterWait,
		RPCTransport:               t.rpcTransport,
		RateLimiterWait:            t.rpcLimiterWait,
	}
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
