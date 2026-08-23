// Package metrics supplies concurrency-safe foundation runtime counters.
package metrics

import (
	"encoding/json"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	started                                                                time.Time
	blocksReceived, rpcErrors, wssDisconnects, wssReconnects               atomic.Uint64
	poolsDiscovered, quotes, quoteFailures, routesEvaluated, opportunities atomic.Uint64
	rpcCalls, cacheHits, cacheMisses                                       atomic.Uint64
	uniswapPools, camelotPools, uniswapQuotes, camelotQuotes               atomic.Uint64
	simulationAttempts, simulationFailures, postGasRejections              atomic.Uint64
	transactionsBroadcast, transactionsSucceeded, transactionsFailed       atomic.Uint64
	blocksCoalesced                                                        atomic.Uint64
	maxLagBlocks                                                           atomic.Uint64
	latencyMu                                                              sync.Mutex
	cycleSamples, quoteSamples                                             []uint64
	lastCycle                                                              CycleSample
	rpcRateMu                                                              sync.Mutex
	rpcRateSecond                                                          int64
	rpcRateCount                                                           uint64
	rpcRateSamples                                                         []uint64
}

type CycleSample struct {
	BlockToStartMS   uint64 `json:"block_to_cycle_start_ms"`
	DurationMS       uint64 `json:"market_cycle_duration_ms"`
	QuoteDurationMS  uint64 `json:"quote_duration_ms"`
	OptimizerMS      uint64 `json:"optimizer_duration_ms"`
	OptimizerRuns    uint64 `json:"optimizer_runs"`
	OptimizerSamples uint64 `json:"optimizer_samples"`
	LagBlocks        uint64 `json:"cycle_lag_blocks"`
	DirtyPools       uint64 `json:"dirty_pools"`
	RoutesRecomputed uint64 `json:"routes_recomputed"`
	RoutesReused     uint64 `json:"routes_reused"`
	RoutesEvaluated  uint64 `json:"routes_evaluated_cycle"`
	RPCCalls         uint64 `json:"rpc_calls_cycle"`
}
type Snapshot struct {
	BlocksReceived        uint64      `json:"blocks_received"`
	RPCErrors             uint64      `json:"rpc_errors"`
	WSSDisconnects        uint64      `json:"wss_disconnects"`
	WSSReconnects         uint64      `json:"wss_reconnects"`
	UptimeSeconds         uint64      `json:"uptime_seconds"`
	PoolsDiscovered       uint64      `json:"pools_discovered"`
	Quotes                uint64      `json:"quotes"`
	QuoteFailures         uint64      `json:"quote_failures"`
	RoutesEvaluated       uint64      `json:"routes_evaluated"`
	Opportunities         uint64      `json:"opportunities"`
	RPCCalls              uint64      `json:"rpc_calls"`
	CacheHits             uint64      `json:"cache_hits"`
	CacheMisses           uint64      `json:"cache_misses"`
	UniswapPools          uint64      `json:"uniswap_pools"`
	CamelotPools          uint64      `json:"camelot_pools"`
	UniswapQuotes         uint64      `json:"uniswap_quotes"`
	CamelotQuotes         uint64      `json:"camelot_quotes"`
	SimulationAttempts    uint64      `json:"simulation_attempts"`
	SimulationFailures    uint64      `json:"simulation_failures"`
	PostGasRejections     uint64      `json:"post_gas_rejections"`
	TransactionsBroadcast uint64      `json:"transactions_broadcast"`
	TransactionsSucceeded uint64      `json:"transactions_succeeded"`
	TransactionsFailed    uint64      `json:"transactions_failed"`
	BlocksCoalesced       uint64      `json:"blocks_coalesced"`
	CycleLatency          CycleSample `json:"cycle_latency"`
	MedianCycleMS         uint64      `json:"median_cycle_duration_ms"`
	P95CycleMS            uint64      `json:"p95_cycle_duration_ms"`
	MaxCycleMS            uint64      `json:"max_cycle_duration_ms"`
	MedianQuoteMS         uint64      `json:"median_quote_duration_ms"`
	P95QuoteMS            uint64      `json:"p95_quote_duration_ms"`
	MaxLagBlocks          uint64      `json:"max_cycle_lag_blocks"`
	RoutesPerSecond       float64     `json:"routes_per_second"`
	QuotesPerSecond       float64     `json:"quotes_per_second"`
	AverageRPS            float64     `json:"average_rpc_requests_per_second"`
	P95RPS                float64     `json:"p95_rpc_requests_per_second"`
}

func New() *Metrics                    { return &Metrics{started: time.Now()} }
func (m *Metrics) IncBlocks()          { m.blocksReceived.Add(1) }
func (m *Metrics) IncRPCErrors()       { m.rpcErrors.Add(1) }
func (m *Metrics) IncWSSDisconnects()  { m.wssDisconnects.Add(1) }
func (m *Metrics) IncWSSReconnects()   { m.wssReconnects.Add(1) }
func (m *Metrics) IncPoolsDiscovered() { m.poolsDiscovered.Add(1) }
func (m *Metrics) IncQuotes()          { m.quotes.Add(1) }
func (m *Metrics) IncQuoteFailures()   { m.quoteFailures.Add(1) }
func (m *Metrics) IncRoutesEvaluated() { m.routesEvaluated.Add(1) }
func (m *Metrics) IncOpportunities()   { m.opportunities.Add(1) }
func (m *Metrics) IncRPCCalls() {
	m.rpcCalls.Add(1)
	now := time.Now().Unix()
	m.rpcRateMu.Lock()
	if m.rpcRateSecond == 0 {
		m.rpcRateSecond = now
	}
	if now != m.rpcRateSecond {
		m.rpcRateSamples = appendBounded(m.rpcRateSamples, m.rpcRateCount, 2048)
		m.rpcRateSecond = now
		m.rpcRateCount = 0
	}
	m.rpcRateCount++
	m.rpcRateMu.Unlock()
}
func (m *Metrics) IncCacheHits()                   { m.cacheHits.Add(1) }
func (m *Metrics) IncCacheMisses()                 { m.cacheMisses.Add(1) }
func (m *Metrics) IncUniswapPools()                { m.uniswapPools.Add(1) }
func (m *Metrics) IncCamelotPools()                { m.camelotPools.Add(1) }
func (m *Metrics) IncUniswapQuotes()               { m.uniswapQuotes.Add(1) }
func (m *Metrics) IncCamelotQuotes()               { m.camelotQuotes.Add(1) }
func (m *Metrics) IncSimulationAttempts()          { m.simulationAttempts.Add(1) }
func (m *Metrics) IncSimulationFailures()          { m.simulationFailures.Add(1) }
func (m *Metrics) IncPostGasRejections()           { m.postGasRejections.Add(1) }
func (m *Metrics) IncTransactionsBroadcast()       { m.transactionsBroadcast.Add(1) }
func (m *Metrics) IncTransactionsSucceeded()       { m.transactionsSucceeded.Add(1) }
func (m *Metrics) IncTransactionsFailed()          { m.transactionsFailed.Add(1) }
func (m *Metrics) AddBlocksCoalesced(count uint64) { m.blocksCoalesced.Add(count) }

func (m *Metrics) ObserveMarketCycle(sample CycleSample) {
	for {
		current := m.maxLagBlocks.Load()
		if sample.LagBlocks <= current || m.maxLagBlocks.CompareAndSwap(current, sample.LagBlocks) {
			break
		}
	}
	m.latencyMu.Lock()
	defer m.latencyMu.Unlock()
	m.lastCycle = sample
	m.cycleSamples = appendBounded(m.cycleSamples, sample.DurationMS, 2048)
	m.quoteSamples = appendBounded(m.quoteSamples, sample.QuoteDurationMS, 2048)
}
func (m *Metrics) Snapshot() Snapshot {
	m.latencyMu.Lock()
	last := m.lastCycle
	cycles := append([]uint64(nil), m.cycleSamples...)
	quotes := append([]uint64(nil), m.quoteSamples...)
	m.latencyMu.Unlock()
	m.rpcRateMu.Lock()
	rps := append([]uint64(nil), m.rpcRateSamples...)
	m.rpcRateMu.Unlock()
	uptime := uint64(time.Since(m.started).Seconds())
	if uptime == 0 {
		uptime = 1
	}
	return Snapshot{
		BlocksReceived: m.blocksReceived.Load(), RPCErrors: m.rpcErrors.Load(),
		WSSDisconnects: m.wssDisconnects.Load(), WSSReconnects: m.wssReconnects.Load(),
		UptimeSeconds: uptime, PoolsDiscovered: m.poolsDiscovered.Load(),
		Quotes: m.quotes.Load(), QuoteFailures: m.quoteFailures.Load(),
		RoutesEvaluated: m.routesEvaluated.Load(), Opportunities: m.opportunities.Load(),
		RPCCalls: m.rpcCalls.Load(), CacheHits: m.cacheHits.Load(), CacheMisses: m.cacheMisses.Load(),
		UniswapPools: m.uniswapPools.Load(), CamelotPools: m.camelotPools.Load(),
		UniswapQuotes: m.uniswapQuotes.Load(), CamelotQuotes: m.camelotQuotes.Load(),
		SimulationAttempts: m.simulationAttempts.Load(), SimulationFailures: m.simulationFailures.Load(), PostGasRejections: m.postGasRejections.Load(),
		TransactionsBroadcast: m.transactionsBroadcast.Load(), TransactionsSucceeded: m.transactionsSucceeded.Load(), TransactionsFailed: m.transactionsFailed.Load(),
		BlocksCoalesced: m.blocksCoalesced.Load(), CycleLatency: last,
		MedianCycleMS: percentile(cycles, 50), P95CycleMS: percentile(cycles, 95), MaxCycleMS: percentile(cycles, 100),
		MedianQuoteMS: percentile(quotes, 50), P95QuoteMS: percentile(quotes, 95),
		MaxLagBlocks: m.maxLagBlocks.Load(), RoutesPerSecond: float64(m.routesEvaluated.Load()) / float64(uptime),
		QuotesPerSecond: float64(m.quotes.Load()) / float64(uptime),
		AverageRPS:      float64(m.rpcCalls.Load()) / float64(uptime), P95RPS: float64(percentile(rps, 95)),
	}
}
func (m *Metrics) WriteJSON(w io.Writer) error { return json.NewEncoder(w).Encode(m.Snapshot()) }

func appendBounded(values []uint64, value uint64, maximum int) []uint64 {
	if len(values) >= maximum {
		copy(values, values[len(values)-maximum+1:])
		values = values[:maximum-1]
	}
	return append(values, value)
}

func percentile(values []uint64, percent int) uint64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if percent >= 100 {
		return values[len(values)-1]
	}
	index := (len(values)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1]
}
