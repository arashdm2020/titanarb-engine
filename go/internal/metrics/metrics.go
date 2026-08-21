// Package metrics supplies concurrency-safe foundation runtime counters.
package metrics

import (
	"encoding/json"
	"io"
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
}
type Snapshot struct {
	BlocksReceived        uint64 `json:"blocks_received"`
	RPCErrors             uint64 `json:"rpc_errors"`
	WSSDisconnects        uint64 `json:"wss_disconnects"`
	WSSReconnects         uint64 `json:"wss_reconnects"`
	UptimeSeconds         uint64 `json:"uptime_seconds"`
	PoolsDiscovered       uint64 `json:"pools_discovered"`
	Quotes                uint64 `json:"quotes"`
	QuoteFailures         uint64 `json:"quote_failures"`
	RoutesEvaluated       uint64 `json:"routes_evaluated"`
	Opportunities         uint64 `json:"opportunities"`
	RPCCalls              uint64 `json:"rpc_calls"`
	CacheHits             uint64 `json:"cache_hits"`
	CacheMisses           uint64 `json:"cache_misses"`
	UniswapPools          uint64 `json:"uniswap_pools"`
	CamelotPools          uint64 `json:"camelot_pools"`
	UniswapQuotes         uint64 `json:"uniswap_quotes"`
	CamelotQuotes         uint64 `json:"camelot_quotes"`
	SimulationAttempts    uint64 `json:"simulation_attempts"`
	SimulationFailures    uint64 `json:"simulation_failures"`
	PostGasRejections     uint64 `json:"post_gas_rejections"`
	TransactionsBroadcast uint64 `json:"transactions_broadcast"`
	TransactionsSucceeded uint64 `json:"transactions_succeeded"`
	TransactionsFailed    uint64 `json:"transactions_failed"`
}

func New() *Metrics                          { return &Metrics{started: time.Now()} }
func (m *Metrics) IncBlocks()                { m.blocksReceived.Add(1) }
func (m *Metrics) IncRPCErrors()             { m.rpcErrors.Add(1) }
func (m *Metrics) IncWSSDisconnects()        { m.wssDisconnects.Add(1) }
func (m *Metrics) IncWSSReconnects()         { m.wssReconnects.Add(1) }
func (m *Metrics) IncPoolsDiscovered()       { m.poolsDiscovered.Add(1) }
func (m *Metrics) IncQuotes()                { m.quotes.Add(1) }
func (m *Metrics) IncQuoteFailures()         { m.quoteFailures.Add(1) }
func (m *Metrics) IncRoutesEvaluated()       { m.routesEvaluated.Add(1) }
func (m *Metrics) IncOpportunities()         { m.opportunities.Add(1) }
func (m *Metrics) IncRPCCalls()              { m.rpcCalls.Add(1) }
func (m *Metrics) IncCacheHits()             { m.cacheHits.Add(1) }
func (m *Metrics) IncCacheMisses()           { m.cacheMisses.Add(1) }
func (m *Metrics) IncUniswapPools()          { m.uniswapPools.Add(1) }
func (m *Metrics) IncCamelotPools()          { m.camelotPools.Add(1) }
func (m *Metrics) IncUniswapQuotes()         { m.uniswapQuotes.Add(1) }
func (m *Metrics) IncCamelotQuotes()         { m.camelotQuotes.Add(1) }
func (m *Metrics) IncSimulationAttempts()    { m.simulationAttempts.Add(1) }
func (m *Metrics) IncSimulationFailures()    { m.simulationFailures.Add(1) }
func (m *Metrics) IncPostGasRejections()     { m.postGasRejections.Add(1) }
func (m *Metrics) IncTransactionsBroadcast() { m.transactionsBroadcast.Add(1) }
func (m *Metrics) IncTransactionsSucceeded() { m.transactionsSucceeded.Add(1) }
func (m *Metrics) IncTransactionsFailed()    { m.transactionsFailed.Add(1) }
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		BlocksReceived: m.blocksReceived.Load(), RPCErrors: m.rpcErrors.Load(),
		WSSDisconnects: m.wssDisconnects.Load(), WSSReconnects: m.wssReconnects.Load(),
		UptimeSeconds: uint64(time.Since(m.started).Seconds()), PoolsDiscovered: m.poolsDiscovered.Load(),
		Quotes: m.quotes.Load(), QuoteFailures: m.quoteFailures.Load(),
		RoutesEvaluated: m.routesEvaluated.Load(), Opportunities: m.opportunities.Load(),
		RPCCalls: m.rpcCalls.Load(), CacheHits: m.cacheHits.Load(), CacheMisses: m.cacheMisses.Load(),
		UniswapPools: m.uniswapPools.Load(), CamelotPools: m.camelotPools.Load(),
		UniswapQuotes: m.uniswapQuotes.Load(), CamelotQuotes: m.camelotQuotes.Load(),
		SimulationAttempts: m.simulationAttempts.Load(), SimulationFailures: m.simulationFailures.Load(), PostGasRejections: m.postGasRejections.Load(),
		TransactionsBroadcast: m.transactionsBroadcast.Load(), TransactionsSucceeded: m.transactionsSucceeded.Load(), TransactionsFailed: m.transactionsFailed.Load(),
	}
}
func (m *Metrics) WriteJSON(w io.Writer) error { return json.NewEncoder(w).Encode(m.Snapshot()) }
