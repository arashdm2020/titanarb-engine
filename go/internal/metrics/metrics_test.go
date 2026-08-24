package metrics

import (
	"bytes"
	"testing"
)

func TestCountersAndJSON(t *testing.T) {
	m := New()
	m.IncBlocks()
	m.IncRPCErrors()
	m.IncWSSDisconnects()
	m.IncWSSReconnects()
	m.IncRPCCalls()
	m.IncCacheHits()
	m.IncCacheMisses()
	m.IncUniswapPools()
	m.IncCamelotPools()
	m.IncUniswapQuotes()
	m.IncCamelotQuotes()
	m.IncCandidateLag1Admitted()
	m.IncCandidateLag2Admitted()
	m.IncCandidateStaleRejected()
	m.AddBlocksCoalesced(3)
	m.rpcRateMu.Lock()
	m.rpcRateSamples = []uint64{8, 10}
	m.rpcRateMu.Unlock()
	m.ObserveMarketCycle(CycleSample{DurationMS: 200, QuoteDurationMS: 150, LagBlocks: 2, RoutesRecomputed: 10, RoutesReused: 90, RPCCalls: 2})
	m.ObserveMarketCycle(CycleSample{DurationMS: 100, QuoteDurationMS: 50, LagBlocks: 0, RoutesRecomputed: 5, RoutesReused: 95, RPCCalls: 1})
	s := m.Snapshot()
	if s.BlocksReceived != 1 || s.RPCErrors != 1 || s.WSSDisconnects != 1 || s.WSSReconnects != 1 {
		t.Fatalf("unexpected snapshot: %+v", s)
	}
	if s.RPCCalls != 1 || s.CacheHits != 1 || s.CacheMisses != 1 {
		t.Fatalf("missing performance counters: %+v", s)
	}
	if s.UniswapPools != 1 || s.CamelotPools != 1 || s.UniswapQuotes != 1 || s.CamelotQuotes != 1 {
		t.Fatalf("missing DEX counters: %+v", s)
	}
	if s.CandidateLag1Admitted != 1 || s.CandidateLag2Admitted != 1 || s.CandidateStaleRejected != 1 {
		t.Fatalf("missing candidate freshness counters: %+v", s)
	}
	if s.BlocksCoalesced != 3 || s.MedianCycleMS != 100 || s.P95CycleMS != 200 || s.MaxCycleMS != 200 || s.CycleLatency.LagBlocks != 0 || s.MaxLagBlocks != 2 {
		t.Fatalf("missing latency/coalescing metrics: %+v", s)
	}
	if s.P95RPS != 10 {
		t.Fatalf("unexpected p95 RPC rate: %+v", s)
	}
	var b bytes.Buffer
	if err := m.WriteJSON(&b); err != nil {
		t.Fatal(err)
	}
	if b.Len() == 0 {
		t.Fatal("missing JSON")
	}
}
