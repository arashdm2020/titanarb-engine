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
	var b bytes.Buffer
	if err := m.WriteJSON(&b); err != nil {
		t.Fatal(err)
	}
	if b.Len() == 0 {
		t.Fatal("missing JSON")
	}
}
