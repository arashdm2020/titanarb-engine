package cache

import (
	"context"
	"math/big"
	"sync"
	"testing"

	"github.com/titanarb/titanarb-go/internal/pools"
)

func TestCacheRefreshAndBlockInvalidation(t *testing.T) {
	c := NewPoolCache()
	p := pools.Pool{Address: "pool", Liquidity: big.NewInt(1), LastUpdatedBlock: 10}
	if err := c.Refresh(context.Background(), func(context.Context) ([]pools.Pool, error) { return []pools.Pool{p}, nil }); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Get("pool", 10); !ok {
		t.Fatal("expected fresh pool")
	}
	if _, ok := c.Get("pool", 11); ok {
		t.Fatal("expected stale pool")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Snapshot(); c.Get("pool", 1) }()
	}
	wg.Wait()
}

func TestReplaceRemovesSupersededPools(t *testing.T) {
	c := NewPoolCache()
	c.Put(pools.Pool{Address: "old", LastUpdatedBlock: 1})
	c.Replace([]pools.Pool{{Address: "new", LastUpdatedBlock: 2}})
	if _, ok := c.Get("old", 0); ok {
		t.Fatal("superseded pool survived full reconciliation")
	}
	if _, ok := c.Get("new", 2); !ok {
		t.Fatal("new reconciled pool missing")
	}
}
