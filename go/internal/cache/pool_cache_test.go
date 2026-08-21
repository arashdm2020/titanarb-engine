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
