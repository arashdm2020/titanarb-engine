// Package cache provides block-aware, concurrency-safe market-state caches.
package cache

import (
	"context"
	"sync"

	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/pools"
)

type PoolCache struct {
	mu      sync.RWMutex
	pools   map[string]pools.Pool
	metrics *metrics.Metrics
}

func NewPoolCache(metric ...*metrics.Metrics) *PoolCache {
	var m *metrics.Metrics
	if len(metric) > 0 {
		m = metric[0]
	}
	return &PoolCache{pools: make(map[string]pools.Pool), metrics: m}
}

func (c *PoolCache) Get(address string, minimumBlock uint64) (pools.Pool, bool) {
	c.mu.RLock()
	p, ok := c.pools[address]
	c.mu.RUnlock()
	fresh := ok && p.LastUpdatedBlock >= minimumBlock
	if c.metrics != nil {
		if fresh {
			c.metrics.IncCacheHits()
		} else {
			c.metrics.IncCacheMisses()
		}
	}
	return p, fresh
}

func (c *PoolCache) Put(pool pools.Pool) {
	c.mu.Lock()
	current, exists := c.pools[pool.Address]
	if !exists || pool.LastUpdatedBlock >= current.LastUpdatedBlock {
		c.pools[pool.Address] = pool
	}
	c.mu.Unlock()
}

func (c *PoolCache) Snapshot() []pools.Pool {
	c.mu.RLock()
	result := make([]pools.Pool, 0, len(c.pools))
	for _, pool := range c.pools {
		result = append(result, pool)
	}
	c.mu.RUnlock()
	return result
}

// Refresh obtains a replacement outside the lock, so readers never block on RPC.
func (c *PoolCache) Refresh(ctx context.Context, load func(context.Context) ([]pools.Pool, error)) error {
	updated, err := load(ctx)
	if err != nil {
		return err
	}
	for _, pool := range updated {
		c.Put(pool)
	}
	return nil
}
