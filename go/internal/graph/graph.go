// Package graph turns discovered pools into an asset-agnostic liquidity graph.
package graph

import (
	"sort"
	"strings"
	"sync"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/pools"
)

type Edge struct {
	From, To    string
	Pool        pools.Pool
	Reliability float64 // [0,1], supplied by the risk/operations layer when known.
}

type Route struct {
	Assets []string
	Hops   []pools.Pool
}

func (r Route) String() string { return strings.Join(r.Assets, " -> ") }

// Graph contains directed views of real two-sided pools. No asset is designated
// as a base; callers choose every eligible loan asset independently.
type Graph struct {
	mu        sync.RWMutex
	byAddress map[string]string
	edges     map[string][]Edge
}

func New(tokens map[string]config.Token) *Graph {
	g := &Graph{byAddress: make(map[string]string), edges: make(map[string][]Edge)}
	for name, token := range tokens {
		g.byAddress[strings.ToLower(token.Address)] = name
		g.edges[name] = nil
	}
	return g
}

// UpsertPool adds both directions of a supported real pool. It returns false for
// a pool whose endpoints are not in the configured execution-safe universe.
func (g *Graph) UpsertPool(pool pools.Pool, reliability float64) bool {
	if pool.Liquidity == nil || pool.Liquidity.Sign() <= 0 {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	a, aOK := g.byAddress[strings.ToLower(pool.Token0)]
	b, bOK := g.byAddress[strings.ToLower(pool.Token1)]
	if !aOK || !bOK || a == b {
		return false
	}
	g.removePoolLocked(strings.ToLower(pool.Address))
	g.edges[a] = append(g.edges[a], Edge{From: a, To: b, Pool: pool, Reliability: clamp(reliability)})
	g.edges[b] = append(g.edges[b], Edge{From: b, To: a, Pool: pool, Reliability: clamp(reliability)})
	g.sortLocked(a)
	g.sortLocked(b)
	return true
}

func (g *Graph) removePoolLocked(address string) {
	for asset, edges := range g.edges {
		kept := edges[:0]
		for _, edge := range edges {
			if !strings.EqualFold(edge.Pool.Address, address) {
				kept = append(kept, edge)
			}
		}
		g.edges[asset] = kept
	}
}

func (g *Graph) sortLocked(asset string) {
	sort.Slice(g.edges[asset], func(i, j int) bool {
		a, b := g.edges[asset][i], g.edges[asset][j]
		if a.To != b.To {
			return a.To < b.To
		}
		return strings.ToLower(a.Pool.Address) < strings.ToLower(b.Pool.Address)
	})
}

// CyclesFrom enumerates simple cycles for one possible loan asset. Calling it for
// every asset is deliberately the normal asset-agnostic operating mode.
func (g *Graph) CyclesFrom(asset string, maxHops, limit int) []Route {
	if maxHops < 2 || limit < 1 {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if _, ok := g.edges[asset]; !ok {
		return nil
	}
	var result []Route
	visitedAssets := map[string]bool{asset: true}
	visitedPools := make(map[string]bool)
	var walk func(current string, assets []string, hops []pools.Pool)
	walk = func(current string, assets []string, hops []pools.Pool) {
		if len(result) >= limit || len(hops) >= maxHops {
			return
		}
		for _, edge := range g.edges[current] {
			poolKey := strings.ToLower(edge.Pool.Address)
			if visitedPools[poolKey] {
				continue
			}
			if edge.To == asset {
				if len(hops)+1 >= 2 {
					result = append(result, Route{Assets: append(append([]string(nil), assets...), asset), Hops: append(append([]pools.Pool(nil), hops...), edge.Pool)})
				}
				continue
			}
			if visitedAssets[edge.To] {
				continue
			}
			visitedAssets[edge.To], visitedPools[poolKey] = true, true
			walk(edge.To, append(assets, edge.To), append(hops, edge.Pool))
			delete(visitedAssets, edge.To)
			delete(visitedPools, poolKey)
		}
	}
	walk(asset, []string{asset}, nil)
	return result
}

// Cycles evaluates every configured asset as a possible start and end asset. The
// returned routes are intentionally not rotation-deduplicated: each loan asset is
// economically distinct even if it traverses the same pools in a different order.
func (g *Graph) Cycles(maxHops, perAssetLimit int) []Route {
	g.mu.RLock()
	assets := make([]string, 0, len(g.edges))
	for asset := range g.edges {
		assets = append(assets, asset)
	}
	g.mu.RUnlock()
	sort.Strings(assets)
	var result []Route
	for _, asset := range assets {
		result = append(result, g.CyclesFrom(asset, maxHops, perAssetLimit)...)
	}
	return result
}

func clamp(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
