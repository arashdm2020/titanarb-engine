// Package volatility supplies token-neutral, read-only search-priority
// signals. It never rejects an asset because it is volatile and it has no
// execution or signing authority.
package volatility

import (
	"math/big"
	"sort"
	"strings"
	"sync"

	"github.com/titanarb/titanarb-go/internal/pools"
)

// Signal captures short-window pool movement and same-pair cross-venue price
// dispersion, both expressed in bps. sqrtPriceX96 is used only as a relative
// state marker; final execution continues to rely on authoritative quotes.
type Signal struct {
	MovementBPS   uint64
	DispersionBPS uint64
}

func (s Signal) Score(weight float64) float64 {
	if weight < 0 {
		weight = 0
	}
	return float64(s.MovementBPS+s.DispersionBPS) * weight
}

// Tracker compares each pool with its previous observed state. It is bounded
// by the supplied active pool set and safe for concurrent read-only cycles.
type Tracker struct {
	mu       sync.Mutex
	previous map[string]*big.Int
}

func NewTracker() *Tracker { return &Tracker{previous: make(map[string]*big.Int)} }

func (t *Tracker) Observe(active []pools.Pool) map[string]Signal {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make(map[string]Signal, len(active))
	byPair := make(map[string][]pools.Pool)
	for _, pool := range active {
		if pool.SqrtPriceX96 == nil || pool.SqrtPriceX96.Sign() <= 0 {
			continue
		}
		key := strings.ToLower(pool.Address)
		result[key] = Signal{MovementBPS: relativeBPS(t.previous[key], pool.SqrtPriceX96)}
		t.previous[key] = new(big.Int).Set(pool.SqrtPriceX96)
		byPair[pairKey(pool.Token0, pool.Token1)] = append(byPair[pairKey(pool.Token0, pool.Token1)], pool)
	}
	for _, pair := range byPair {
		if len(pair) < 2 {
			continue
		}
		min, max := pair[0].SqrtPriceX96, pair[0].SqrtPriceX96
		for _, pool := range pair[1:] {
			if pool.SqrtPriceX96.Cmp(min) < 0 {
				min = pool.SqrtPriceX96
			}
			if pool.SqrtPriceX96.Cmp(max) > 0 {
				max = pool.SqrtPriceX96
			}
		}
		dispersion := relativeBPS(min, max)
		for _, pool := range pair {
			key := strings.ToLower(pool.Address)
			signal := result[key]
			signal.DispersionBPS = dispersion
			result[key] = signal
		}
	}
	return result
}

// RankPools is deterministic. Higher movement/dispersion receives more
// read-only refresh attention; token symbol/name is never a tie-breaker.
func RankPools(active []pools.Pool, signals map[string]Signal, weight float64) []pools.Pool {
	result := append([]pools.Pool(nil), active...)
	sort.SliceStable(result, func(i, j int) bool {
		left := signals[strings.ToLower(result[i].Address)].Score(weight)
		right := signals[strings.ToLower(result[j].Address)].Score(weight)
		if left != right {
			return left > right
		}
		return strings.ToLower(result[i].Address) < strings.ToLower(result[j].Address)
	})
	return result
}

func relativeBPS(a, b *big.Int) uint64 {
	if a == nil || b == nil || a.Sign() <= 0 || b.Sign() <= 0 {
		return 0
	}
	delta := new(big.Int).Sub(a, b)
	if delta.Sign() < 0 {
		delta.Neg(delta)
	}
	denominator := a
	if b.Cmp(a) > 0 {
		denominator = b
	}
	delta.Mul(delta, big.NewInt(10_000))
	delta.Div(delta, denominator)
	return delta.Uint64()
}

func pairKey(a, b string) string {
	values := []string{strings.ToLower(a), strings.ToLower(b)}
	sort.Strings(values)
	return values[0] + ":" + values[1]
}
