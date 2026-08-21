package routes

import (
	"github.com/titanarb/titanarb-go/internal/pools"
	"math/big"
	"testing"
)

func pool() pools.Pool {
	return pools.Pool{Address: "p", Token0: "a", Token1: "b", Liquidity: big.NewInt(1), DEX: pools.UniswapV3, Fee: 500}
}
func TestBoundedRoutes(t *testing.T) {
	pairs := map[Pair][]pools.Pool{}
	for _, a := range []string{"USDC", "WETH", "ARB", "USDT"} {
		for _, b := range []string{"USDC", "WETH", "ARB", "USDT"} {
			if a != b {
				pairs[Pair{a, b}] = []pools.Pool{pool()}
			}
		}
	}
	routes := Build("USDC", []string{"WETH", "ARB", "USDT"}, pairs, 200)
	counts := map[int]int{}
	for _, route := range routes {
		counts[len(route.Hops)]++
		if len(route.Hops) < 2 || len(route.Hops) > 4 || route.Symbols[0] != "USDC" || route.Symbols[len(route.Symbols)-1] != "USDC" {
			t.Fatalf("invalid route %+v", route)
		}
	}
	if counts[2] == 0 || counts[3] == 0 || counts[4] == 0 {
		t.Fatalf("missing bounded routes: %+v", counts)
	}
}
