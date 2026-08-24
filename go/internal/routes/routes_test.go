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

func TestBuildAllHasNoStartAssetAnchorOrOrderingBias(t *testing.T) {
	assets := []string{"WETH", "ARB", "USDC"}
	pairs := map[Pair][]pools.Pool{}
	for _, a := range assets {
		for _, b := range assets {
			if a != b {
				pairs[Pair{a, b}] = []pools.Pool{pool()}
			}
		}
	}
	routes := BuildAll([]string{"USDC", "WETH", "ARB"}, pairs, 200)
	starts := map[string]int{}
	for _, route := range routes {
		starts[route.Symbols[0]]++
		if route.Symbols[0] != route.Symbols[len(route.Symbols)-1] {
			t.Fatalf("cycle is not asset-continuous: %s", route.String())
		}
	}
	for _, asset := range assets {
		if starts[asset] == 0 {
			t.Fatalf("asset %s was structurally excluded: %+v", asset, starts)
		}
	}
	other := BuildAll([]string{"ARB", "USDC", "WETH"}, pairs, 200)
	if len(routes) != len(other) {
		t.Fatalf("token ordering changed route count: %d != %d", len(routes), len(other))
	}
	for i := range routes {
		if routes[i].String() != other[i].String() {
			t.Fatalf("token ordering changed route ordering: %q != %q", routes[i], other[i])
		}
	}
}

func TestBuildAllSharesBoundedCapacityAcrossStartAssets(t *testing.T) {
	assets := []string{"A", "B", "C", "D"}
	pairs := make(map[Pair][]pools.Pool)
	for _, from := range assets {
		for _, to := range assets {
			if from == to {
				continue
			}
			// Multiple real-pool choices make every start able to exhaust a
			// naive global cap. Addresses only need to be deterministic here.
			pairs[Pair{From: from, To: to}] = []pools.Pool{{Address: from + to + "1"}, {Address: from + to + "2"}}
		}
	}
	routes := BuildAll(assets, pairs, 8)
	counts := make(map[string]int)
	for _, route := range routes {
		counts[route.Symbols[0]]++
	}
	if len(routes) != 8 {
		t.Fatalf("route limit not respected: %d", len(routes))
	}
	for _, asset := range assets {
		if counts[asset] == 0 {
			t.Fatalf("global cap structurally excluded %s: %v", asset, counts)
		}
	}
}

func TestBuildForStartsUsesDynamicAssetsOnlyAsIntermediates(t *testing.T) {
	assets := []string{"USDC", "WETH", "USDC_E"}
	pairs := make(map[Pair][]pools.Pool)
	for _, from := range assets {
		for _, to := range assets {
			if from == to {
				continue
			}
			pairs[Pair{From: from, To: to}] = []pools.Pool{{Address: from + to, DEX: pools.UniswapV3, Liquidity: big.NewInt(1)}}
		}
	}

	got := BuildForStarts([]string{"USDC", "WETH"}, assets, pairs, 100)
	if len(got) == 0 {
		t.Fatal("expected routes through dynamic market asset")
	}
	usesDynamic := false
	for _, route := range got {
		if route.Symbols[0] == "USDC_E" {
			t.Fatalf("dynamic market-only asset became a loan/start asset: %#v", route.Symbols)
		}
		for _, symbol := range route.Symbols[1 : len(route.Symbols)-1] {
			if symbol == "USDC_E" {
				usesDynamic = true
			}
		}
	}
	if !usesDynamic {
		t.Fatalf("dynamic asset was not used as an intermediate: %#v", got)
	}
}

func TestBuildForStartsBalancesHopDepthWhenTwoHopCanExhaustBudget(t *testing.T) {
	assets := []string{"USDC", "WETH", "ARB", "USDT", "USDC_E"}
	pairs := make(map[Pair][]pools.Pool)
	for _, from := range assets {
		for _, to := range assets {
			if from == to {
				continue
			}
			pairs[Pair{From: from, To: to}] = []pools.Pool{
				{Address: from + to + "1", DEX: pools.UniswapV3, Liquidity: big.NewInt(1)},
				{Address: from + to + "2", DEX: pools.CamelotV3, Liquidity: big.NewInt(1)},
				{Address: from + to + "3", DEX: pools.UniswapV3, Liquidity: big.NewInt(1)},
			}
		}
	}

	got := BuildForStarts([]string{"USDC"}, assets, pairs, 60)
	counts := make(map[int]int)
	for _, route := range got {
		counts[len(route.Hops)]++
	}
	if counts[2] == 60 || counts[3] == 0 || counts[4] == 0 {
		t.Fatalf("hop-depth budget was monopolized by short cycles: %+v", counts)
	}
}

func TestTwoHopCrossVenueReservationPreservesCapAndLongerHops(t *testing.T) {
	assets := []string{"A", "B", "C", "D"}
	by := map[Pair][]pools.Pool{}
	for _, a := range assets {
		for _, b := range assets {
			if a == b {
				continue
			}
			by[Pair{a, b}] = []pools.Pool{{Address: a + b + "u", DEX: pools.UniswapV3}, {Address: a + b + "c", DEX: pools.CamelotV3}}
		}
	}
	got := BuildForStartsWithCrossVenueShare([]string{"A"}, assets, by, 60, 5000)
	if len(got) == 0 || len(got) > 60 {
		t.Fatalf("route budget violated: %d", len(got))
	}
	cross, two, three, four := 0, 0, 0, 0
	for _, r := range got {
		switch len(r.Hops) {
		case 2:
			two++
			if r.Hops[0].DEX != r.Hops[1].DEX {
				cross++
			}
		case 3:
			three++
		case 4:
			four++
		}
	}
	if cross < two/2 || three == 0 || four == 0 {
		t.Fatalf("2hop=%d cross=%d 3=%d 4=%d", two, cross, three, four)
	}
}
