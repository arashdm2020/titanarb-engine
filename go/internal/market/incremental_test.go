package market

import (
	"math/big"
	"testing"

	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
)

func TestRoutesAffectedByDirtyPool(t *testing.T) {
	p1 := pools.Pool{Address: "0x0000000000000000000000000000000000000001"}
	p2 := pools.Pool{Address: "0x0000000000000000000000000000000000000002"}
	all := []routes.Route{
		{Symbols: []string{"A", "B", "A"}, Hops: []pools.Pool{p1, p1}},
		{Symbols: []string{"C", "D", "C"}, Hops: []pools.Pool{p2, p2}},
	}
	affected := routesAffectedBy(all, map[string]struct{}{p1.Address: {}}, false)
	if len(affected) != 1 || affected[0].Symbols[0] != "A" {
		t.Fatalf("dirty routing mismatch: %#v", affected)
	}
	if got := routesAffectedBy(all, nil, true); len(got) != 2 {
		t.Fatalf("full reconciliation returned %d routes", len(got))
	}
}

func TestPoolChangedUsesMutableState(t *testing.T) {
	before := pools.Pool{Liquidity: big.NewInt(10), SqrtPriceX96: big.NewInt(20)}
	if poolChanged(before, before) {
		t.Fatal("identical state marked dirty")
	}
	after := before
	after.SqrtPriceX96 = big.NewInt(21)
	if !poolChanged(before, after) {
		t.Fatal("price change not marked dirty")
	}
}

func TestRefreshRoutesDropsInactivePool(t *testing.T) {
	p := pools.Pool{Address: "0x0000000000000000000000000000000000000001", Liquidity: big.NewInt(10)}
	route := routes.Route{Symbols: []string{"A", "B", "A"}, Hops: []pools.Pool{p, p}}
	if got := refreshRoutes([]routes.Route{route}, nil); len(got) != 0 {
		t.Fatalf("route using inactive pool remained executable: %#v", got)
	}
	if got := refreshRoutes([]routes.Route{route}, []pools.Pool{p}); len(got) != 1 {
		t.Fatalf("active route was removed: %#v", got)
	}
}

func TestCapLoanMaxUsesAaveLiquidityAsUpperBound(t *testing.T) {
	configured := big.NewInt(1_000)
	if got := capLoanMax(configured, big.NewInt(250)); got.Cmp(big.NewInt(250)) != 0 {
		t.Fatalf("available liquidity did not cap max: %s", got)
	}
	if got := capLoanMax(configured, big.NewInt(2_000)); got.Cmp(configured) != 0 {
		t.Fatalf("larger liquidity expanded max: %s", got)
	}
	if got := capLoanMax(configured, nil); got.Cmp(configured) != 0 {
		t.Fatalf("nil liquidity changed max: %s", got)
	}
}

func TestBoundedRoutesCapsHotPathWork(t *testing.T) {
	var candidates []routes.Route
	for i := 0; i < maxEvaluationRoutesPerAsset+5; i++ {
		candidates = append(candidates, routes.Route{Symbols: []string{"A", "B", "A"}})
	}
	if got := boundedRoutes(candidates, maxEvaluationRoutesPerAsset); len(got) != maxEvaluationRoutesPerAsset {
		t.Fatalf("evaluation candidates were not capped: %d", len(got))
	}
	if got := boundedRoutes(candidates, maxOptimizerRoutesPerAsset); len(got) != maxOptimizerRoutesPerAsset {
		t.Fatalf("optimizer candidates were not capped: %d", len(got))
	}
}

func TestSelectOptimizerCandidatesPrefersBestNearMisses(t *testing.T) {
	evaluated := []evaluatedRoute{
		{route: routes.Route{Symbols: []string{"ARB", "USDC", "ARB"}}, score: big.NewInt(-50)},
		{route: routes.Route{Symbols: []string{"WETH", "ARB", "WETH"}}, score: big.NewInt(10)},
		{route: routes.Route{Symbols: []string{"USDC", "WETH", "USDC"}}, score: big.NewInt(10)},
		{route: routes.Route{Symbols: []string{"USDT", "ARB", "USDT"}}, score: big.NewInt(-5)},
	}

	selected := selectOptimizerCandidates(evaluated, 2)
	if len(selected) != 2 {
		t.Fatalf("selected %d optimizer candidates", len(selected))
	}

	got := []string{selected[0].String(), selected[1].String()}
	want := []string{"USDC -> WETH -> USDC", "WETH -> ARB -> WETH"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate %d mismatch: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestSearchOptionsNormalizeToSafeDefaults(t *testing.T) {
	got := (SearchOptions{}).Normalized()
	defaults := DefaultSearchOptions()
	if got != defaults {
		t.Fatalf("zero search options did not normalize to defaults: got %#v want %#v", got, defaults)
	}
}
