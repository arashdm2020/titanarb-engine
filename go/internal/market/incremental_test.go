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
