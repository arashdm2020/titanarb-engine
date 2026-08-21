package graph

import (
	"math/big"
	"testing"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/pools"
)

func TestCyclesAreAssetAgnostic(t *testing.T) {
	tokens := map[string]config.Token{
		"A": {Address: "0x000000000000000000000000000000000000000a"},
		"B": {Address: "0x000000000000000000000000000000000000000b"},
		"C": {Address: "0x000000000000000000000000000000000000000c"},
	}
	g := New(tokens)
	for i, endpoints := range [][2]string{{tokens["A"].Address, tokens["B"].Address}, {tokens["B"].Address, tokens["C"].Address}, {tokens["C"].Address, tokens["A"].Address}} {
		g.UpsertPool(pools.Pool{Address: "0x000000000000000000000000000000000000000" + string(rune('1'+i)), Token0: endpoints[0], Token1: endpoints[1], DEX: pools.UniswapV3, Liquidity: big.NewInt(1)}, 1)
	}
	cycles := g.Cycles(4, 10)
	starts := map[string]bool{}
	for _, cycle := range cycles {
		starts[cycle.Assets[0]] = true
	}
	for _, asset := range []string{"A", "B", "C"} {
		if !starts[asset] {
			t.Fatalf("asset %s was not treated as an equal cycle root: %#v", asset, starts)
		}
	}
}
