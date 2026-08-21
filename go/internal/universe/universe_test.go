package universe

import (
	"math/big"
	"testing"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/pools"
)

func TestRegistryHasNoBaseAssetAndAcceptsKnownPool(t *testing.T) {
	r := New(map[string]config.Token{
		"B": {Symbol: "B", Address: "0x000000000000000000000000000000000000000b"},
		"A": {Symbol: "A", Address: "0x000000000000000000000000000000000000000a"},
	})
	if got := r.TokenNames(); len(got) != 2 || got[0] != "A" || got[1] != "B" {
		t.Fatalf("unexpected token registry: %v", got)
	}
	known := pools.Pool{Address: "0x0000000000000000000000000000000000000001", Token0: "0x000000000000000000000000000000000000000a", Token1: "0x000000000000000000000000000000000000000b", Liquidity: big.NewInt(1)}
	if !r.AddPool(known) || len(r.Pools()) != 1 {
		t.Fatal("registered pool should be accepted")
	}
	unknown := known
	unknown.Address = "0x0000000000000000000000000000000000000002"
	unknown.Token1 = "0x000000000000000000000000000000000000000c"
	if r.AddPool(unknown) {
		t.Fatal("unregistered token must not enter the execution-safe universe")
	}
}
