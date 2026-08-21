package volatility

import (
	"math/big"
	"testing"

	"github.com/titanarb/titanarb-go/internal/pools"
)

func TestCrossVenueDispersionIsAssetNameNeutral(t *testing.T) {
	tracker := NewTracker()
	active := []pools.Pool{
		{Address: "0x2", Token0: "token-z", Token1: "token-y", SqrtPriceX96: big.NewInt(100)},
		{Address: "0x1", Token0: "token-y", Token1: "token-z", SqrtPriceX96: big.NewInt(120)},
	}
	signals := tracker.Observe(active)
	if signals["0x1"].DispersionBPS == 0 || signals["0x2"].DispersionBPS == 0 {
		t.Fatalf("expected cross-venue dispersion: %#v", signals)
	}
	ranked := RankPools(active, signals, 1)
	if ranked[0].Address != "0x1" { // stable address tie-break only
		t.Fatalf("unexpected deterministic tie-break: %#v", ranked)
	}
}

func TestMovementPrioritizesChangedPoolWithoutTokenPreference(t *testing.T) {
	tracker := NewTracker()
	baseline := []pools.Pool{
		{Address: "0xa", Token0: "A", Token1: "B", SqrtPriceX96: big.NewInt(100)},
		{Address: "0xb", Token0: "C", Token1: "D", SqrtPriceX96: big.NewInt(100)},
	}
	tracker.Observe(baseline)
	changed := []pools.Pool{
		{Address: "0xa", Token0: "A", Token1: "B", SqrtPriceX96: big.NewInt(101)},
		{Address: "0xb", Token0: "C", Token1: "D", SqrtPriceX96: big.NewInt(130)},
	}
	ranked := RankPools(changed, tracker.Observe(changed), 1.5)
	if ranked[0].Address != "0xb" {
		t.Fatalf("larger movement did not receive priority: %#v", ranked)
	}
}
