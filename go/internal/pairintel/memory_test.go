package pairintel

import (
	"math"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/pools"
)

const (
	tokenA = "0x0000000000000000000000000000000000000001"
	tokenB = "0x0000000000000000000000000000000000000002"
	poolA  = "0x0000000000000000000000000000000000000011"
	poolB  = "0x0000000000000000000000000000000000000012"
)

func testMemory(t *testing.T) (*Memory, *time.Time) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.MinObservation = time.Minute
	cfg.MinScore = 0.01
	cfg.MinConfidence = 0.01
	m := NewMemory(cfg)
	now := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }
	if err := m.RegisterToken(TokenMeta{Address: tokenA, Decimals: 6, HasCode: true, Core: true}); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterToken(TokenMeta{Address: tokenB, Decimals: 6, HasCode: true}); err != nil {
		t.Fatal(err)
	}
	return m, &now
}

func TestCanonicalPairAndDecimalAdjustedPrice(t *testing.T) {
	k, err := CanonicalPair(tokenB, tokenA)
	if err != nil || k.Token0 != tokenA {
		t.Fatalf("canonical=%+v err=%v", k, err)
	}
	q96 := new(big.Int).Lsh(big.NewInt(1), 96)
	if got := canonicalPrice(q96, 18, 6); math.Abs(got-1e12) > 1 {
		t.Fatalf("price=%g", got)
	}
	if got := canonicalPrice(q96, 6, 18); math.Abs(got-1e-12) > 1e-18 {
		t.Fatalf("inverse decimals=%g", got)
	}
}

func TestRollingWindowsVolumeScoreConfidenceAndVenueDedupe(t *testing.T) {
	m, now := testMemory(t)
	q96 := new(big.Int).Lsh(big.NewInt(1), 96)
	m.ObservePool(pools.Pool{Address: poolA, Token0: tokenA, Token1: tokenB, DEX: pools.UniswapV3, SqrtPriceX96: q96, LastUpdatedBlock: 1})
	m.ObservePool(pools.Pool{Address: poolB, Token0: tokenA, Token1: tokenB, DEX: pools.UniswapV3, SqrtPriceX96: q96, LastUpdatedBlock: 1})
	p := m.Pairs()[0]
	if p.Components.VenueDiversity != .25 {
		t.Fatalf("fee tiers counted as venues: %v", p.Components.VenueDiversity)
	}
	*now = now.Add(time.Minute)
	m.ObservePool(pools.Pool{Address: poolB, Token0: tokenA, Token1: tokenB, DEX: pools.CamelotV3, SqrtPriceX96: scaledQ96(1.01), LastUpdatedBlock: 2})
	for i := 0; i < 25; i++ {
		m.ObserveSwap(pools.Swap{Pool: poolA, Amount0: big.NewInt(1_000_000), Amount1: big.NewInt(-1_000_000), Block: uint64(i + 2)})
	}
	if got := m.Pairs()[0].Components.Volume1h; got <= 0 {
		t.Fatalf("1h volume not recorded: %v", got)
	}
	m.RecordDepth(poolA, Depth{Direction: "0to1", Bucket: "small", AmountIn: "1000000", AmountOut: "990000", AmountInNormalized: 1, PriceImpactBPS: 100, Score: .8, Successful: true, ObservedAt: *now})
	*now = now.Add(61 * time.Minute)
	m.ObservePool(pools.Pool{Address: poolA, Token0: tokenA, Token1: tokenB, DEX: pools.UniswapV3, SqrtPriceX96: scaledQ96(1.02), LastUpdatedBlock: 3})
	p = m.Pairs()[0]
	if p.Components.VenueDiversity != 1 || p.SwapCount != 25 || p.Components.Volume24h <= 0 || p.Components.RV1h <= 0 || p.Score <= 0 || p.Confidence <= 0 {
		t.Fatalf("incomplete pair metrics: %+v", p)
	}
	if got := m.SelectShadow(); len(got) != 1 || got[0].State != Shadow {
		t.Fatalf("shadow=%+v", got)
	}
}

func TestTelemetryDeduplicatesDEXVenuesAcrossFeeTiers(t *testing.T) {
	m := NewMemory(DefaultConfig())
	a := TokenMeta{Address: "0x0000000000000000000000000000000000000001", Decimals: 18, HasCode: true}
	b := TokenMeta{Address: "0x0000000000000000000000000000000000000002", Decimals: 6, HasCode: true}
	if err := m.RegisterToken(a); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterToken(b); err != nil {
		t.Fatal(err)
	}
	q := new(big.Int).Lsh(big.NewInt(1), 96)
	m.ObservePool(pools.Pool{Address: "0x0000000000000000000000000000000000000011", Token0: a.Address, Token1: b.Address, DEX: pools.UniswapV3, Fee: 500, SqrtPriceX96: q})
	m.ObservePool(pools.Pool{Address: "0x0000000000000000000000000000000000000012", Token0: a.Address, Token1: b.Address, DEX: pools.UniswapV3, Fee: 3000, SqrtPriceX96: q})
	m.ObservePool(pools.Pool{Address: "0x0000000000000000000000000000000000000013", Token0: a.Address, Token1: b.Address, DEX: pools.CamelotV3, SqrtPriceX96: q})
	rows := m.Telemetry(1)
	if len(rows) != 1 {
		t.Fatalf("telemetry rows=%d", len(rows))
	}
	venues, ok := rows[0]["venues"].([]string)
	if !ok || len(venues) != 2 || venues[0] != "camelot_v3" || venues[1] != "uniswap_v3" {
		t.Fatalf("venues must be independent DEXes: %#v", rows[0]["venues"])
	}
}

func TestDepthFailureCooldown(t *testing.T) {
	m, _ := testMemory(t)
	q := new(big.Int).Lsh(big.NewInt(1), 96)
	m.ObservePool(pools.Pool{Address: poolA, Token0: tokenA, Token1: tokenB, DEX: pools.UniswapV3, SqrtPriceX96: q})
	for i := 0; i < 10; i++ {
		m.RecordDepth(poolA, Depth{Direction: "0to1", Bucket: "small", Successful: false})
	}
	p := m.Pairs()[0]
	if p.State != Cooled || p.Components.FailurePenalty < .8 || p.Components.IlliquidityPenalty < .8 {
		t.Fatalf("not cooled: %+v", p)
	}
}

func TestLiveAdmissionEjectionAndPersistence(t *testing.T) {
	m, now := testMemory(t)
	m.cfg.Mode = "live"
	m.cfg.MinDepthScore = .1
	m.cfg.MinQuoteSuccess = .8
	m.cfg.MaxLivePairs = 2
	m.cfg.MaxLiveDynamicAssets = 2
	q := new(big.Int).Lsh(big.NewInt(1), 96)
	m.ObservePool(pools.Pool{Address: poolA, Token0: tokenA, Token1: tokenB, DEX: pools.UniswapV3, SqrtPriceX96: q})
	m.ObservePool(pools.Pool{Address: poolB, Token0: tokenA, Token1: tokenB, DEX: pools.CamelotV3, SqrtPriceX96: q})
	for _, pool := range []string{poolA, poolB} {
		for _, dir := range []string{"0to1", "1to0"} {
			m.RecordDepth(pool, Depth{Direction: dir, Bucket: "small", Successful: true, Score: .9})
		}
	}
	*now = now.Add(61 * time.Minute)
	m.ObservePool(pools.Pool{Address: poolA, Token0: tokenA, Token1: tokenB, DEX: pools.UniswapV3, SqrtPriceX96: scaledQ96(1.01)})
	var live AdmissionSnapshot
	for i := 0; i < 3; i++ {
		live = m.SelectLive()
	}
	if len(live.Pairs) != 1 || live.Pairs[0].State != Admitted {
		t.Fatalf("live=%+v", live)
	}
	path := filepath.Join(t.TempDir(), "live.json")
	if err := m.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path, m.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Pairs()[0].State != Admitted {
		t.Fatal("admission did not survive restart")
	}
	m.cfg.MinScore = 101
	for window := 1; window < m.cfg.EjectWindows; window++ {
		if got := m.SelectLive(); len(got.Pairs) != 1 || m.Pairs()[0].State != Admitted {
			t.Fatalf("pair ejected before consecutive-window limit at %d: %+v", window, got)
		}
	}
	if got := m.SelectLive(); len(got.Pairs) != 0 || m.Pairs()[0].State != Cooled {
		t.Fatalf("ejection=%+v pair=%+v", got, m.Pairs()[0])
	}
}

func TestDynamicSymbolIsDeterministicAddressIdentity(t *testing.T) {
	a := "0xabcdef1200000000000000000000000000000000"
	want := "DYN_ABCDEF1200000000000000000000000000000000"
	if DynamicSymbol(a) != want || DynamicSymbol(strings.ToUpper(a)) != want {
		t.Fatal("unstable dynamic symbol")
	}
}

func TestPersistenceAndCheckpoint(t *testing.T) {
	m, _ := testMemory(t)
	q := new(big.Int).Lsh(big.NewInt(1), 96)
	m.ObservePool(pools.Pool{Address: poolA, Token0: tokenA, Token1: tokenB, DEX: pools.UniswapV3, SqrtPriceX96: q})
	m.SetCheckpoint("uniswap_v3", 123)
	path := filepath.Join(t.TempDir(), "pair.json")
	if err := m.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path, m.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Checkpoint("uniswap_v3") != 123 || len(loaded.Pairs()) != 1 {
		t.Fatalf("restart state lost: %+v", loaded.Pairs())
	}
}

func TestRejectMalformedNoCodeAndInvalidDecimals(t *testing.T) {
	m := NewMemory(DefaultConfig())
	cases := []TokenMeta{{Address: "bad", Decimals: 6, HasCode: true}, {Address: tokenA, Decimals: 6, HasCode: false}, {Address: tokenA, Decimals: 37, HasCode: true}}
	for _, c := range cases {
		if m.RegisterToken(c) == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
}

func scaledQ96(price float64) *big.Int {
	f := new(big.Float).SetPrec(256).SetFloat64(math.Sqrt(price))
	f.Mul(f, new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), 96)))
	out, _ := f.Int(nil)
	return out
}
