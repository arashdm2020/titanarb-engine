package nearmiss

import (
	"math/big"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
)

func TestRecordCapturesEconomicsAndGap(t *testing.T) {
	route := routes.Route{
		Symbols: []string{"USDC", "WETH", "USDC"},
		Hops: []pools.Pool{
			{DEX: pools.UniswapV3, Liquidity: big.NewInt(10)},
			{DEX: pools.CamelotV3, Liquidity: big.NewInt(10)},
		},
	}
	op := &opportunity.Opportunity{
		Route:          route,
		AmountIn:       big.NewInt(1_000_000),
		AmountOut:      big.NewInt(1_000_500),
		GrossProfit:    big.NewInt(500),
		AavePremium:    big.NewInt(50),
		GasEstimate:    big.NewInt(100),
		L1DataFee:      big.NewInt(25),
		ExpectedProfit: big.NewInt(325),
		MinProfit:      big.NewInt(1_000),
		Timestamp:      time.Unix(1, 0),
	}
	got := FromOpportunity(op, "profitability threshold not met", 50*time.Millisecond, 2)
	if got.Route != "USDC -> WETH -> USDC" || got.DEXPath != "uniswap_v3 -> camelot_v3" || !got.CrossVenue {
		t.Fatalf("route metadata mismatch: %+v", got)
	}
	if got.GapToProfit.Cmp(big.NewInt(675)) != 0 {
		t.Fatalf("gap mismatch: %s", got.GapToProfit)
	}
	if got.Fields()["amount_out"] != "1000500" || got.Fields()["l1_fee"] != "25" {
		t.Fatalf("economics fields missing: %+v", got.Fields())
	}
}

func TestScorePrioritizesCrossVenueSmallGap(t *testing.T) {
	base := Record{HopCount: 2, QuoteSuccessful: true, SufficientLiquidity: true, GapToProfit: big.NewInt(1_000), NetProfit: big.NewInt(-1)}
	cross := base
	cross.CrossVenue = true
	same := base
	same.CrossVenue = false
	if Score(cross) <= Score(same) {
		t.Fatalf("cross venue was not prioritized: cross=%d same=%d", Score(cross), Score(same))
	}
	far := cross
	far.GapToProfit = big.NewInt(1_000_000_000_000)
	if Score(cross) <= Score(far) {
		t.Fatalf("smaller gap was not prioritized: near=%d far=%d", Score(cross), Score(far))
	}
}

func TestSnapshotSortsClosestNearMisses(t *testing.T) {
	records := []Record{
		{Route: "far", GapToProfit: big.NewInt(100), Score: 10},
		{Route: "near", GapToProfit: big.NewInt(1), Score: 1},
		{Route: "near-high-score", GapToProfit: big.NewInt(1), Score: 99},
	}
	got := Snapshot(records, 2)
	if len(got) != 2 || got[0]["route"] != "near-high-score" || got[1]["route"] != "near" {
		t.Fatalf("unexpected snapshot ordering: %+v", got)
	}
}

func TestRouteMemoryUsesNeutralPriorForUnseenRoutes(t *testing.T) {
	memory := NewRouteMemory(16)
	same := memory.PreQuoteScore(PreQuoteInput{Key: "same", CrossVenue: false})
	cross := memory.PreQuoteScore(PreQuoteInput{Key: "cross", CrossVenue: true})
	if same <= 0 {
		t.Fatalf("unseen same-DEX route started unusably low: %d", same)
	}
	if cross <= same {
		t.Fatalf("cross-venue soft prior missing: cross=%d same=%d", cross, same)
	}
}

func TestRouteMemoryNearMissImprovesPriorityAndLosingRouteDecays(t *testing.T) {
	memory := NewRouteMemory(16)
	good := Record{CrossVenue: true, QuoteSuccessful: true, SufficientLiquidity: true, HopCount: 2, GapToProfit: big.NewInt(500), NetProfit: big.NewInt(-1), Score: 5_000}
	bad := Record{CrossVenue: true, QuoteSuccessful: true, SufficientLiquidity: true, HopCount: 2, GapToProfit: big.NewInt(1_000_000_000_000), NetProfit: big.NewInt(-1), FailureCount: 10, Score: 1_000, RejectionReason: "profitability threshold not met"}
	memory.Observe("good", good, 100)
	memory.Observe("bad", bad, 100)
	if memory.PreQuoteScore(PreQuoteInput{Key: "good", CrossVenue: true, CurrentBlock: 101}) <= memory.PreQuoteScore(PreQuoteInput{Key: "bad", CrossVenue: true, CurrentBlock: 101}) {
		t.Fatalf("historical near-miss did not improve priority")
	}
	before := memory.PreQuoteScore(PreQuoteInput{Key: "bad", CrossVenue: true, CurrentBlock: 101})
	memory.MarkDirty("bad", 102)
	after := memory.PreQuoteScore(PreQuoteInput{Key: "bad", CrossVenue: true, CurrentBlock: 102})
	if after <= before {
		t.Fatalf("dirty update did not revive down-ranked route: before=%d after=%d", before, after)
	}
}

func TestRouteMemoryPrunesOldestEntries(t *testing.T) {
	memory := NewRouteMemory(2)
	record := Record{QuoteSuccessful: true, SufficientLiquidity: true, Score: 1_000}
	memory.Observe("old", record, 1)
	memory.Observe("mid", record, 2)
	memory.Observe("new", record, 3)
	if memory.Len() != 2 {
		t.Fatalf("memory was not bounded: %d", memory.Len())
	}
	if _, ok := memory.Stats("old"); ok {
		t.Fatalf("oldest route was not pruned")
	}
}
