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
