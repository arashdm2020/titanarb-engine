package risk

import "testing"

func TestRiskOnlyTightensRequiredProfit(t *testing.T) {
	high := Assess(Inputs{LiquidityRatio: 1, SuccessRate: 1, Hops: 2})
	low := Assess(Inputs{LiquidityRatio: .1, SuccessRate: .2, VolatilityBPS: 900, GasUSD: 80, RPCLatencyMS: 1000, QuoteAgeMS: 2000, Hops: 4})
	if high.RequiredProfitMultiplierBPS < 10_000 || low.RequiredProfitMultiplierBPS <= high.RequiredProfitMultiplierBPS || low.Score >= high.Score {
		t.Fatalf("risk must only tighten: high=%+v low=%+v", high, low)
	}
}
