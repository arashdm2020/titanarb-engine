// Package risk ranks read-only opportunities. It never loosens on-chain or
// pipeline safeguards; it only raises the confidence/profit hurdle for riskier work.
package risk

import "math"

type Inputs struct {
	LiquidityRatio float64 // 0..1 relative to desired executable depth
	VolatilityBPS  uint64
	GasUSD         float64
	RPCLatencyMS   uint64
	QuoteAgeMS     uint64
	Hops           int
	SuccessRate    float64 // 0..1 historical, unknown uses 0.5
}
type Assessment struct {
	Score                       uint8
	Confidence                  string
	RequiredProfitMultiplierBPS uint64
}

func Assess(in Inputs) Assessment {
	liquidity := clamp(in.LiquidityRatio)
	success := in.SuccessRate
	if success < 0 {
		success = .5
	}
	success = clamp(success)
	penalty := (1-liquidity)*30 + (1-success)*25 + math.Min(float64(in.VolatilityBPS)/100, 15) + math.Min(in.GasUSD/10, 10) + math.Min(float64(in.RPCLatencyMS)/100, 10) + math.Min(float64(in.QuoteAgeMS)/250, 10)
	if in.Hops > 2 {
		penalty += float64(in.Hops-2) * 3
	}
	score := uint8(math.Max(0, math.Min(100, math.Round(100-penalty))))
	confidence := "low"
	if score >= 80 {
		confidence = "high"
	} else if score >= 55 {
		confidence = "medium"
	}
	// This is additive tightening, never a lower minProfit requirement.
	return Assessment{Score: score, Confidence: confidence, RequiredProfitMultiplierBPS: 10_000 + uint64(100-score)*50}
}
func clamp(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
