package dashboard

import (
	"strings"
	"testing"

	"github.com/titanarb/titanarb-go/internal/metrics"
)

func TestFormatMarketUsesFixedReadableOrder(t *testing.T) {
	text := FormatMarket(MarketSnapshot{Status: "ONLINE", RiskProfile: "AGGRESSIVE", WSS: "Healthy", ActivePools: 12, Cycles: 34, BestCycle: "ARB → WETH → ARB", BestNet: "+$4.20", ClosestNearMiss: "-$0.10", Metrics: metrics.Snapshot{RoutesEvaluated: 56, Quotes: 78, BlocksCoalesced: 3, P95CycleMS: 240, CycleLatency: metrics.CycleSample{DurationMS: 184, LagBlocks: 0, RoutesRecomputed: 214, RoutesReused: 1930}}})
	for _, want := range []string{"📊 TITANARB — MARKET", "🧠 Risk: AGGRESSIVE", "💧 Active pools: 12", "⚡ Engine latency", "⏱ Cycle: 184 ms (p95 240 ms)", "♻️ Routes refreshed: 214", "💾 Routes reused: 1930", "🔀 Blocks coalesced: 3", "🔁 ARB → WETH → ARB", "🚀 Trades: 0", "🔌 WSS: Healthy"} {
		if !strings.Contains(text, want) {
			t.Fatalf("dashboard missing %q:\n%s", want, text)
		}
	}
	if strings.Index(text, "📈 Market") > strings.Index(text, "🏆 Best observed cycle") {
		t.Fatalf("market sections are not fixed-order:\n%s", text)
	}
}
