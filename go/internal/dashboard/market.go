// Package dashboard formats bounded, human-readable operator messages. It has
// no market, signer, or transport authority.
package dashboard

import (
	"fmt"
	"strings"

	"github.com/titanarb/titanarb-go/internal/metrics"
)

// MarketSnapshot deliberately separates operator-facing presentation from the
// detailed JSONL records. Fields are emitted in a fixed order to make Telegram
// summaries scannable rather than raw map dumps.
type MarketSnapshot struct {
	Status, RiskProfile, WSS string
	ActivePools, Cycles      uint64
	BestCycle, BestNet       string
	ClosestNearMiss          string
	Metrics                  metrics.Snapshot
}

func FormatMarket(s MarketSnapshot) string {
	status := nonEmpty(s.Status, "ONLINE")
	risk := nonEmpty(s.RiskProfile, "BALANCED")
	wss := nonEmpty(s.WSS, "UNKNOWN")
	bestCycle := nonEmpty(s.BestCycle, "No candidate passed the preliminary threshold")
	bestNet := nonEmpty(s.BestNet, "n/a")
	nearMiss := nonEmpty(s.ClosestNearMiss, "n/a")
	lines := []string{
		"📊 TITANARB — MARKET",
		fmt.Sprintf("🟢 Status: %s", status),
		"🌐 Network: Arbitrum One",
		fmt.Sprintf("🧠 Risk: %s", risk),
		"",
		"📈 Market",
		fmt.Sprintf("💧 Active pools: %d", s.ActivePools),
		fmt.Sprintf("🧭 Cycles available: %d", s.Cycles),
		fmt.Sprintf("🔎 Routes evaluated: %d", s.Metrics.RoutesEvaluated),
		fmt.Sprintf("💱 Quotes: %d", s.Metrics.Quotes),
		"",
		"⚡ Engine latency",
		fmt.Sprintf("⏱ Cycle: %d ms (p95 %d ms)", s.Metrics.CycleLatency.DurationMS, s.Metrics.P95CycleMS),
		fmt.Sprintf("🚦 Start delay: %d ms", s.Metrics.CycleLatency.BlockToStartMS),
		fmt.Sprintf("💱 Quote: %d ms", s.Metrics.CycleLatency.QuoteDurationMS),
		fmt.Sprintf("🧮 Optimizer: %d ms", s.Metrics.CycleLatency.OptimizerMS),
		fmt.Sprintf("📦 Block lag: %d (max %d)", s.Metrics.CycleLatency.LagBlocks, s.Metrics.MaxLagBlocks),
		fmt.Sprintf("♻️ Routes refreshed: %d", s.Metrics.CycleLatency.RoutesRecomputed),
		fmt.Sprintf("💾 Routes reused: %d", s.Metrics.CycleLatency.RoutesReused),
		fmt.Sprintf("🔀 Blocks coalesced: %d", s.Metrics.BlocksCoalesced),
		fmt.Sprintf("⚙️ Throughput: %.1f routes/s · %.1f quotes/s", s.Metrics.RoutesPerSecond, s.Metrics.QuotesPerSecond),
		"",
		"🏆 Best observed cycle",
		fmt.Sprintf("🔁 %s", bestCycle),
		fmt.Sprintf("💵 Expected net: %s", bestNet),
		fmt.Sprintf("📉 Closest near-miss: %s", nearMiss),
		"",
		fmt.Sprintf("🚀 Trades: %d", s.Metrics.TransactionsBroadcast),
		fmt.Sprintf("🔌 WSS: %s", wss),
	}
	return strings.Join(lines, "\n")
}

func nonEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
