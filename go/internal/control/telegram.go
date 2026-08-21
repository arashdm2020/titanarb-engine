// Package control validates Telegram operator commands. Transport polling is
// intentionally separate so command handling remains deterministic and easy
// to test; an unauthorized sender cannot mutate runtime state.
package control

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/titanarb/titanarb-go/internal/runtimeconfig"
	"github.com/titanarb/titanarb-go/internal/telegram"
)

type Request struct {
	ChatID, SenderID, Text string
}

type Authorizer struct {
	ChatID, AdminID string
}

func (a Authorizer) Allowed(r Request) bool {
	if strings.TrimSpace(a.ChatID) == "" || strings.TrimSpace(r.ChatID) != strings.TrimSpace(a.ChatID) {
		return false
	}
	return strings.TrimSpace(a.AdminID) == "" || strings.TrimSpace(r.SenderID) == strings.TrimSpace(a.AdminID)
}

type Handler struct {
	Auth   Authorizer
	Risk   *runtimeconfig.Manager
	Status func() string
	Market func() string
	Top    func() string
}

type UpdateSource interface {
	Updates(context.Context, int64, int) ([]telegram.Update, error)
	SendTo(string, string)
}

// Run polls commands in a dedicated fail-open goroutine. A Telegram outage
// only delays operator control; it never blocks market work or execution.
func Run(ctx context.Context, source UpdateSource, handler Handler) {
	if source == nil {
		return
	}
	var offset int64
	for {
		if ctx.Err() != nil {
			return
		}
		updates, err := source.Updates(ctx, offset, 25)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
				continue
			}
		}
		for _, update := range updates {
			if update.ID >= offset {
				offset = update.ID + 1
			}
			if response, handled := handler.Handle(Request{ChatID: update.ChatID, SenderID: update.SenderID, Text: update.Text}); handled && response != "" {
				source.SendTo(update.ChatID, response)
			}
		}
	}
}

func (h Handler) Handle(r Request) (string, bool) {
	if !h.Auth.Allowed(r) {
		return "", false
	}
	parts := strings.Fields(strings.TrimSpace(strings.ToLower(r.Text)))
	if len(parts) == 0 {
		return "", false
	}
	switch parts[0] {
	case "/status":
		return callback(h.Status, "🟢 TITANARB — STATUS"), true
	case "/market":
		return callback(h.Market, "📊 TITANARB — MARKET"), true
	case "/top":
		return callback(h.Top, "🏆 TITANARB — TOP CYCLES"), true
	case "/settings", "/risk":
		if h.Risk == nil {
			return "Risk control unavailable", true
		}
		if len(parts) == 1 {
			return formatSettings(h.Risk.Snapshot()), true
		}
		profile := runtimeconfig.Profile(strings.ToUpper(parts[1]))
		updated, err := h.Risk.SetProfile(profile)
		if err != nil {
			return "❌ " + err.Error(), true
		}
		return "✅ Risk profile applied\n" + formatSettings(updated), true
	case "/set":
		if h.Risk == nil || len(parts) != 3 {
			return "Usage: /set <setting> <value>", true
		}
		updated, err := h.Risk.Set(parts[1], parts[2])
		if err != nil {
			return "❌ " + err.Error(), true
		}
		return "✅ Runtime setting applied\n" + formatSettings(updated), true
	case "/help":
		return "Commands: /status, /market, /risk, /risk conservative|balanced|aggressive|custom, /settings, /top, /set <setting> <value>", true
	default:
		return "", false
	}
}

func callback(fn func() string, fallback string) string {
	if fn == nil {
		return fallback
	}
	value := fn()
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func formatSettings(s runtimeconfig.Settings) string {
	return fmt.Sprintf("🧠 Risk: %s\n🎯 Min profit: $%.2f\n📉 Max price impact: %d bps\n↔️ Slippage: %d bps\n💧 Liquidity utilization: %.0f%%\n🔎 Optimizer depth: %d\n🧭 Route depth: %d\n🌊 Volatility weight: %.2f", s.Profile, s.MinProfitUSD, s.MaxPriceImpactBPS, s.SlippageBPS, s.LiquidityUtilization*100, s.OptimizerDepth, s.RouteSearchDepth, s.VolatilityWeight)
}
