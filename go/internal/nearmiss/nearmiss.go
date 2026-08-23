// Package nearmiss records rejected route economics and scores routes for the
// read-only market scheduler. It has no execution authority and does not alter
// profitability, simulation, slippage, stale-state, or allow-list gates.
package nearmiss

import (
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
)

type Record struct {
	Route               string
	DEXPath             string
	HopCount            int
	LoanAsset           string
	AmountIn            *big.Int
	AmountOut           *big.Int
	GrossProfit         *big.Int
	AavePremium         *big.Int
	L1Fee               *big.Int
	L2Fee               *big.Int
	PriceImpactBPS      uint64
	NetProfit           *big.Int
	MinProfit           *big.Int
	GapToProfit         *big.Int
	RejectionReason     string
	Score               int64
	EvaluationLatency   time.Duration
	QuoteSuccessful     bool
	SufficientLiquidity bool
	CrossVenue          bool
	FailureCount        uint64
	Timestamp           time.Time
}

type RouteStats struct {
	GapEMA          float64
	NetProfitEMA    float64
	QuoteSuccessEMA float64
	FailureEMA      float64
	LastScore       int64
	LastSeenBlock   uint64
	LastDirtyBlock  uint64
	CrossVenue      bool
	Observations    uint64
	LastLatencyMS   float64
}

type RouteMemory struct {
	mu        sync.RWMutex
	routes    map[string]RouteStats
	maxRoutes int
}

type PreQuoteInput struct {
	Key          string
	CrossVenue   bool
	Volatility   float64
	CurrentBlock uint64
}

func NewRouteMemory(maxRoutes int) *RouteMemory {
	if maxRoutes < 1 {
		maxRoutes = 4096
	}
	return &RouteMemory{routes: make(map[string]RouteStats), maxRoutes: maxRoutes}
}

func (m *RouteMemory) Observe(key string, record Record, block uint64) {
	key = strings.TrimSpace(key)
	if m == nil || key == "" {
		return
	}
	stats := RouteStats{
		GapEMA:          magnitude(record.GapToProfit),
		NetProfitEMA:    signedMagnitude(record.NetProfit),
		QuoteSuccessEMA: boolFloat(record.QuoteSuccessful),
		FailureEMA:      boolFloat(record.GapToProfit != nil && record.GapToProfit.Sign() > 0 || strings.TrimSpace(record.RejectionReason) != ""),
		LastScore:       record.Score,
		LastSeenBlock:   block,
		CrossVenue:      record.CrossVenue,
		Observations:    1,
		LastLatencyMS:   float64(record.EvaluationLatency.Milliseconds()),
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if previous, ok := m.routes[key]; ok {
		const alpha = 0.35
		stats.GapEMA = ema(previous.GapEMA, stats.GapEMA, alpha)
		stats.NetProfitEMA = ema(previous.NetProfitEMA, stats.NetProfitEMA, alpha)
		stats.QuoteSuccessEMA = ema(previous.QuoteSuccessEMA, stats.QuoteSuccessEMA, alpha)
		stats.FailureEMA = ema(previous.FailureEMA, stats.FailureEMA, alpha)
		stats.LastDirtyBlock = previous.LastDirtyBlock
		stats.Observations = previous.Observations + 1
		stats.LastLatencyMS = ema(previous.LastLatencyMS, stats.LastLatencyMS, alpha)
	}
	m.routes[key] = stats
	m.pruneLocked()
}

func (m *RouteMemory) MarkDirty(key string, block uint64) {
	key = strings.TrimSpace(key)
	if m == nil || key == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.routes[key]
	stats.LastDirtyBlock = block
	stats.FailureEMA *= 0.5
	m.routes[key] = stats
}

func (m *RouteMemory) Stats(key string) (RouteStats, bool) {
	if m == nil {
		return RouteStats{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	stats, ok := m.routes[strings.TrimSpace(key)]
	return stats, ok
}

func (m *RouteMemory) Len() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.routes)
}

func (m *RouteMemory) PreQuoteScore(input PreQuoteInput) int64 {
	score := int64(1_000)
	if input.CrossVenue {
		score += 500
	}
	if input.Volatility > 0 {
		if input.Volatility > 5 {
			input.Volatility = 5
		}
		score += int64(input.Volatility * 150)
	}
	stats, ok := m.Stats(input.Key)
	if !ok || stats.Observations == 0 {
		return score + 250
	}
	score += int64(float64(stats.LastScore-1_000) * 0.55)
	score += int64((stats.QuoteSuccessEMA - 0.5) * 900)
	score -= int64(stats.FailureEMA * 700)
	if stats.GapEMA <= 6 {
		score += 550
	} else if stats.GapEMA <= 9 {
		score += 250
	}
	if stats.LastLatencyMS > 0 {
		score -= int64(stats.LastLatencyMS / 50)
	}
	if input.CurrentBlock > stats.LastSeenBlock {
		age := input.CurrentBlock - stats.LastSeenBlock
		if age > 240 {
			score -= int64(minUint64(age-240, 600))
		}
	}
	return score
}

func (m *RouteMemory) pruneLocked() {
	if m.maxRoutes < 1 || len(m.routes) <= m.maxRoutes {
		return
	}
	type item struct {
		key   string
		block uint64
	}
	items := make([]item, 0, len(m.routes))
	for key, stats := range m.routes {
		items = append(items, item{key: key, block: stats.LastSeenBlock})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].block < items[j].block })
	for len(m.routes) > m.maxRoutes && len(items) > 0 {
		delete(m.routes, items[0].key)
		items = items[1:]
	}
}

func FromOpportunity(op *opportunity.Opportunity, reason string, latency time.Duration, failureCount uint64) Record {
	if op == nil {
		return Record{RejectionReason: reason, EvaluationLatency: latency, FailureCount: failureCount, Timestamp: time.Now().UTC()}
	}
	record := Record{
		Route:               op.Route.String(),
		DEXPath:             DEXPath(op.Route),
		HopCount:            len(op.Route.Hops),
		LoanAsset:           "",
		AmountIn:            clone(op.AmountIn),
		AmountOut:           clone(op.AmountOut),
		GrossProfit:         clone(op.GrossProfit),
		AavePremium:         clone(op.AavePremium),
		L1Fee:               clone(op.L1DataFee),
		L2Fee:               clone(op.GasEstimate),
		PriceImpactBPS:      PriceImpactBPS(op.AmountIn, op.AmountOut),
		NetProfit:           clone(op.ExpectedProfit),
		MinProfit:           clone(op.MinProfit),
		RejectionReason:     strings.TrimSpace(reason),
		EvaluationLatency:   latency,
		QuoteSuccessful:     op.AmountOut != nil && op.AmountOut.Sign() > 0,
		SufficientLiquidity: routeHasLiquidity(op.Route),
		CrossVenue:          CrossVenue(op.Route),
		FailureCount:        failureCount,
		Timestamp:           op.Timestamp,
	}
	if len(op.Route.Symbols) > 0 {
		record.LoanAsset = op.Route.Symbols[0]
	}
	record.GapToProfit = Gap(record.NetProfit, record.MinProfit)
	record.Score = Score(record)
	return record
}

func DEXPath(route routes.Route) string {
	parts := make([]string, 0, len(route.Hops))
	for _, hop := range route.Hops {
		parts = append(parts, string(hop.DEX))
	}
	return strings.Join(parts, " -> ")
}

func CrossVenue(route routes.Route) bool {
	seen := make(map[pools.DEX]struct{})
	for _, hop := range route.Hops {
		seen[hop.DEX] = struct{}{}
	}
	return len(seen) > 1
}

func PriceImpactBPS(amountIn, amountOut *big.Int) uint64 {
	if amountIn == nil || amountOut == nil || amountIn.Sign() <= 0 || amountOut.Cmp(amountIn) >= 0 {
		return 0
	}
	loss := new(big.Int).Sub(amountIn, amountOut)
	scaled := new(big.Int).Mul(loss, big.NewInt(10_000))
	scaled.Div(scaled, amountIn)
	if !scaled.IsUint64() {
		return math.MaxUint64
	}
	return scaled.Uint64()
}

func Gap(net, minProfit *big.Int) *big.Int {
	minimum := clone(minProfit)
	netValue := clone(net)
	if minimum == nil {
		minimum = big.NewInt(0)
	}
	if netValue == nil {
		netValue = big.NewInt(0)
	}
	gap := new(big.Int).Sub(minimum, netValue)
	if gap.Sign() < 0 {
		return big.NewInt(0)
	}
	return gap
}

func Score(r Record) int64 {
	score := int64(1_000)
	if r.CrossVenue {
		score += 2_500
	} else {
		score -= 600
	}
	if r.QuoteSuccessful {
		score += 1_000
	} else {
		score -= 2_000
	}
	if r.SufficientLiquidity {
		score += 750
	} else {
		score -= 1_500
	}
	score -= int64(r.HopCount) * 75
	score -= int64(r.PriceImpactBPS) / 2
	score -= int64(r.FailureCount) * 150
	score -= int64(r.EvaluationLatency.Milliseconds() / 25)
	score += gapBonus(r.GapToProfit)
	return score
}

func ema(previous, next, alpha float64) float64 {
	if alpha <= 0 {
		return previous
	}
	if alpha >= 1 {
		return next
	}
	return previous*(1-alpha) + next*alpha
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func magnitude(value *big.Int) float64 {
	if value == nil || value.Sign() == 0 {
		return 0
	}
	return float64(len(new(big.Int).Abs(value).String()))
}

func signedMagnitude(value *big.Int) float64 {
	if value == nil || value.Sign() == 0 {
		return 0
	}
	sign := 1.0
	if value.Sign() < 0 {
		sign = -1
	}
	return sign * magnitude(value)
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func Snapshot(records []Record, limit int) []map[string]any {
	if limit < 1 || len(records) == 0 {
		return nil
	}
	copyRecords := append([]Record(nil), records...)
	sort.SliceStable(copyRecords, func(i, j int) bool {
		left, right := copyRecords[i], copyRecords[j]
		if compareBig(left.GapToProfit, right.GapToProfit) != 0 {
			return compareBig(left.GapToProfit, right.GapToProfit) < 0
		}
		if left.Score != right.Score {
			return left.Score > right.Score
		}
		return left.Route < right.Route
	})
	if len(copyRecords) > limit {
		copyRecords = copyRecords[:limit]
	}
	out := make([]map[string]any, 0, len(copyRecords))
	for _, r := range copyRecords {
		out = append(out, r.Fields())
	}
	return out
}

func Distribution(records []Record) map[string]int {
	out := map[string]int{"high": 0, "medium": 0, "low": 0}
	for _, r := range records {
		switch {
		case r.Score >= 4_000:
			out["high"]++
		case r.Score >= 2_000:
			out["medium"]++
		default:
			out["low"]++
		}
	}
	return out
}

func RejectionReasons(records []Record) map[string]int {
	out := make(map[string]int)
	for _, r := range records {
		reason := strings.TrimSpace(r.RejectionReason)
		if reason == "" {
			reason = "unknown"
		}
		out[reason]++
	}
	return out
}

func (r Record) Fields() map[string]any {
	return map[string]any{
		"route":                       r.Route,
		"dex_path":                    r.DEXPath,
		"hop_count":                   r.HopCount,
		"loan_asset":                  r.LoanAsset,
		"amount_in":                   decimal(r.AmountIn),
		"amount_out":                  decimal(r.AmountOut),
		"gross_profit":                decimal(r.GrossProfit),
		"aave_premium":                decimal(r.AavePremium),
		"l1_fee":                      decimal(r.L1Fee),
		"l2_fee":                      decimal(r.L2Fee),
		"price_impact_bps":            r.PriceImpactBPS,
		"net_profit":                  decimal(r.NetProfit),
		"min_profit_threshold":        decimal(r.MinProfit),
		"distance_from_profitability": decimal(r.GapToProfit),
		"rejection_reason":            r.RejectionReason,
		"route_score":                 r.Score,
		"evaluation_latency_ms":       r.EvaluationLatency.Milliseconds(),
		"quote_successful":            r.QuoteSuccessful,
		"sufficient_liquidity":        r.SufficientLiquidity,
		"cross_venue":                 r.CrossVenue,
		"repeated_losing_route_count": r.FailureCount,
	}
}

func gapBonus(gap *big.Int) int64 {
	if gap == nil || gap.Sign() <= 0 {
		return 2_500
	}
	digits := len(gap.String())
	switch {
	case digits <= 3:
		return 2_000
	case digits <= 5:
		return 1_500
	case digits <= 7:
		return 1_000
	case digits <= 9:
		return 500
	default:
		return 0
	}
}

func routeHasLiquidity(route routes.Route) bool {
	if len(route.Hops) == 0 {
		return false
	}
	for _, hop := range route.Hops {
		if hop.Liquidity == nil || hop.Liquidity.Sign() <= 0 {
			return false
		}
	}
	return true
}

func compareBig(a, b *big.Int) int {
	left, right := clone(a), clone(b)
	if left == nil {
		left = big.NewInt(0)
	}
	if right == nil {
		right = big.NewInt(0)
	}
	return left.Cmp(right)
}

func clone(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}

func decimal(value *big.Int) string {
	if value == nil {
		return "0"
	}
	return value.String()
}
