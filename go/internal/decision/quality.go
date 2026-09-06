package decision

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/nearmiss"
	"github.com/titanarb/titanarb-go/internal/pairintel"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
)

type PoolQuality struct {
	Pool         string    `json:"pool"`
	DEX          pools.DEX `json:"dex"`
	Score        float64   `json:"score"`
	Depth        float64   `json:"depth"`
	QuoteSuccess float64   `json:"quote_success"`
	Freshness    float64   `json:"freshness"`
	Directions   int       `json:"fresh_depth_directions"`
	Reasons      []string  `json:"reasons"`
}
type PairQuality struct {
	Key            string               `json:"pair"`
	Score          float64              `json:"score"`
	Confidence     float64              `json:"confidence"`
	Components     pairintel.Components `json:"components"`
	Venues         int                  `json:"independent_venues"`
	ShadowAdmitted bool                 `json:"shadow_admitted"`
	Reasons        []string             `json:"reasons"`
	Pools          []PoolQuality        `json:"pools"`
}
type History struct {
	Evaluations   uint64    `json:"evaluations"`
	PositiveGross uint64    `json:"positive_gross"`
	PositiveNet   uint64    `json:"positive_net"`
	NearMisses    uint64    `json:"near_misses"`
	Failures      uint64    `json:"failures"`
	GrossBPS      float64   `json:"gross_bps_ema"`
	NetBPS        float64   `json:"legacy_net_bps_ema"`
	LossBPS       float64   `json:"roundtrip_loss_bps_ema"`
	LatencyMS     float64   `json:"latency_ms_ema"`
	BestGrossBPS  float64   `json:"best_gross_bps"`
	BestNetBPS    float64   `json:"best_legacy_net_bps"`
	LastSeen      time.Time `json:"last_seen"`
	LastUseful    time.Time `json:"last_useful"`
}
type RankedRoute struct {
	Key                 string       `json:"route_key"`
	Route               routes.Route `json:"route"`
	Score               float64      `json:"score"`
	PairQuality         float64      `json:"pair_quality"`
	PoolQuality         float64      `json:"pool_quality"`
	HistoryContribution float64      `json:"history_contribution"`
	CrossVenue          bool         `json:"cross_venue"`
	Observations        uint64       `json:"observations"`
	Dirty               bool         `json:"dirty"`
	Bucket              string       `json:"selection_bucket"`
	Reasons             []string     `json:"reasons"`
}

func clamp(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) || x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
func PoolScore(v pairintel.Venue, now time.Time) PoolQuality {
	q := PoolQuality{Pool: strings.ToLower(v.Pool), DEX: v.DEX, QuoteSuccess: clamp(v.QuoteSuccessEMA), Reasons: []string{}}
	dirs := map[string]float64{}
	for _, d := range v.Depth {
		if !d.Successful || d.ObservedAt.After(now) || now.Sub(d.ObservedAt) > 5*time.Minute {
			continue
		}
		if d.Direction != "0to1" && d.Direction != "1to0" {
			continue
		}
		if x := clamp(d.Score); x > dirs[d.Direction] {
			dirs[d.Direction] = x
		}
	}
	q.Directions = len(dirs)
	if q.Directions == 2 {
		q.Depth = math.Min(dirs["0to1"], dirs["1to0"])
		q.Freshness = 1
	} else {
		q.Reasons = append(q.Reasons, "missing fresh bidirectional executable depth")
	}
	if v.QuoteObservations == 0 {
		q.QuoteSuccess = .5
		q.Reasons = append(q.Reasons, "neutral quote prior")
	}
	q.Score = 100 * clamp(.5*q.Depth+.3*q.QuoteSuccess+.2*q.Freshness-.25*clamp(v.FailureEMA))
	return q
}
func PairScore(p pairintel.Pair, now time.Time) PairQuality {
	q := PairQuality{Key: p.Key.String(), Confidence: clamp(p.Confidence), Components: p.Components, Reasons: []string{}}
	venues := map[pools.DEX]bool{}
	usable := map[pools.DEX]bool{}
	for _, v := range p.Venues {
		if v.DEX != pools.UniswapV3 && v.DEX != pools.CamelotV3 {
			continue
		}
		venues[v.DEX] = true
		pq := PoolScore(v, now)
		q.Pools = append(q.Pools, pq)
		if pq.Directions == 2 && pq.Depth >= .5 && pq.QuoteSuccess >= .8 {
			usable[v.DEX] = true
		}
	}
	sort.Slice(q.Pools, func(i, j int) bool { return q.Pools[i].Pool < q.Pools[j].Pool })
	q.Venues = len(venues)
	c := p.Components
	q.Score = 100 * clamp(.15*clamp(c.RV)+.15*clamp(c.Volume)+.25*clamp(c.Depth)+.15*clamp(float64(len(usable))/2)+.15*clamp(c.QuoteSuccess)+.15*clamp(c.DislocationFrequency)-.2*clamp(c.FailurePenalty)-.2*clamp(c.IlliquidityPenalty))
	if len(usable) < 2 {
		q.Reasons = append(q.Reasons, "fewer than two usable independent venues")
	}
	if p.FirstObserved.IsZero() || now.Sub(p.FirstObserved) < time.Hour {
		q.Reasons = append(q.Reasons, "observation below one hour")
	}
	if p.LastObserved.After(now) || now.Sub(p.LastObserved) > 5*time.Minute {
		q.Reasons = append(q.Reasons, "stale pair observations")
	}
	if p.CooldownUntil.After(now) {
		q.Reasons = append(q.Reasons, "pair cooldown")
	}
	if q.Score < 65 || q.Confidence < .7 {
		q.Reasons = append(q.Reasons, "score/confidence below shadow threshold")
	}
	if c.Volume1h <= 0 {
		q.Reasons = append(q.Reasons, "no observed one-hour activity")
	}
	q.ShadowAdmitted = len(q.Reasons) == 0
	return q
}

func RouteKey(r routes.Route, m config.MarketConfig) string {
	var b strings.Builder
	for i, s := range r.Symbols {
		t, ok := m.Tokens[s]
		if !ok {
			return ""
		}
		a, err := address(Asset{t.Address, t.Decimals})
		if err != nil {
			return ""
		}
		b.WriteString(a)
		if i < len(r.Hops) {
			fmt.Fprintf(&b, "/%s/%s/%d/", r.Hops[i].DEX, strings.ToLower(r.Hops[i].Address), r.Hops[i].Fee)
		}
	}
	return b.String()
}
func weakestHarmonic(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := 100.0
	sum := 0.0
	for _, v := range values {
		if v <= 0 {
			return 0
		}
		if v < min {
			min = v
		}
		sum += 1 / v
	}
	return .6*min + .4*float64(len(values))/sum
}
func Rank(rs []routes.Route, m config.MarketConfig, pairs []PairQuality, hist map[string]History, dirty map[string]bool, now time.Time) []RankedRoute {
	pm := map[string]PairQuality{}
	qm := map[string]PoolQuality{}
	for _, p := range pairs {
		pm[p.Key] = p
		for _, q := range p.Pools {
			qm[q.Pool] = q
		}
	}
	out := []RankedRoute{}
	seen := map[string]bool{}
	for _, r := range rs {
		key := RouteKey(r, m)
		if key == "" || seen[key] || len(r.Hops) < 2 || len(r.Hops) > 4 || len(r.Symbols) != len(r.Hops)+1 {
			continue
		}
		seen[key] = true
		x := RankedRoute{Key: key, Route: r, CrossVenue: nearmiss.CrossVenue(r), Reasons: []string{}}
		ps, qs := []float64{}, []float64{}
		for i, p := range r.Hops {
			pk, _ := pairintel.CanonicalPair(m.Tokens[r.Symbols[i]].Address, m.Tokens[r.Symbols[i+1]].Address)
			ps = append(ps, pm[pk.String()].Score)
			qs = append(qs, qm[strings.ToLower(p.Address)].Score)
			x.Dirty = x.Dirty || dirty[strings.ToLower(p.Address)]
		}
		x.PairQuality = weakestHarmonic(ps)
		x.PoolQuality = weakestHarmonic(qs)
		h := hist[key]
		x.Observations = h.Evaluations
		if h.Evaluations > 0 {
			decay := math.Exp(-math.Max(0, now.Sub(h.LastSeen).Seconds()) / 3600)
			x.HistoryContribution = decay * (10*clamp((h.GrossBPS+100)/200) - 10*clamp(h.LossBPS/1000) - 5*clamp(h.LatencyMS/5000) - 10*float64(h.Failures)/float64(h.Evaluations))
		}
		x.Score = .4*x.PairQuality + .25*x.PoolQuality + 10 + x.HistoryContribution - 2*float64(len(r.Hops)-2)
		if x.CrossVenue {
			x.Score += 10
			x.Reasons = append(x.Reasons, "independent venues")
		}
		if x.Dirty {
			x.Score += 5
			x.Reasons = append(x.Reasons, "changed pool; reconsider history")
		}
		if h.NearMisses > 0 {
			x.Score += 5
			x.Reasons = append(x.Reasons, "historical near miss")
		}
		if h.Evaluations == 0 {
			x.Reasons = append(x.Reasons, "unseen; exploration eligible")
		}
		x.Score = 100 * clamp(x.Score/100)
		out = append(out, x)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// Select rotates funding-asset priority and guarantees an exploration slot when
// budget >=2. It only selects proposals for shadow, never live engine work.
func Select(in []RankedRoute, limit int, round uint64) []RankedRoute {
	if limit <= 0 {
		return nil
	}
	out := []RankedRoute{}
	seen := map[string]bool{}
	per := map[string]int{}
	assets := []string{}
	aset := map[string]bool{}
	for _, r := range in {
		a := r.Route.Symbols[0]
		if !aset[a] {
			aset[a] = true
			assets = append(assets, a)
		}
	}
	sort.Strings(assets)
	if len(assets) == 0 {
		return nil
	}
	quota := (limit + len(assets) - 1) / len(assets)
	add := func(r RankedRoute, bucket string) bool {
		a := r.Route.Symbols[0]
		if seen[r.Key] || per[a] >= quota || len(out) >= limit {
			return false
		}
		r.Bucket = bucket
		out = append(out, r)
		seen[r.Key] = true
		per[a]++
		return true
	}
	explore := append([]RankedRoute(nil), in...)
	sort.SliceStable(explore, func(i, j int) bool { return explore[i].Observations < explore[j].Observations })
	if limit > 1 {
		for _, r := range explore {
			if add(r, "explore") {
				break
			}
		}
	}
	for n := 0; n < len(assets); n++ {
		a := assets[(n+int(round%uint64(len(assets))))%len(assets)]
		for _, r := range in {
			if r.Route.Symbols[0] == a {
				bucket := "exploit"
				if r.Dirty {
					bucket = "event"
				}
				add(r, bucket)
			}
		}
	}
	return out
}

func bps(raw, amount *big.Int) float64 {
	if raw == nil || amount == nil || amount.Sign() <= 0 {
		return 0
	}
	f, _ := new(big.Rat).SetFrac(new(big.Int).Mul(raw, big.NewInt(10000)), amount).Float64()
	return f
}
func UpdateHistory(h History, r nearmiss.Record, now time.Time) History {
	g, n := bps(r.GrossProfit, r.AmountIn), bps(r.NetProfit, r.AmountIn)
	if h.Evaluations == 0 {
		h.GrossBPS = g
		h.NetBPS = n
		h.LossBPS = math.Max(0, -g)
		h.LatencyMS = float64(r.EvaluationLatency.Milliseconds())
		h.BestGrossBPS = g
		h.BestNetBPS = n
	} else {
		h.GrossBPS = .8*h.GrossBPS + .2*g
		h.NetBPS = .8*h.NetBPS + .2*n
		h.LossBPS = .8*h.LossBPS + .2*math.Max(0, -g)
		h.LatencyMS = .8*h.LatencyMS + .2*float64(r.EvaluationLatency.Milliseconds())
	}
	h.Evaluations++
	h.LastSeen = now
	if !r.QuoteSuccessful {
		h.Failures++
	}
	if g > 0 {
		h.PositiveGross++
	}
	if n > 0 {
		h.PositiveNet++
	}
	if r.NetProfit != nil && r.MinProfit != nil && r.MinProfit.Sign() > 0 && r.NetProfit.Sign() > 0 && r.NetProfit.Cmp(r.MinProfit) <= 0 {
		h.NearMisses++
		h.LastUseful = now
	}
	h.BestGrossBPS = math.Max(h.BestGrossBPS, g)
	h.BestNetBPS = math.Max(h.BestNetBPS, n)
	return h
}
