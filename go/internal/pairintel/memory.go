// Package pairintel maintains bounded, token-neutral pair market intelligence.
// It has no authority to alter execution assets, routes, profitability, or safety.
package pairintel

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/titanarb/titanarb-go/internal/pools"
)

type State string

const (
	Observed State = "observed"
	Shadow   State = "shadow"
	Admitted State = "admitted"
	Cooled   State = "cooled"
)

type Config struct {
	Enabled                 bool
	Mode                    string
	MaxTrackedPairs         int
	MaxShadowPairs          int
	MaxDynamicAssets        int
	MaxLivePairs            int
	MaxLiveDynamicAssets    int
	MinObservation          time.Duration
	MaxRPS                  float64
	Burst                   int
	MinScore                float64
	MinConfidence           float64
	Cooldown                time.Duration
	DislocationThresholdBPS float64
	MinVenueDEXes           int
	MinQuoteSuccess         float64
	MinDepthScore           float64
	MaxFailurePenalty       float64
	MaxIlliquidityPenalty   float64
	MaxScoreDrift           float64
	EjectWindows            int
}

func DefaultConfig() Config {
	return Config{Enabled: true, Mode: "shadow", MaxTrackedPairs: 16, MaxShadowPairs: 8, MaxDynamicAssets: 4,
		MinObservation: time.Hour, MaxRPS: .5, Burst: 1, MinScore: 65, MinConfidence: .70,
		Cooldown: time.Hour, DislocationThresholdBPS: 10, MaxLivePairs: 2, MaxLiveDynamicAssets: 2,
		MinVenueDEXes: 2, MinQuoteSuccess: .80, MinDepthScore: .50, MaxFailurePenalty: .50,
		MaxIlliquidityPenalty: .50, MaxScoreDrift: 15, EjectWindows: 3}
}

type TokenMeta struct {
	Address  string `json:"address"`
	Decimals uint8  `json:"decimals"`
	HasCode  bool   `json:"has_code"`
	Core     bool   `json:"core"`
	Symbol   string `json:"symbol,omitempty"`
}

type PairKey struct {
	Token0 string `json:"token0"`
	Token1 string `json:"token1"`
}

func CanonicalPair(a, b string) (PairKey, error) {
	a, b = strings.ToLower(strings.TrimSpace(a)), strings.ToLower(strings.TrimSpace(b))
	if !validAddress(a) || !validAddress(b) || a == b {
		return PairKey{}, errors.New("invalid pair addresses")
	}
	if a > b {
		a, b = b, a
	}
	return PairKey{Token0: a, Token1: b}, nil
}

func (k PairKey) String() string { return k.Token0 + ":" + k.Token1 }

type Venue struct {
	Pool              string           `json:"pool"`
	DEX               pools.DEX        `json:"dex"`
	Fee               uint32           `json:"fee"`
	LastPrice         float64          `json:"last_price"`
	LastUpdatedBlock  uint64           `json:"last_updated_block"`
	QuoteSuccessEMA   float64          `json:"quote_success_ema"`
	FailureEMA        float64          `json:"failure_ema"`
	QuoteObservations uint64           `json:"quote_observations"`
	Depth             map[string]Depth `json:"depth"`
}

type Depth struct {
	Direction          string    `json:"direction"`
	Bucket             string    `json:"bucket"`
	AmountIn           string    `json:"amount_in"`
	AmountOut          string    `json:"amount_out"`
	AmountInNormalized float64   `json:"amount_in_normalized"`
	PriceImpactBPS     float64   `json:"price_impact_bps"`
	Score              float64   `json:"score"`
	Successful         bool      `json:"successful"`
	ObservedAt         time.Time `json:"observed_at"`
}

type Bucket struct {
	Minute            int64   `json:"minute"`
	PriceOpen         float64 `json:"price_open"`
	PriceHigh         float64 `json:"price_high"`
	PriceLow          float64 `json:"price_low"`
	PriceClose        float64 `json:"price_close"`
	PriceObservations uint64  `json:"price_observations"`
	SquaredLogReturns float64 `json:"squared_log_returns"`
	VolumeToken0      float64 `json:"volume_token0"`
	Swaps             uint64  `json:"swaps"`
	DislocationHits   uint64  `json:"dislocation_hits"`
	DislocationChecks uint64  `json:"dislocation_checks"`
}

type Components struct {
	RV5m, RV1h, RV4h        float64
	Volume1h, Volume24h     float64
	Turnover1h, Turnover24h float64
	RV, Volume, Depth       float64
	VenueDiversity          float64
	QuoteSuccess            float64
	DislocationFrequency    float64
	FailurePenalty          float64
	IlliquidityPenalty      float64
}

type Pair struct {
	Key            PairKey          `json:"key"`
	Venues         map[string]Venue `json:"venues"`
	Buckets        []Bucket         `json:"buckets"`
	FirstObserved  time.Time        `json:"first_observed"`
	LastObserved   time.Time        `json:"last_observed"`
	LastPrice      float64          `json:"last_price"`
	SwapCount      uint64           `json:"swap_count"`
	FailureEMA     float64          `json:"failure_ema"`
	IlliquidityEMA float64          `json:"illiquidity_ema"`
	State          State            `json:"state"`
	CooldownUntil  time.Time        `json:"cooldown_until"`
	Score          float64          `json:"score"`
	Confidence     float64          `json:"confidence"`
	Components     Components       `json:"components"`
	RecentScores   []float64        `json:"recent_scores,omitempty"`
	BelowWindows   int              `json:"below_windows,omitempty"`
}

type Snapshot struct {
	Version     int                  `json:"version"`
	SavedAt     time.Time            `json:"saved_at"`
	Checkpoints map[string]uint64    `json:"factory_checkpoints"`
	Tokens      map[string]TokenMeta `json:"tokens"`
	Pairs       []Pair               `json:"pairs"`
}

type Memory struct {
	mu          sync.RWMutex
	persistMu   sync.Mutex
	cfg         Config
	tokens      map[string]TokenMeta
	pairs       map[string]*Pair
	poolPairs   map[string]string
	checkpoints map[string]uint64
	now         func() time.Time
}

func NewMemory(cfg Config) *Memory {
	if cfg.MaxTrackedPairs < 1 {
		cfg.MaxTrackedPairs = 16
	}
	if cfg.MaxShadowPairs < 1 {
		cfg.MaxShadowPairs = 8
	}
	if cfg.MaxDynamicAssets < 1 {
		cfg.MaxDynamicAssets = 4
	}
	if cfg.MaxLivePairs < 1 {
		cfg.MaxLivePairs = 2
	}
	if cfg.MaxLiveDynamicAssets < 1 {
		cfg.MaxLiveDynamicAssets = 2
	}
	if cfg.MinVenueDEXes < 2 {
		cfg.MinVenueDEXes = 2
	}
	if cfg.MinQuoteSuccess <= 0 {
		cfg.MinQuoteSuccess = .80
	}
	if cfg.MinDepthScore <= 0 {
		cfg.MinDepthScore = .50
	}
	if cfg.MaxFailurePenalty <= 0 {
		cfg.MaxFailurePenalty = .50
	}
	if cfg.MaxIlliquidityPenalty <= 0 {
		cfg.MaxIlliquidityPenalty = .50
	}
	if cfg.MaxScoreDrift <= 0 {
		cfg.MaxScoreDrift = 15
	}
	if cfg.EjectWindows < 1 {
		cfg.EjectWindows = 3
	}
	if cfg.MinObservation <= 0 {
		cfg.MinObservation = time.Hour
	}
	if cfg.MinScore <= 0 {
		cfg.MinScore = 65
	}
	if cfg.MinConfidence <= 0 {
		cfg.MinConfidence = .70
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = time.Hour
	}
	if cfg.DislocationThresholdBPS <= 0 {
		cfg.DislocationThresholdBPS = 10
	}
	return &Memory{cfg: cfg, tokens: make(map[string]TokenMeta), pairs: make(map[string]*Pair), poolPairs: make(map[string]string), checkpoints: make(map[string]uint64), now: time.Now}
}

func (m *Memory) RegisterToken(meta TokenMeta) error {
	meta.Address = strings.ToLower(strings.TrimSpace(meta.Address))
	if !validAddress(meta.Address) || !meta.HasCode || meta.Decimals > 36 {
		return errors.New("token rejected: no code, malformed address, or invalid decimals")
	}
	m.mu.Lock()
	m.tokens[meta.Address] = meta
	m.mu.Unlock()
	return nil
}

func (m *Memory) ObservePool(pool pools.Pool) {
	key, err := CanonicalPair(pool.Token0, pool.Token1)
	if err != nil {
		return
	}
	now := m.now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tokens[key.Token0]; !ok {
		return
	}
	if _, ok := m.tokens[key.Token1]; !ok {
		return
	}
	pair := m.ensurePairLocked(key, now)
	price := canonicalPrice(pool.SqrtPriceX96, m.tokens[key.Token0].Decimals, m.tokens[key.Token1].Decimals)
	if !strings.EqualFold(pool.Token0, key.Token0) && price > 0 {
		price = 1 / price
	}
	venue := pair.Venues[strings.ToLower(pool.Address)]
	venue.Pool, venue.DEX, venue.Fee = strings.ToLower(pool.Address), pool.DEX, pool.Fee
	venue.LastUpdatedBlock = pool.LastUpdatedBlock
	if venue.Depth == nil {
		venue.Depth = make(map[string]Depth)
	}
	if price > 0 && !math.IsInf(price, 0) && !math.IsNaN(price) {
		bucket := m.bucketLocked(pair, now)
		if pair.LastPrice > 0 {
			bucket.SquaredLogReturns += math.Pow(math.Log(price/pair.LastPrice), 2)
		}
		if bucket.PriceObservations == 0 {
			bucket.PriceOpen, bucket.PriceHigh, bucket.PriceLow = price, price, price
		}
		if price > bucket.PriceHigh {
			bucket.PriceHigh = price
		}
		if bucket.PriceLow == 0 || price < bucket.PriceLow {
			bucket.PriceLow = price
		}
		bucket.PriceClose = price
		bucket.PriceObservations++
		venue.LastPrice, pair.LastPrice = price, price
	}
	pair.Venues[venue.Pool] = venue
	pair.LastObserved = now
	m.poolPairs[venue.Pool] = pair.Key.String()
	m.observeDislocationLocked(pair, now)
	m.scoreLocked(pair, now)
	m.pruneLocked()
}

func (m *Memory) ObserveSwap(swap pools.Swap) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pair := m.pairByPoolLocked(swap.Pool)
	if pair == nil {
		return
	}
	now := m.now().UTC()
	b := m.bucketLocked(pair, now)
	a0 := normalizedAbs(swap.Amount0, m.tokens[pair.Key.Token0].Decimals)
	a1 := normalizedAbs(swap.Amount1, m.tokens[pair.Key.Token1].Decimals)
	price := pair.LastPrice
	if price > 0 {
		b.VolumeToken0 += .5 * (a0 + a1/price)
	} else {
		b.VolumeToken0 += a0
	}
	b.Swaps++
	pair.SwapCount++
	pair.LastObserved = now
	m.scoreLocked(pair, now)
}

func (m *Memory) RecordDepth(pool string, depth Depth) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pair := m.pairByPoolLocked(pool)
	if pair == nil {
		return
	}
	venue := pair.Venues[strings.ToLower(pool)]
	if venue.Depth == nil {
		venue.Depth = make(map[string]Depth)
	}
	alpha := .25
	success := boolFloat(depth.Successful)
	if venue.QuoteObservations == 0 {
		venue.QuoteSuccessEMA = success
	} else {
		venue.QuoteSuccessEMA = ema(venue.QuoteSuccessEMA, success, alpha)
	}
	venue.FailureEMA = ema(venue.FailureEMA, 1-success, alpha)
	venue.QuoteObservations++
	venue.Depth[depth.Direction+":"+depth.Bucket] = depth
	pair.Venues[venue.Pool] = venue
	pair.FailureEMA = ema(pair.FailureEMA, 1-success, alpha)
	pair.IlliquidityEMA = ema(pair.IlliquidityEMA, boolFloat(!depth.Successful || depth.Score < .35), alpha)
	if pair.FailureEMA > .80 || pair.IlliquidityEMA > .80 {
		pair.State = Cooled
		pair.CooldownUntil = m.now().Add(m.cfg.Cooldown)
	}
	m.scoreLocked(pair, m.now().UTC())
}

func (m *Memory) SelectShadow() []Pair {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now().UTC()
	var candidates []*Pair
	for _, p := range m.pairs {
		m.scoreLocked(p, now)
		if p.State == Cooled && now.Before(p.CooldownUntil) {
			continue
		}
		if p.State == Cooled {
			p.State = Observed
		}
		if now.Sub(p.FirstObserved) >= m.cfg.MinObservation && p.Score >= m.cfg.MinScore && p.Confidence >= m.cfg.MinConfidence {
			candidates = append(candidates, p)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		return candidates[i].Key.String() < candidates[j].Key.String()
	})
	for _, p := range m.pairs {
		if p.State == Shadow {
			p.State = Observed
		}
	}
	limit := m.cfg.MaxShadowPairs
	if len(candidates) < limit {
		limit = len(candidates)
	}
	out := make([]Pair, 0, limit)
	dynamic := make(map[string]struct{})
	for _, p := range candidates {
		if len(out) >= limit {
			break
		}
		added := []string{}
		for _, a := range []string{p.Key.Token0, p.Key.Token1} {
			if !m.tokens[a].Core {
				if _, ok := dynamic[a]; !ok {
					added = append(added, a)
				}
			}
		}
		if len(dynamic)+len(added) > m.cfg.MaxDynamicAssets {
			continue
		}
		for _, a := range added {
			dynamic[a] = struct{}{}
		}
		p.State = Shadow
		out = append(out, clonePair(p))
	}
	return out
}

// AdmissionSnapshot is an immutable market-only graph proposal. Tokens in
// this snapshot never become flash-loan/start assets; that boundary remains
// owned by MarketConfig.ExecutionAssets and the execution preflight.
type AdmissionSnapshot struct {
	Pairs  []Pair
	Tokens []TokenMeta
}

func (m *Memory) SelectLive() AdmissionSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now().UTC()
	candidates := make([]*Pair, 0)
	preserved := make([]*Pair, 0)
	for _, p := range m.pairs {
		m.scoreLocked(p, now)
		p.RecentScores = append(p.RecentScores, p.Score)
		if len(p.RecentScores) > 4 {
			p.RecentScores = append([]float64(nil), p.RecentScores[len(p.RecentScores)-4:]...)
		}
		qualified := m.liveQualifiedLocked(p, now)
		if p.State == Admitted && !qualified {
			p.BelowWindows++
			if p.BelowWindows >= m.cfg.EjectWindows {
				p.State, p.CooldownUntil = Cooled, now.Add(m.cfg.Cooldown)
			} else {
				// Keep the current graph stable during the configured consecutive
				// below-threshold grace windows. Ejection occurs atomically only
				// after the threshold is reached.
				preserved = append(preserved, p)
			}
		} else if qualified {
			p.BelowWindows = 0
			candidates = append(candidates, p)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		return candidates[i].Key.String() < candidates[j].Key.String()
	})
	selected := make(map[string]struct{})
	dynamic := make(map[string]struct{})
	for _, p := range preserved {
		selected[p.Key.String()] = struct{}{}
		for _, a := range []string{p.Key.Token0, p.Key.Token1} {
			if !m.tokens[a].Core {
				dynamic[a] = struct{}{}
			}
		}
	}
	for _, p := range candidates {
		if len(selected) >= m.cfg.MaxLivePairs {
			break
		}
		if _, exists := selected[p.Key.String()]; exists {
			continue
		}
		added := make([]string, 0, 2)
		for _, a := range []string{p.Key.Token0, p.Key.Token1} {
			if !m.tokens[a].Core {
				if _, ok := dynamic[a]; !ok {
					added = append(added, a)
				}
			}
		}
		if len(dynamic)+len(added) > m.cfg.MaxLiveDynamicAssets {
			continue
		}
		for _, a := range added {
			dynamic[a] = struct{}{}
		}
		selected[p.Key.String()] = struct{}{}
		p.State = Admitted
	}
	for _, p := range m.pairs {
		if p.State == Admitted {
			if _, ok := selected[p.Key.String()]; !ok {
				p.State = Observed
			}
		}
	}
	out := AdmissionSnapshot{}
	for key := range selected {
		out.Pairs = append(out.Pairs, clonePair(m.pairs[key]))
	}
	for a := range dynamic {
		out.Tokens = append(out.Tokens, m.tokens[a])
	}
	sort.Slice(out.Pairs, func(i, j int) bool { return out.Pairs[i].Key.String() < out.Pairs[j].Key.String() })
	sort.Slice(out.Tokens, func(i, j int) bool { return out.Tokens[i].Address < out.Tokens[j].Address })
	return out
}

func (m *Memory) liveQualifiedLocked(p *Pair, now time.Time) bool {
	if p.State == Cooled && now.Before(p.CooldownUntil) {
		return false
	}
	if now.Sub(p.FirstObserved) < m.cfg.MinObservation || p.Score < m.cfg.MinScore || p.Confidence < m.cfg.MinConfidence {
		return false
	}
	dexes := map[pools.DEX]struct{}{}
	coreConnected := false
	for _, v := range p.Venues {
		dexes[v.DEX] = struct{}{}
	}
	for _, a := range []string{p.Key.Token0, p.Key.Token1} {
		coreConnected = coreConnected || m.tokens[a].Core
	}
	if len(dexes) < m.cfg.MinVenueDEXes || !coreConnected {
		return false
	}
	c := p.Components
	if c.QuoteSuccess < m.cfg.MinQuoteSuccess || c.Depth < m.cfg.MinDepthScore || c.FailurePenalty > m.cfg.MaxFailurePenalty || c.IlliquidityPenalty > m.cfg.MaxIlliquidityPenalty {
		return false
	}
	if len(p.RecentScores) < 3 {
		return false
	}
	lo, hi := p.RecentScores[0], p.RecentScores[0]
	for _, s := range p.RecentScores[1:] {
		if s < lo {
			lo = s
		}
		if s > hi {
			hi = s
		}
	}
	return hi-lo <= m.cfg.MaxScoreDrift
}

func (m *Memory) Tokens() []TokenMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TokenMeta, 0, len(m.tokens))
	for _, t := range m.tokens {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address < out[j].Address })
	return out
}
func (m *Memory) PairScore(a, b string) (float64, bool) {
	k, err := CanonicalPair(a, b)
	if err != nil {
		return 0, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.pairs[k.String()]
	if !ok {
		return 0, false
	}
	return p.Score, true
}

func (m *Memory) Pairs() []Pair {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Pair, 0, len(m.pairs))
	for _, p := range m.pairs {
		out = append(out, clonePair(p))
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Key.String() < out[j].Key.String()
	})
	return out
}

func (m *Memory) Telemetry(limit int) []map[string]any {
	pairs := m.Pairs()
	if limit > 0 && len(pairs) > limit {
		pairs = pairs[:limit]
	}
	out := make([]map[string]any, 0, len(pairs))
	for _, p := range pairs {
		venueSet := make(map[string]struct{})
		depth := make(map[string]Depth)
		for _, v := range p.Venues {
			venueSet[string(v.DEX)] = struct{}{}
			for k, d := range v.Depth {
				depth[v.Pool+":"+k] = d
			}
		}
		venues := make([]string, 0, len(venueSet))
		for venue := range venueSet {
			venues = append(venues, venue)
		}
		sort.Strings(venues)
		c := p.Components
		out = append(out, map[string]any{"pair": p.Key.String(), "state": p.State, "score": round2(p.Score), "confidence": round2(p.Confidence), "venues": venues, "swap_count": p.SwapCount, "rv_5m_bps": round2(c.RV5m), "rv_1h_bps": round2(c.RV1h), "rv_4h_bps": round2(c.RV4h), "volume_1h_token0": round2(c.Volume1h), "volume_24h_token0": round2(c.Volume24h), "turnover_1h": round2(c.Turnover1h), "turnover_24h": round2(c.Turnover24h), "depth_score": round2(c.Depth), "quote_success": round2(c.QuoteSuccess), "venue_diversity": round2(c.VenueDiversity), "dislocation_frequency": round2(c.DislocationFrequency), "failure_penalty": round2(c.FailurePenalty), "illiquidity_penalty": round2(c.IlliquidityPenalty), "depth_buckets": depth})
	}
	return out
}

func (m *Memory) Summary() (mode string, tracked, shadow int, topScore float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mode = m.cfg.Mode
	tracked = len(m.pairs)
	for _, p := range m.pairs {
		if p.State == Shadow {
			shadow++
		}
		if p.Score > topScore {
			topScore = p.Score
		}
	}
	return mode, tracked, shadow, round2(topScore)
}

func (m *Memory) Checkpoint(dex string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkpoints[dex]
}
func (m *Memory) SetCheckpoint(dex string, block uint64) {
	m.mu.Lock()
	if block > m.checkpoints[dex] {
		m.checkpoints[dex] = block
	}
	m.mu.Unlock()
}

func (m *Memory) Save(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	m.mu.RLock()
	snap := Snapshot{Version: 1, SavedAt: m.now().UTC(), Checkpoints: cloneCheckpoints(m.checkpoints), Tokens: cloneTokens(m.tokens)}
	for _, p := range m.pairs {
		snap.Pairs = append(snap.Pairs, clonePair(p))
	}
	m.mu.RUnlock()
	sort.Slice(snap.Pairs, func(i, j int) bool { return snap.Pairs[i].Key.String() < snap.Pairs[j].Key.String() })
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func Load(path string, cfg Config) (*Memory, error) {
	m := NewMemory(cfg)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	var s Snapshot
	if err = json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	if s.Version != 1 {
		return nil, fmt.Errorf("unsupported pair memory version %d", s.Version)
	}
	for k, v := range s.Tokens {
		m.tokens[k] = v
	}
	for _, p := range s.Pairs {
		cp := p
		if cp.Venues == nil {
			cp.Venues = make(map[string]Venue)
		}
		m.pairs[p.Key.String()] = &cp
		for addr := range cp.Venues {
			m.poolPairs[strings.ToLower(addr)] = p.Key.String()
		}
	}
	for k, v := range s.Checkpoints {
		m.checkpoints[k] = v
	}
	m.pruneLocked()
	return m, nil
}

func (m *Memory) ensurePairLocked(key PairKey, now time.Time) *Pair {
	if p := m.pairs[key.String()]; p != nil {
		return p
	}
	p := &Pair{Key: key, Venues: make(map[string]Venue), FirstObserved: now, LastObserved: now, State: Observed}
	m.pairs[key.String()] = p
	return p
}
func (m *Memory) pairByPoolLocked(pool string) *Pair {
	key := m.poolPairs[strings.ToLower(pool)]
	if key != "" {
		return m.pairs[key]
	}
	for k, p := range m.pairs {
		if _, ok := p.Venues[strings.ToLower(pool)]; ok {
			m.poolPairs[strings.ToLower(pool)] = k
			return p
		}
	}
	return nil
}
func (m *Memory) bucketLocked(p *Pair, now time.Time) *Bucket {
	minute := now.Unix() / 60
	for i := range p.Buckets {
		if p.Buckets[i].Minute == minute {
			return &p.Buckets[i]
		}
	}
	p.Buckets = append(p.Buckets, Bucket{Minute: minute})
	cut := minute - 24*60
	first := 0
	for first < len(p.Buckets) && p.Buckets[first].Minute < cut {
		first++
	}
	if first > 0 {
		p.Buckets = append([]Bucket(nil), p.Buckets[first:]...)
	}
	return &p.Buckets[len(p.Buckets)-1]
}
func (m *Memory) observeDislocationLocked(p *Pair, now time.Time) {
	prices := make(map[pools.DEX][]float64)
	for _, v := range p.Venues {
		if v.LastPrice > 0 {
			prices[v.DEX] = append(prices[v.DEX], v.LastPrice)
		}
	}
	if len(prices) < 2 {
		return
	}
	var min, max float64
	for _, vs := range prices {
		for _, v := range vs {
			if min == 0 || v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	if min <= 0 {
		return
	}
	b := m.bucketLocked(p, now)
	b.DislocationChecks++
	if (max/min-1)*10000 >= m.cfg.DislocationThresholdBPS {
		b.DislocationHits++
	}
}
func (m *Memory) scoreLocked(p *Pair, now time.Time) {
	c := Components{}
	c.RV5m = rv(p.Buckets, now, 5)
	c.RV1h = rv(p.Buckets, now, 60)
	c.RV4h = rv(p.Buckets, now, 240)
	c.Volume1h, c.Volume24h = volume(p.Buckets, now, 60), volume(p.Buckets, now, 1440)
	depth := pairDepth(p)
	c.Turnover1h = c.Volume1h / math.Max(depth, .000001)
	c.Turnover24h = c.Volume24h / math.Max(depth, .000001)
	c.RV = .45*saturate(c.RV5m, 500) + .35*saturate(c.RV1h, 1500) + .20*saturate(c.RV4h, 3000)
	c.Volume = .60*saturateLog(c.Turnover1h, 20) + .40*saturateLog(c.Turnover24h, 100)
	c.Depth = depthScore(p)
	c.VenueDiversity = venueDiversity(p)
	c.QuoteSuccess = quoteSuccess(p)
	c.DislocationFrequency = dislocation(p.Buckets, now, 240)
	c.FailurePenalty = p.FailureEMA
	c.IlliquidityPenalty = p.IlliquidityEMA
	p.Components = c
	p.Score = 100 * clamp(.20*c.RV+.15*c.Volume+.25*c.Depth+.15*c.VenueDiversity+.10*c.QuoteSuccess+.15*c.DislocationFrequency-.15*c.FailurePenalty-.20*c.IlliquidityPenalty)
	age := now.Sub(p.FirstObserved)
	ageScore := math.Min(1, age.Seconds()/m.cfg.MinObservation.Seconds())
	obsScore := math.Min(1, float64(p.SwapCount+quoteObservations(p))/20)
	windowScore := math.Min(1, float64(len(p.Buckets))/60)
	p.Confidence = clamp(.35*ageScore + .30*obsScore + .20*windowScore + .15*c.VenueDiversity)
}
func rv(bs []Bucket, now time.Time, mins int) float64 {
	cut := now.Unix()/60 - int64(mins)
	sum := 0.0
	for _, b := range bs {
		if b.Minute >= cut {
			sum += b.SquaredLogReturns
		}
	}
	return math.Sqrt(sum) * 10000
}
func volume(bs []Bucket, now time.Time, mins int) float64 {
	cut := now.Unix()/60 - int64(mins)
	sum := 0.0
	for _, b := range bs {
		if b.Minute >= cut {
			sum += b.VolumeToken0
		}
	}
	return sum
}
func dislocation(bs []Bucket, now time.Time, mins int) float64 {
	cut := now.Unix()/60 - int64(mins)
	var h, n uint64
	for _, b := range bs {
		if b.Minute >= cut {
			h += b.DislocationHits
			n += b.DislocationChecks
		}
	}
	if n == 0 {
		return 0
	}
	return float64(h) / float64(n)
}
func venueDiversity(p *Pair) float64 {
	s := map[pools.DEX]bool{}
	for _, v := range p.Venues {
		if v.LastPrice > 0 {
			s[v.DEX] = true
		}
	}
	if len(s) >= 2 {
		return 1
	}
	if len(s) == 1 {
		return .25
	}
	return 0
}
func quoteSuccess(p *Pair) float64 {
	sum := 0.0
	n := 0
	for _, v := range p.Venues {
		if v.QuoteObservations > 0 {
			sum += v.QuoteSuccessEMA
			n++
		}
	}
	if n == 0 {
		return .5
	}
	return sum / float64(n)
}
func quoteObservations(p *Pair) uint64 {
	var n uint64
	for _, v := range p.Venues {
		n += v.QuoteObservations
	}
	return n
}
func depthScore(p *Pair) float64 {
	sum := 0.0
	n := 0
	for _, v := range p.Venues {
		for _, d := range v.Depth {
			sum += d.Score
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}
func pairDepth(p *Pair) float64 {
	sum := 0.0
	n := 0
	for _, v := range p.Venues {
		for _, d := range v.Depth {
			if d.Successful && d.AmountInNormalized > 0 {
				sum += d.AmountInNormalized * d.Score
				n++
			}
		}
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}
func canonicalPrice(sqrt *big.Int, d0, d1 uint8) float64 {
	if sqrt == nil || sqrt.Sign() <= 0 {
		return 0
	}
	r := new(big.Rat).SetInt(sqrt)
	q := new(big.Int).Lsh(big.NewInt(1), 96)
	r.Quo(r, new(big.Rat).SetInt(q))
	r.Mul(r, r)
	if d0 > d1 {
		r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d0-d1)), nil)))
	} else if d1 > d0 {
		r.Quo(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d1-d0)), nil)))
	}
	f, _ := r.Float64()
	return f
}
func normalizedAbs(v *big.Int, d uint8) float64 {
	if v == nil {
		return 0
	}
	x := new(big.Int).Abs(v)
	r := new(big.Rat).SetInt(x)
	r.Quo(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d)), nil)))
	f, _ := r.Float64()
	return f
}
func (m *Memory) pruneLocked() {
	if len(m.pairs) <= m.cfg.MaxTrackedPairs {
		return
	}
	all := make([]*Pair, 0, len(m.pairs))
	for _, p := range m.pairs {
		all = append(all, p)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].State != all[j].State {
			return stateRank(all[i].State) < stateRank(all[j].State)
		}
		if all[i].Score != all[j].Score {
			return all[i].Score < all[j].Score
		}
		return all[i].LastObserved.Before(all[j].LastObserved)
	})
	for len(m.pairs) > m.cfg.MaxTrackedPairs {
		p := all[0]
		all = all[1:]
		delete(m.pairs, p.Key.String())
		for a := range p.Venues {
			delete(m.poolPairs, a)
		}
	}
}
func stateRank(s State) int {
	switch s {
	case Cooled:
		return 0
	case Observed:
		return 1
	case Shadow:
		return 2
	case Admitted:
		return 3
	}
	return 0
}
func round2(v float64) float64 { return math.Round(v*100) / 100 }
func clonePair(p *Pair) Pair {
	b, _ := json.Marshal(p)
	var c Pair
	_ = json.Unmarshal(b, &c)
	return c
}
func cloneTokens(in map[string]TokenMeta) map[string]TokenMeta {
	o := make(map[string]TokenMeta, len(in))
	for k, v := range in {
		o[k] = v
	}
	return o
}
func cloneCheckpoints(in map[string]uint64) map[string]uint64 {
	o := make(map[string]uint64, len(in))
	for k, v := range in {
		o[k] = v
	}
	return o
}
func validAddress(s string) bool {
	if len(s) != 42 || !strings.HasPrefix(s, "0x") {
		return false
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func ema(a, b, x float64) float64 { return a*(1-x) + b*x }
func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
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
func saturate(v, max float64) float64 { return clamp(v / max) }
func saturateLog(v, max float64) float64 {
	if v <= 0 {
		return 0
	}
	return clamp(math.Log1p(v) / math.Log1p(max))
}
