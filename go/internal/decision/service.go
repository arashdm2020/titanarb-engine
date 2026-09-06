package decision

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/nearmiss"
	"github.com/titanarb/titanarb-go/internal/pairintel"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/routes"
	"github.com/titanarb/titanarb-go/internal/rpc"
)

// ReadClient deliberately does not expose submission, signing, or execution.
type ReadClient interface {
	EthCallAt(context.Context, map[string]string, string) (string, error)
}
type Input struct {
	Block      uint64
	Market     config.MarketConfig
	Routes     []routes.Route
	Amounts    map[string]*big.Int
	Dirty      map[string]bool
	Unresolved map[string]bool
}
type Observation struct {
	Key    string
	Block  uint64
	Record nearmiss.Record
	Asset  Asset
}
type Service struct {
	client       ReadClient
	pairs        *pairintel.Memory
	dir          string
	busy         func() bool
	in           chan Input
	observations chan Observation
	dropped      atomic.Uint64
	writeErrors  atomic.Uint64
	history      map[string]History // owned only by Run
	shadowSeen   map[string]uint64
	round        uint64
	reads        uint64
	minute       time.Time
	lastRead     time.Time
	ReadLimit    int
	ProbeEnabled bool
}

func NewService(c ReadClient, p *pairintel.Memory, dir string, busy func() bool) *Service {
	return &Service{client: c, pairs: p, dir: dir, busy: busy, in: make(chan Input, 1), observations: make(chan Observation, 128), history: map[string]History{}, shadowSeen: map[string]uint64{}, ReadLimit: 12, ProbeEnabled: true}
}
func (s *Service) Observe(r routes.Route, m config.MarketConfig, n nearmiss.Record, block uint64) {
	if s == nil {
		return
	}
	n.AmountIn = copyInt(n.AmountIn)
	n.AmountOut = copyInt(n.AmountOut)
	n.GrossProfit = copyInt(n.GrossProfit)
	n.NetProfit = copyInt(n.NetProfit)
	n.MinProfit = copyInt(n.MinProfit)
	n.GapToProfit = copyInt(n.GapToProfit)
	n.AavePremium = copyInt(n.AavePremium)
	n.L1Fee = copyInt(n.L1Fee)
	n.L2Fee = copyInt(n.L2Fee)
	if len(r.Symbols) == 0 {
		return
	}
	t := m.Tokens[r.Symbols[0]]
	o := Observation{RouteKey(r, m), block, n, Asset{t.Address, t.Decimals}}
	select {
	case s.observations <- o:
	default:
		s.dropped.Add(1)
	}
}
func copyInt(x *big.Int) *big.Int {
	if x == nil {
		return nil
	}
	return new(big.Int).Set(x)
}
func (s *Service) Offer(in Input) {
	if s == nil {
		return
	}
	// Detach mutable maps and big integers while the market cycle owns them.
	m := in.Market
	m.Tokens = map[string]config.Token{}
	for k, t := range in.Market.Tokens {
		m.Tokens[k] = t
	}
	m.ExecutionAssetNames = append([]string(nil), in.Market.ExecutionAssets()...)
	m.MarketAssetNames = append([]string(nil), in.Market.MarketAssets()...)
	in.Market = m
	routesCopy := make([]routes.Route, len(in.Routes))
	for i, r := range in.Routes {
		r.Symbols = append([]string(nil), r.Symbols...)
		r.Hops = append([]pools.Pool(nil), r.Hops...)
		for j := range r.Hops {
			r.Hops[j].Liquidity = copyInt(r.Hops[j].Liquidity)
			r.Hops[j].SqrtPriceX96 = copyInt(r.Hops[j].SqrtPriceX96)
		}
		routesCopy[i] = r
	}
	in.Routes = routesCopy
	a := map[string]*big.Int{}
	for k, v := range in.Amounts {
		a[k] = copyInt(v)
	}
	in.Amounts = a
	in.Dirty = copySet(in.Dirty)
	in.Unresolved = copySet(in.Unresolved)
	select {
	case s.in <- in:
		return
	default:
	}
	select {
	case <-s.in:
		s.dropped.Add(1)
	default:
	}
	select {
	case s.in <- in:
	default:
		s.dropped.Add(1)
	}
}
func copySet(in map[string]bool) map[string]bool {
	o := map[string]bool{}
	for k, v := range in {
		o[strings.ToLower(k)] = v
	}
	return o
}

func (s *Service) Run(ctx context.Context) {
	if s == nil || s.pairs == nil {
		return
	}
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		s.writeErrors.Add(1)
		return
	}
	s.loadHistory()
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	var latest Input
	for {
		select {
		case <-ctx.Done():
			s.saveHistory()
			return
		case in := <-s.in:
			latest = in
		case o := <-s.observations:
			if o.Key == "" {
				continue
			}
			s.history[o.Key] = UpdateHistory(s.history[o.Key], o.Record, time.Now().UTC())
			s.prune()
			if o.Record.QuoteSuccessful && o.Record.AmountIn != nil && o.Record.AmountOut != nil && o.Record.AavePremium != nil {
				portfolio := EvaluatePortfolio(PortfolioInput{Funding: FundingState{Borrowed: []Balance{{o.Asset, o.Record.AmountIn}}}, Liabilities: []Liability{{o.Asset, o.Record.AmountIn, o.Record.AavePremium}}, After: []Balance{{o.Asset, o.Record.AmountOut}}, Valuation: ValuationState{Numeraire: "USD", Now: time.Now().UTC(), MaxAge: time.Minute}})
				s.write("shadow_portfolio_replay", map[string]any{"route_key": o.Key, "block": o.Block, "portfolio": portfolio, "cost_status": "UNKNOWN: live token-denominated gas not independently validated", "simulation_status": "NOT_REQUESTED"})
			}
			s.write("current_evaluation", map[string]any{"route_key": o.Key, "block": o.Block, "route": o.Record.Route, "dex_path": o.Record.DEXPath, "hops": o.Record.HopCount, "funding_asset": o.Record.LoanAsset, "amount_in": raw(o.Record.AmountIn), "amount_out": raw(o.Record.AmountOut), "gross_raw": raw(o.Record.GrossProfit), "legacy_net_raw": raw(o.Record.NetProfit), "min_profit_raw": raw(o.Record.MinProfit), "legacy_l1_raw": raw(o.Record.L1Fee), "legacy_l2_raw": raw(o.Record.L2Fee), "premium_raw": raw(o.Record.AavePremium), "roundtrip_loss_bps": o.Record.PriceImpactBPS, "actual_price_impact_bps": "UNKNOWN", "reason": o.Record.RejectionReason, "quote_success": o.Record.QuoteSuccessful})
		case <-tick.C:
			if latest.Block == 0 {
				s.write("shadow_waiting", map[string]any{"reason": "no completed market snapshot"})
				continue
			}
			s.analyze(ctx, latest)
			s.saveHistory()
		}
	}
}
func raw(x *big.Int) string {
	if x == nil {
		return "UNKNOWN"
	}
	return x.String()
}
func (s *Service) analyze(ctx context.Context, in Input) {
	now := time.Now().UTC()
	start := time.Now()
	ps := s.pairs.Pairs()
	quality := make([]PairQuality, 0, len(ps))
	for _, p := range ps {
		quality = append(quality, PairScore(p, now))
	}
	all, market := shadowRoutes(in, ps, s.pairs.Tokens(), quality)
	ranked := Rank(all, market, quality, s.history, in.Dirty, now)
	for i := range ranked {
		ranked[i].Observations += s.shadowSeen[ranked[i].Key]
	}
	selected := Select(ranked, 8, s.round)
	s.round++
	proposals := []map[string]any{}
	for _, r := range selected {
		amounts := AmountLadder(in.Amounts[r.Route.Symbols[0]], firstDepthCap(r.Route, market, ps, now))
		ladder := []string{}
		for _, a := range amounts {
			ladder = append(ladder, a.String())
		}
		proposals = append(proposals, map[string]any{"route_key": r.Key, "route": r.Route.String(), "hops": len(r.Route.Hops), "score": r.Score, "pair_quality": r.PairQuality, "pool_quality": r.PoolQuality, "history": r.HistoryContribution, "bucket": r.Bucket, "reasons": r.Reasons, "amount_ladder_raw": ladder, "execution_authority": false})
	}
	poolset := map[string]bool{}
	tokens := map[string]bool{}
	for _, r := range all {
		for _, p := range r.Hops {
			poolset[strings.ToLower(p.Address)] = true
			tokens[strings.ToLower(p.Token0)] = true
			tokens[strings.ToLower(p.Token1)] = true
		}
	}
	q := map[string]any{"timestamp": now, "mode": "shadow", "execution_mode": "ATOMIC_RISK_FREE", "inventory_execution_enabled": false, "pairs": quality, "tracked_tokens": len(s.pairs.Tokens()), "candidate_routes": len(all), "live_routes": len(in.Routes), "covered_pools": len(poolset), "covered_tokens": len(tokens), "proposals": proposals, "valuation_policy": "USD; independent fresh executable liquidation bounds required; unknown prices never assumed $1", "elapsed_ms": time.Since(start).Milliseconds(), "events_dropped": s.dropped.Load(), "write_errors": s.writeErrors.Load()}
	s.saveJSON("TITANARB_PHASE2_MARKET_QUALITY.json", q)
	s.write("shadow_comparison", q)
	if s.ProbeEnabled && s.client != nil && len(selected) > 0 {
		// Rotate proposed routes and sizes. At most ONE route/size probe per minute.
		r := selected[int((s.round-1)%uint64(len(selected)))]
		ladder := AmountLadder(in.Amounts[r.Route.Symbols[0]], firstDepthCap(r.Route, market, ps, now))
		if len(ladder) > 0 {
			s.rememberShadow(r.Key)
			s.probe(ctx, in, market, r, ladder[int((s.round-1)%uint64(len(ladder)))])
		}
	}
}

func (s *Service) rememberShadow(key string) {
	if _, ok := s.shadowSeen[key]; !ok && len(s.shadowSeen) >= 4096 {
		// Sampling history has a hard bound even if the market graph churns.
		old := ""
		for k := range s.shadowSeen {
			if old == "" || k < old {
				old = k
			}
		}
		delete(s.shadowSeen, old)
	}
	s.shadowSeen[key]++
}

func firstDepthCap(r routes.Route, m config.MarketConfig, ps []pairintel.Pair, now time.Time) *big.Int {
	if len(r.Hops) == 0 || len(r.Symbols) == 0 {
		return nil
	}
	pool := r.Hops[0]
	in := strings.ToLower(m.Tokens[r.Symbols[0]].Address)
	direction := "0to1"
	if !strings.EqualFold(pool.Token0, in) {
		direction = "1to0"
	}
	var best *big.Int
	for _, p := range ps {
		for _, v := range p.Venues {
			if !strings.EqualFold(v.Pool, pool.Address) {
				continue
			}
			for _, d := range v.Depth {
				if d.Direction != direction || !d.Successful || d.Score < .5 || d.ObservedAt.After(now) || now.Sub(d.ObservedAt) > 5*time.Minute {
					continue
				}
				a, ok := new(big.Int).SetString(d.AmountIn, 10)
				if ok && a.Sign() > 0 && (best == nil || a.Cmp(best) > 0) {
					best = a
				}
			}
		}
	}
	return best
}

func shadowRoutes(in Input, ps []pairintel.Pair, ts []pairintel.TokenMeta, quality []PairQuality) ([]routes.Route, config.MarketConfig) {
	m := in.Market
	m.Tokens = map[string]config.Token{}
	byAddress := map[string]string{}
	names := make([]string, 0, len(in.Market.Tokens))
	for n := range in.Market.Tokens {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		t := in.Market.Tokens[n]
		m.Tokens[n] = t
		if _, ok := byAddress[strings.ToLower(t.Address)]; !ok {
			byAddress[strings.ToLower(t.Address)] = n
		}
	}
	for _, t := range ts {
		if !t.HasCode || t.Decimals > 36 {
			continue
		}
		if _, ok := byAddress[t.Address]; ok {
			continue
		}
		n := pairintel.DynamicSymbol(t.Address)
		if n == "" {
			continue
		}
		m.Tokens[n] = config.Token{Symbol: n, Address: t.Address, Decimals: t.Decimals}
		byAddress[t.Address] = n
	}
	approved := map[string]bool{}
	for _, n := range in.Market.ExecutionAssets() {
		approved[strings.ToLower(in.Market.Tokens[n].Address)] = true
	}
	qualified := map[string]bool{}
	for _, q := range quality {
		qualified[q.Key] = q.ShadowAdmitted
	}
	out := append([]routes.Route(nil), in.Routes...)
	seen := map[string]bool{}
	for _, r := range out {
		seen[RouteKey(r, m)] = true
	}
	for _, p := range ps {
		if !qualified[p.Key.String()] {
			continue
		}
		for _, start := range []string{p.Key.Token0, p.Key.Token1} {
			if !approved[start] {
				continue
			}
			end := p.Key.Token0
			if end == start {
				end = p.Key.Token1
			}
			a, b := byAddress[start], byAddress[end]
			if a == "" || b == "" {
				continue
			}
			venues := []pairintel.Venue{}
			for _, v := range p.Venues {
				if v.DEX == pools.UniswapV3 || v.DEX == pools.CamelotV3 {
					venues = append(venues, v)
				}
			}
			sort.Slice(venues, func(i, j int) bool { return venues[i].Pool < venues[j].Pool })
			for _, v := range venues {
				for _, w := range venues {
					if v.DEX == w.DEX || v.Pool == w.Pool || in.Unresolved[strings.ToLower(v.Pool)] || in.Unresolved[strings.ToLower(w.Pool)] {
						continue
					}
					makePool := func(v pairintel.Venue) pools.Pool {
						return pools.Pool{Address: v.Pool, Token0: p.Key.Token0, Token1: p.Key.Token1, DEX: v.DEX, Fee: v.Fee, LastUpdatedBlock: v.LastUpdatedBlock}
					}
					r := routes.Route{Symbols: []string{a, b, a}, Hops: []pools.Pool{makePool(v), makePool(w)}}
					k := RouteKey(r, m)
					if !seen[k] && len(out) < 512 {
						out = append(out, r)
						seen[k] = true
					}
				}
			}
		}
	}
	// Never analyze fresh reads through known unresolved pools, including live routes.
	clean := out[:0]
	for _, r := range out {
		ok := true
		for _, p := range r.Hops {
			if in.Unresolved[strings.ToLower(p.Address)] {
				ok = false
			}
		}
		if ok {
			clean = append(clean, r)
		}
	}
	return clean, m
}

// pinnedReader preserves a single historical block for the entire shadow quote.
// It shares the production provider manager/global budgets, uses Background
// priority, and adds a stricter shadow-only bound. It cannot submit transactions.
type pinnedReader struct {
	s     *Service
	block uint64
	calls uint64
}

func (p *pinnedReader) EthCall(ctx context.Context, call map[string]string) (string, error) {
	now := time.Now()
	if p.s.minute.IsZero() || now.Sub(p.s.minute) >= time.Minute {
		p.s.minute = now
		p.s.reads = 0
	}
	limit := p.s.ReadLimit
	if limit < 1 || limit > 12 {
		limit = 12
	}
	if p.s.reads >= uint64(limit) {
		return "", fmt.Errorf("shadow read budget exhausted")
	}
	// Yield immediately instead of competing with a live cycle.
	if p.s.busy != nil && p.s.busy() {
		return "", fmt.Errorf("shadow yielded to live work")
	}
	if wait := 500*time.Millisecond - time.Since(p.s.lastRead); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p.s.busy != nil && p.s.busy() {
		return "", fmt.Errorf("shadow yielded to live work")
	}
	p.s.reads++
	p.calls++
	p.s.lastRead = time.Now()
	ctx = rpc.WithRequestMetadata(rpc.WithRequestClass(ctx, rpc.Background), "phase2_shadow", p.block)
	return p.s.client.EthCallAt(ctx, call, fmt.Sprintf("0x%x", p.block))
}
func (s *Service) probe(parent context.Context, in Input, m config.MarketConfig, r RankedRoute, amount *big.Int) {
	ctx, cancel := context.WithTimeout(parent, 4*time.Second)
	defer cancel()
	p := &pinnedReader{s: s, block: in.Block}
	uni := quotes.NewUniswapV3(p, m.UniswapQuoterV2, nil)
	camel := quotes.NewCamelot(p, m.CamelotQuoter, nil)
	start := time.Now()
	current := new(big.Int).Set(amount)
	f := map[string]any{"route_key": r.Key, "route": r.Route.String(), "block": in.Block, "amount_in": amount.String(), "bucket": r.Bucket, "score": r.Score, "quote_success": false, "net_raw": "UNKNOWN", "net_usd": "UNKNOWN", "execution_eligible": false, "reason": "incomplete quote", "mode": "shadow"}
	defer func() {
		f["rpc_calls"] = p.calls
		f["duration_ms"] = time.Since(start).Milliseconds()
		s.write("shadow_quote", f)
	}()
	for i, pool := range r.Route.Hops {
		var q quotes.Quoter
		switch pool.DEX {
		case pools.UniswapV3:
			q = uni
		case pools.CamelotV3:
			q = camel
		default:
			f["reason"] = "unsupported adapter"
			return
		}
		result, err := q.Quote(ctx, quotes.Request{TokenIn: m.Tokens[r.Route.Symbols[i]].Address, TokenOut: m.Tokens[r.Route.Symbols[i+1]].Address, AmountIn: current, Pool: pool})
		if err != nil {
			f["reason"] = safeFailure(err)
			return
		}
		if result.AmountOut == nil || result.AmountOut.Sign() <= 0 {
			f["reason"] = "nonpositive output"
			return
		}
		current = new(big.Int).Set(result.AmountOut)
	}
	gross := new(big.Int).Sub(current, amount)
	f["quote_success"] = true
	f["amount_out"] = current.String()
	f["gross_raw"] = gross.String()
	f["gross_bps"] = bps(gross, amount)
	f["positive_gross"] = gross.Sign() > 0
	// No fee rescaling from funding-token decimals: USD fees are not token fees.
	// Keep net UNKNOWN until independent denomination/liquidation evidence exists.
	f["reason"] = "gross quote only; independent cost/valuation evidence required"
	rawPremium, err := p.EthCall(ctx, map[string]string{"to": m.AavePool, "data": dex.StaticCall("FLASHLOAN_PREMIUM_TOTAL()")})
	if err != nil {
		f["premium_status"] = safeFailure(err)
		return
	}
	words, err := dex.DecodeWords(rawPremium)
	if err != nil || len(words) != 1 {
		f["premium_status"] = "invalid response"
		return
	}
	premium := new(big.Int).Quo(new(big.Int).Mul(amount, dex.WordUint(words[0])), big.NewInt(10000))
	f["premium_raw"] = premium.String()
	f["after_premium_raw"] = new(big.Int).Sub(gross, premium).String()
	f["repayment_covered_before_gas"] = current.Cmp(new(big.Int).Add(amount, premium)) >= 0
	if len(r.Route.Hops) == 2 && current.Cmp(new(big.Int).Add(amount, premium)) > 0 {
		// Residual-inventory analysis requires an ACTUAL partial close-out quote.
		// Never infer it by prorating the full-swap output. Costs/valuation remain
		// unknown, and this path cannot emit an execution opportunity.
		f["residual_analysis"] = "requires partial close-out quote and independent liquidation valuation"
	}
}
func safeFailure(err error) string {
	if err == nil {
		return ""
	}
	if err == context.Canceled {
		return "canceled"
	}
	if err == context.DeadlineExceeded {
		return "deadline"
	}
	x := strings.ToLower(err.Error())
	for _, c := range []string{"shadow yielded", "shadow read budget", "context canceled", "deadline exceeded", "execution reverted", "rate limit", "budget exhausted"} {
		if strings.Contains(x, c) {
			return c
		}
	}
	return "read/quote failed (details in sanitized RPC telemetry)"
}
func (s *Service) prune() {
	for len(s.history) > 4096 {
		old := ""
		var t time.Time
		for k, h := range s.history {
			if old == "" || h.LastSeen.Before(t) || (h.LastSeen.Equal(t) && k < old) {
				old = k
				t = h.LastSeen
			}
		}
		delete(s.history, old)
	}
}
func (s *Service) saveJSON(name string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		s.writeErrors.Add(1)
		return
	}
	path := filepath.Join(s.dir, name)
	if err = os.WriteFile(path+".tmp", b, 0600); err == nil {
		err = os.Rename(path+".tmp", path)
	}
	if err != nil {
		s.writeErrors.Add(1)
	}
}
func (s *Service) saveHistory() { s.saveJSON("phase2_history.json", s.history) }
func (s *Service) loadHistory() {
	b, err := os.ReadFile(filepath.Join(s.dir, "phase2_history.json"))
	if err != nil || len(b) > 8<<20 {
		return
	}
	h := map[string]History{}
	if json.Unmarshal(b, &h) == nil {
		s.history = h
		s.prune()
	}
}
func (s *Service) write(event string, f map[string]any) {
	// All fields are local numeric data, canonical identities or fixed reasons.
	b, err := json.Marshal(map[string]any{"timestamp": time.Now().UTC(), "event": event, "fields": f})
	if err != nil {
		s.writeErrors.Add(1)
		return
	}
	path := filepath.Join(s.dir, "TITANARB_PHASE2_SHADOW_DECISIONS.jsonl")
	if stat, err := os.Stat(path); err == nil && stat.Size() > 32<<20 {
		if err = os.Rename(path, path+".1"); err != nil {
			s.writeErrors.Add(1)
			return
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		s.writeErrors.Add(1)
		return
	}
	_, err = file.Write(append(b, '\n'))
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		s.writeErrors.Add(1)
	}
}
