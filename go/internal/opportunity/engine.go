// Package opportunity evaluates bounded market routes without execution authority.
package opportunity

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/fees"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/pricing"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/routes"
)

type EventType string

const (
	Found       EventType = "opportunity_found"
	Rejected    EventType = "opportunity_rejected"
	QuoteFailed EventType = "quote_failed"
	PoolUpdated EventType = "pool_updated"
)

type Hop struct {
	Pool                pools.Pool
	AmountIn, AmountOut *big.Int
	QuoteGas            uint64
	Fee                 uint32
}
type Opportunity struct {
	Route          routes.Route
	AmountIn       *big.Int
	AmountOut      *big.Int
	Hops           []Hop
	SourceBlock    uint64
	GrossProfit    *big.Int
	AavePremium    *big.Int
	ExpectedProfit *big.Int
	GasEstimate    *big.Int
	L1DataFee      *big.Int
	MinProfit      *big.Int
	Confidence     string
	Timestamp      time.Time
}
type Event struct {
	Type        EventType
	Opportunity *Opportunity
	Reason      string
	Timestamp   time.Time
}

type Caller interface {
	EthCall(context.Context, map[string]string) (string, error)
}
type CostModel interface {
	Estimate(context.Context, config.Token, routes.Route, []Hop) (l2, l1 *big.Int, err error)
}
type StaticCostModel struct{ L2Fee, L1Fee *big.Int }

func (s StaticCostModel) Estimate(_ context.Context, _ config.Token, _ routes.Route, _ []Hop) (*big.Int, *big.Int, error) {
	if s.L2Fee == nil || s.L1Fee == nil {
		return nil, nil, fmt.Errorf("L2 and L1/data fee estimates are required")
	}
	return new(big.Int).Set(s.L2Fee), new(big.Int).Set(s.L1Fee), nil
}

// ArbitrumCostModel mirrors Python's provisional 5M-gas, ABI-size route model
// but obtains current Arbitrum fee components from the authoritative services.
type ArbitrumCostModel struct {
	Service *fees.Service
}

func (m ArbitrumCostModel) Estimate(ctx context.Context, asset config.Token, _ routes.Route, hops []Hop) (*big.Int, *big.Int, error) {
	payload := make([]byte, 4+160+256*len(hops))
	estimate, err := m.Service.Estimate(ctx, 5_000_000, payload)
	if err != nil {
		return nil, nil, err
	}
	return feeToBaseRaw(estimate.L2Cost, estimate.TotalUSD, estimate.TotalETH, asset.Decimals), feeToBaseRaw(estimate.L1Cost, estimate.TotalUSD, estimate.TotalETH, asset.Decimals), nil
}
func feeToBaseRaw(component *big.Int, totalUSD, totalETH *big.Rat, decimals uint8) *big.Int {
	if component.Sign() == 0 || totalETH.Sign() == 0 {
		return big.NewInt(0)
	}
	componentETH := new(big.Rat).SetFrac(component, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	price := new(big.Rat).Quo(totalUSD, totalETH)
	value := new(big.Rat).Mul(componentETH, price)
	value.Mul(value, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)))
	return new(big.Int).Quo(value.Num(), value.Denom())
}

type Engine struct {
	caller        Caller
	market        config.MarketConfig
	uni           quotes.Quoter
	camelot       quotes.Quoter
	cost          CostModel
	metrics       *metrics.Metrics
	minProfit     *big.Int
	workers       int
	Events        chan Event
	premiumMu     sync.Mutex
	premiumBPS    *big.Int
	premiumExpiry time.Time
}

func New(c Caller, market config.MarketConfig, uni, camelot quotes.Quoter, cost CostModel, minProfit *big.Int, workers int, m *metrics.Metrics) *Engine {
	if workers < 1 {
		workers = 1
	}
	// The route universe is bounded by the runtime. Keep one complete bounded
	// cycle of diagnostic events so a burst of routine rejections cannot hide a
	// later profitable candidate before the runner drains the cycle.
	return &Engine{caller: c, market: market, uni: uni, camelot: camelot, cost: cost, minProfit: new(big.Int).Set(minProfit), workers: workers, metrics: m, Events: make(chan Event, 4096)}
}

type cachedCostEntry struct {
	l2, l1 *big.Int
	until  time.Time
}

type CachedCostModel struct {
	Base CostModel
	TTL  time.Duration

	mu    sync.Mutex
	cache map[string]cachedCostEntry
}

func NewCachedCostModel(base CostModel, ttl time.Duration) *CachedCostModel {
	return &CachedCostModel{Base: base, TTL: ttl, cache: make(map[string]cachedCostEntry)}
}

func (m *CachedCostModel) Estimate(ctx context.Context, asset config.Token, route routes.Route, hops []Hop) (*big.Int, *big.Int, error) {
	if m == nil || m.Base == nil {
		return nil, nil, fmt.Errorf("cached cost model requires a base model")
	}
	if m.TTL <= 0 {
		return m.Base.Estimate(ctx, asset, route, hops)
	}
	key := fmt.Sprintf("%s:%d:%d", asset.Address, asset.Decimals, len(hops))
	now := time.Now()

	m.mu.Lock()
	if entry, ok := m.cache[key]; ok && now.Before(entry.until) {
		l2 := new(big.Int).Set(entry.l2)
		l1 := new(big.Int).Set(entry.l1)
		m.mu.Unlock()
		return l2, l1, nil
	}
	m.mu.Unlock()

	l2, l1, err := m.Base.Estimate(ctx, asset, route, hops)
	if err != nil {
		return nil, nil, err
	}

	m.mu.Lock()
	m.cache[key] = cachedCostEntry{l2: new(big.Int).Set(l2), l1: new(big.Int).Set(l1), until: now.Add(m.TTL)}
	m.mu.Unlock()
	return l2, l1, nil
}

// EvaluateSilent performs the same economic checks as Evaluate but never emits
// operator events. It is intended for optimizer sampling where intermediate
// sizes must not become execution candidates.
func (e *Engine) EvaluateSilent(ctx context.Context, route routes.Route, amount *big.Int) (*Opportunity, error) {
	if len(route.Hops) < 2 || len(route.Hops) > 4 || len(route.Symbols) != len(route.Hops)+1 {
		return nil, fmt.Errorf("invalid bounded route")
	}

	asset, ok := e.market.Tokens[route.Symbols[0]]
	if !ok || route.Symbols[len(route.Symbols)-1] != route.Symbols[0] {
		return nil, fmt.Errorf("route must start and end in configured loan asset")
	}

	current := new(big.Int).Set(amount)
	hops := make([]Hop, 0, len(route.Hops))

	for i, pool := range route.Hops {
		in, ok := e.market.Tokens[route.Symbols[i]]
		if !ok {
			return nil, fmt.Errorf("unknown token %s", route.Symbols[i])
		}

		out, ok := e.market.Tokens[route.Symbols[i+1]]
		if !ok {
			return nil, fmt.Errorf("unknown token %s", route.Symbols[i+1])
		}

		if !pool.Supports(in.Address, out.Address) {
			return nil, fmt.Errorf("pool does not support %s", route.String())
		}

		var quoter quotes.Quoter
		switch pool.DEX {
		case pools.UniswapV3:
			quoter = e.uni
		case pools.CamelotV3:
			quoter = e.camelot
		default:
			return nil, fmt.Errorf("unsupported DEX")
		}

		result, err := quoter.Quote(ctx, quotes.Request{
			TokenIn:  in.Address,
			TokenOut: out.Address,
			AmountIn: current,
			Pool:     pool,
		})
		if err != nil {
			return nil, err
		}

		hops = append(hops, Hop{
			Pool:      pool,
			AmountIn:  new(big.Int).Set(current),
			AmountOut: new(big.Int).Set(result.AmountOut),
			QuoteGas:  result.EstimatedGas,
			Fee:       result.Fee,
		})

		current = result.AmountOut
	}

	premium, err := e.aavePremium(ctx, amount)
	if err != nil {
		return nil, err
	}

	l2, l1, err := e.cost.Estimate(ctx, asset, route, hops)
	if err != nil {
		return nil, err
	}

	result := pricing.Evaluate(pricing.Inputs{
		AmountIn:    amount,
		AmountOut:   current,
		AavePremium: premium,
		L2Fee:       l2,
		L1DataFee:   l1,
		MinProfit:   e.minProfit,
	})

	return &Opportunity{
		Route:          route,
		AmountIn:       new(big.Int).Set(amount),
		AmountOut:      new(big.Int).Set(current),
		Hops:           hops,
		GrossProfit:    new(big.Int).Set(result.GrossProfit),
		AavePremium:    new(big.Int).Set(premium),
		ExpectedProfit: result.ExpectedProfit,
		GasEstimate:    l2,
		L1DataFee:      l1,
		MinProfit:      new(big.Int).Set(e.minProfit),
		Confidence:     "optimizer_sample",
		Timestamp:      time.Now().UTC(),
	}, nil
}

func (e *Engine) Evaluate(ctx context.Context, route routes.Route, amount *big.Int) (*Opportunity, error) {
	if len(route.Hops) < 2 || len(route.Hops) > 4 || len(route.Symbols) != len(route.Hops)+1 {
		return nil, fmt.Errorf("invalid bounded route")
	}
	asset, ok := e.market.Tokens[route.Symbols[0]]
	if !ok || route.Symbols[len(route.Symbols)-1] != route.Symbols[0] {
		return nil, fmt.Errorf("route must start and end in a configured loan asset")
	}
	current := new(big.Int).Set(amount)
	hops := make([]Hop, 0, len(route.Hops))
	for i, pool := range route.Hops {
		in, ok := e.market.Tokens[route.Symbols[i]]
		if !ok {
			return nil, fmt.Errorf("unknown token %s", route.Symbols[i])
		}
		out, ok := e.market.Tokens[route.Symbols[i+1]]
		if !ok {
			return nil, fmt.Errorf("unknown token %s", route.Symbols[i+1])
		}
		if !pool.Supports(in.Address, out.Address) {
			return nil, fmt.Errorf("pool does not support %s", route.String())
		}
		var quoter quotes.Quoter
		if pool.DEX == pools.UniswapV3 {
			quoter = e.uni
		} else if pool.DEX == pools.CamelotV3 {
			quoter = e.camelot
		} else {
			return nil, fmt.Errorf("unsupported DEX")
		}
		quote, err := quoter.Quote(ctx, quotes.Request{TokenIn: in.Address, TokenOut: out.Address, AmountIn: current, Pool: pool})
		if err != nil {
			e.emit(Event{Type: QuoteFailed, Reason: err.Error(), Timestamp: time.Now().UTC()})
			return nil, err
		}
		hops = append(hops, Hop{Pool: pool, AmountIn: new(big.Int).Set(current), AmountOut: quote.AmountOut, QuoteGas: quote.EstimatedGas, Fee: quote.Fee})
		current = quote.AmountOut
	}
	premium, err := e.aavePremium(ctx, amount)
	if err != nil {
		return nil, err
	}
	l2, l1, err := e.cost.Estimate(ctx, asset, route, hops)
	if err != nil {
		return nil, err
	}
	result := pricing.Evaluate(pricing.Inputs{AmountIn: amount, AmountOut: current, AavePremium: premium, L2Fee: l2, L1DataFee: l1, MinProfit: e.minProfit})
	opportunity := &Opportunity{
		Route:          route,
		AmountIn:       new(big.Int).Set(amount),
		AmountOut:      new(big.Int).Set(current),
		Hops:           hops,
		SourceBlock:    routeSourceBlock(route),
		GrossProfit:    new(big.Int).Set(result.GrossProfit),
		AavePremium:    new(big.Int).Set(premium),
		ExpectedProfit: result.ExpectedProfit,
		GasEstimate:    l2,
		L1DataFee:      l1,
		MinProfit:      new(big.Int).Set(e.minProfit),
		Confidence:     "pre_simulation",
		Timestamp:      time.Now().UTC(),
	}
	if e.metrics != nil {
		e.metrics.IncRoutesEvaluated()
	}
	if result.Profitable {
		if e.metrics != nil {
			e.metrics.IncOpportunities()
		}
		e.emit(Event{Type: Found, Opportunity: opportunity, Timestamp: opportunity.Timestamp})
	} else {
		e.emit(Event{Type: Rejected, Opportunity: opportunity, Reason: "profitability threshold not met", Timestamp: opportunity.Timestamp})
	}
	return opportunity, nil
}

func routeSourceBlock(route routes.Route) uint64 {
	var source uint64
	for _, pool := range route.Hops {
		if pool.LastUpdatedBlock == 0 {
			continue
		}
		if source == 0 || pool.LastUpdatedBlock < source {
			source = pool.LastUpdatedBlock
		}
	}
	return source
}

func (e *Engine) EvaluateMany(ctx context.Context, candidates []routes.Route, amount *big.Int) {
	jobs := make(chan routes.Route)
	var wg sync.WaitGroup
	for i := 0; i < e.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for route := range jobs {
				_, _ = e.Evaluate(ctx, route, amount)
			}
		}()
	}
	for _, route := range candidates {
		select {
		case jobs <- route:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		}
	}
	close(jobs)
	wg.Wait()
}

func (e *Engine) aavePremium(ctx context.Context, amount *big.Int) (*big.Int, error) {
	bps, err := e.aavePremiumBPS(ctx)
	if err != nil {
		return nil, err
	}
	return new(big.Int).Div(new(big.Int).Mul(amount, bps), big.NewInt(10_000)), nil
}

func (e *Engine) aavePremiumBPS(ctx context.Context) (*big.Int, error) {
	now := time.Now()
	e.premiumMu.Lock()
	if e.premiumBPS != nil && now.Before(e.premiumExpiry) {
		bps := new(big.Int).Set(e.premiumBPS)
		e.premiumMu.Unlock()
		return bps, nil
	}
	e.premiumMu.Unlock()

	raw, err := e.caller.EthCall(ctx, map[string]string{"to": e.market.AavePool, "data": dex.StaticCall("FLASHLOAN_PREMIUM_TOTAL()")})
	if err != nil {
		return nil, err
	}
	words, err := dex.DecodeWords(raw)
	if err != nil {
		return nil, err
	}
	bps := dex.WordUint(words[0])

	e.premiumMu.Lock()
	e.premiumBPS = new(big.Int).Set(bps)
	e.premiumExpiry = now.Add(10 * time.Second)
	e.premiumMu.Unlock()
	return bps, nil
}
func (e *Engine) emit(event Event) {
	select {
	case e.Events <- event:
	default:
	}
}
