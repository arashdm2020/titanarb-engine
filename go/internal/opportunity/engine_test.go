package opportunity

import (
	"context"
	"encoding/hex"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/routes"
)

type caller struct{}

func (caller) EthCall(context.Context, map[string]string) (string, error) {
	return "0x" + hex.EncodeToString(dex.UintWord(big.NewInt(5))), nil
}

type countingCaller struct{ calls atomic.Uint64 }

func (c *countingCaller) EthCall(context.Context, map[string]string) (string, error) {
	c.calls.Add(1)
	return "0x" + hex.EncodeToString(dex.UintWord(big.NewInt(5))), nil
}

type countingCost struct{ calls atomic.Uint64 }

func (c *countingCost) Estimate(context.Context, config.Token, routes.Route, []Hop) (*big.Int, *big.Int, error) {
	c.calls.Add(1)
	return big.NewInt(10), big.NewInt(20), nil
}

type quoter struct{}

func (quoter) Quote(_ context.Context, r quotes.Request) (quotes.Result, error) {
	return quotes.Result{AmountOut: new(big.Int).Add(r.AmountIn, big.NewInt(100))}, nil
}

type countingQuoter struct {
	calls atomic.Uint64
	delay time.Duration
}

func (q *countingQuoter) Quote(ctx context.Context, r quotes.Request) (quotes.Result, error) {
	q.calls.Add(1)
	if q.delay > 0 {
		select {
		case <-time.After(q.delay):
		case <-ctx.Done():
			return quotes.Result{}, ctx.Err()
		}
	}
	return quotes.Result{AmountOut: new(big.Int).Add(r.AmountIn, big.NewInt(100)), Fee: r.Pool.Fee}, nil
}
func token(symbol, address string) config.Token {
	return config.Token{Symbol: symbol, Address: address, Decimals: 6}
}
func TestEvaluateAndWorkerShutdown(t *testing.T) {
	a := "0x0000000000000000000000000000000000000001"
	b := "0x0000000000000000000000000000000000000002"
	market := config.MarketConfig{AavePool: "0x0000000000000000000000000000000000000003", Tokens: map[string]config.Token{"USDC": token("USDC", a), "WETH": token("WETH", b)}}
	p := pools.Pool{Address: "p", Token0: a, Token1: b, DEX: pools.UniswapV3, Fee: 500, Liquidity: big.NewInt(1)}
	back := p
	route := routes.Route{Symbols: []string{"USDC", "WETH", "USDC"}, Hops: []pools.Pool{p, back}}
	e := New(caller{}, market, quoter{}, quoter{}, StaticCostModel{L2Fee: big.NewInt(1), L1Fee: big.NewInt(1)}, big.NewInt(1), 2, nil)
	got, err := e.Evaluate(context.Background(), route, big.NewInt(1000))
	if err != nil || got.ExpectedProfit.Sign() <= 0 {
		t.Fatalf("opportunity=%+v err=%v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { e.EvaluateMany(ctx, []routes.Route{route}, big.NewInt(1000)); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop")
	}
}

func TestAavePremiumIsShortLivedCached(t *testing.T) {
	a := "0x0000000000000000000000000000000000000001"
	b := "0x0000000000000000000000000000000000000002"
	market := config.MarketConfig{AavePool: "0x0000000000000000000000000000000000000003", Tokens: map[string]config.Token{"USDC": token("USDC", a), "WETH": token("WETH", b)}}
	p := pools.Pool{Address: "p", Token0: a, Token1: b, DEX: pools.UniswapV3, Fee: 500, Liquidity: big.NewInt(1)}
	route := routes.Route{Symbols: []string{"USDC", "WETH", "USDC"}, Hops: []pools.Pool{p, p}}
	c := &countingCaller{}
	e := New(c, market, quoter{}, quoter{}, StaticCostModel{L2Fee: big.NewInt(1), L1Fee: big.NewInt(1)}, big.NewInt(1), 1, nil)
	for i := 0; i < 3; i++ {
		if _, err := e.EvaluateSilent(context.Background(), route, big.NewInt(1000)); err != nil {
			t.Fatal(err)
		}
	}
	if got := c.calls.Load(); got != 1 {
		t.Fatalf("premium bps was not cached: calls=%d", got)
	}
}

func TestCachedCostModelCachesByAssetAndHopCount(t *testing.T) {
	base := &countingCost{}
	model := NewCachedCostModel(base, time.Minute)
	asset := token("USDC", "0x0000000000000000000000000000000000000001")
	route := routes.Route{Symbols: []string{"USDC", "WETH", "USDC"}, Hops: []pools.Pool{{}, {}}}
	if _, _, err := model.Estimate(context.Background(), asset, route, []Hop{{}, {}}); err != nil {
		t.Fatal(err)
	}
	l2, _, err := model.Estimate(context.Background(), asset, route, []Hop{{}, {}})
	if err != nil {
		t.Fatal(err)
	}
	l2.SetInt64(999)
	l2, _, err = model.Estimate(context.Background(), asset, route, []Hop{{}, {}})
	if err != nil {
		t.Fatal(err)
	}
	if l2.Cmp(big.NewInt(10)) != 0 {
		t.Fatalf("cached value was mutated by caller: %s", l2)
	}
	if got := base.calls.Load(); got != 1 {
		t.Fatalf("base cost model was not cached: calls=%d", got)
	}
}

func TestQuoteCacheReusesSameBlockRouteAmount(t *testing.T) {
	a := "0x0000000000000000000000000000000000000001"
	b := "0x0000000000000000000000000000000000000002"
	market := config.MarketConfig{AavePool: "0x0000000000000000000000000000000000000003", Tokens: map[string]config.Token{"USDC": token("USDC", a), "WETH": token("WETH", b)}}
	p := pools.Pool{Address: "0x00000000000000000000000000000000000000aa", Token0: a, Token1: b, DEX: pools.UniswapV3, Fee: 500, Liquidity: big.NewInt(1)}
	route := routes.Route{Symbols: []string{"USDC", "WETH", "USDC"}, Hops: []pools.Pool{p, p}}
	q := &countingQuoter{}
	e := New(caller{}, market, q, q, StaticCostModel{L2Fee: big.NewInt(1), L1Fee: big.NewInt(1)}, big.NewInt(1), 1, nil)
	e.ResetQuoteCache(123)

	if _, err := e.EvaluateSilent(context.Background(), route, big.NewInt(1000)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.EvaluateSilent(context.Background(), route, big.NewInt(1000)); err != nil {
		t.Fatal(err)
	}
	if got := q.calls.Load(); got != 2 {
		t.Fatalf("same route+amount+block was not cached: quote calls=%d", got)
	}
	stats := e.QuoteCacheStats()
	if stats.Hits == 0 {
		t.Fatalf("cache hit was not recorded: %+v", stats)
	}
}

func TestQuoteCacheDeduplicatesConcurrentSameBlockQuotes(t *testing.T) {
	a := "0x0000000000000000000000000000000000000001"
	b := "0x0000000000000000000000000000000000000002"
	market := config.MarketConfig{AavePool: "0x0000000000000000000000000000000000000003", Tokens: map[string]config.Token{"USDC": token("USDC", a), "WETH": token("WETH", b)}}
	p := pools.Pool{Address: "0x00000000000000000000000000000000000000bb", Token0: a, Token1: b, DEX: pools.UniswapV3, Fee: 500, Liquidity: big.NewInt(1)}
	route := routes.Route{Symbols: []string{"USDC", "WETH", "USDC"}, Hops: []pools.Pool{p, p}}
	q := &countingQuoter{delay: 20 * time.Millisecond}
	e := New(caller{}, market, q, q, StaticCostModel{L2Fee: big.NewInt(1), L1Fee: big.NewInt(1)}, big.NewInt(1), 2, nil)
	e.ResetQuoteCache(123)

	var done atomic.Uint64
	for i := 0; i < 2; i++ {
		go func() {
			if _, err := e.EvaluateSilent(context.Background(), route, big.NewInt(1000)); err != nil {
				t.Errorf("evaluate failed: %v", err)
			}
			done.Add(1)
		}()
	}
	deadline := time.After(time.Second)
	for done.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("concurrent evaluations did not finish")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if got := q.calls.Load(); got != 2 {
		t.Fatalf("concurrent duplicate route was not deduped: quote calls=%d", got)
	}
	stats := e.QuoteCacheStats()
	if stats.DedupHits == 0 {
		t.Fatalf("dedupe hit was not recorded: %+v", stats)
	}
}
