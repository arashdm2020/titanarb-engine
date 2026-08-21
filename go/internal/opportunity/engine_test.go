package opportunity

import (
	"context"
	"encoding/hex"
	"math/big"
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

type quoter struct{}

func (quoter) Quote(_ context.Context, r quotes.Request) (quotes.Result, error) {
	return quotes.Result{AmountOut: new(big.Int).Add(r.AmountIn, big.NewInt(100))}, nil
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
