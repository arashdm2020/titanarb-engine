package alpha

import (
	"context"
	"math/big"
	"testing"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/graph"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/risk"
)

type discovery struct{ found map[string][]pools.Pool }

func (d discovery) DiscoverPair(_ context.Context, a, b string) ([]pools.Pool, error) {
	return d.found[a+b], nil
}

type quoter struct{}

func (quoter) Quote(_ context.Context, req quotes.Request) (quotes.Result, error) {
	return quotes.Result{AmountOut: new(big.Int).Add(req.AmountIn, big.NewInt(100)), Fee: req.Pool.Fee}, nil
}

type premiums struct{}

func (premiums) Premium(_ context.Context, _ *big.Int) (*big.Int, error) { return big.NewInt(5), nil }

type costs struct{}

func (costs) Estimate(_ context.Context, _ config.Token, _ graph.Route, _ []Hop) (*big.Int, *big.Int, error) {
	return big.NewInt(5), big.NewInt(5), nil
}

func TestGraphQuoteAndProfitAreAssetAgnostic(t *testing.T) {
	tokens := map[string]config.Token{"A": {Address: "0x000000000000000000000000000000000000000a"}, "B": {Address: "0x000000000000000000000000000000000000000b"}, "C": {Address: "0x000000000000000000000000000000000000000c"}}
	pool := func(id, a, b string) pools.Pool {
		return pools.Pool{Address: id, Token0: a, Token1: b, DEX: pools.UniswapV3, Fee: 500, Liquidity: big.NewInt(1)}
	}
	d := discovery{found: map[string][]pools.Pool{tokens["A"].Address + tokens["B"].Address: {pool("0x0000000000000000000000000000000000000001", tokens["A"].Address, tokens["B"].Address)}, tokens["A"].Address + tokens["C"].Address: {pool("0x0000000000000000000000000000000000000002", tokens["A"].Address, tokens["C"].Address)}, tokens["B"].Address + tokens["C"].Address: {pool("0x0000000000000000000000000000000000000003", tokens["B"].Address, tokens["C"].Address)}}}
	e := New(tokens, d, quoter{}, nil, 2)
	stats, err := e.Refresh(context.Background(), 4, 10)
	if err != nil || stats.Pools != 3 {
		t.Fatalf("refresh=%+v err=%v", stats, err)
	}
	var route graph.Route
	for _, candidate := range e.Routes(4, 10) {
		if candidate.Assets[0] == "B" && len(candidate.Hops) == 3 {
			route = candidate
			break
		}
	}
	if len(route.Hops) != 3 {
		t.Fatal("B was not evaluated as an equal loan asset")
	}
	chain, err := e.Quote(context.Background(), "B", route, big.NewInt(100))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := e.Evaluate(context.Background(), chain, premiums{}, costs{}, big.NewInt(10), risk.Assess(risk.Inputs{LiquidityRatio: 1, SuccessRate: 1, Hops: 3}))
	if err != nil || !candidate.Profitable {
		t.Fatalf("candidate=%+v err=%v", candidate, err)
	}
	quoted := e.QuoteMany(context.Background(), []graph.Route{route}, map[string]*big.Int{"B": big.NewInt(100)})
	if len(quoted) != 1 || quoted[0].Err != nil || quoted[0].Chain.AmountOut.Sign() <= 0 {
		t.Fatalf("parallel quote result=%+v", quoted)
	}
}
