package optimizer

import (
	"context"
	"math/big"
	"testing"

	"github.com/titanarb/titanarb-go/internal/graph"
)

type curve struct{}

func (curve) Evaluate(_ context.Context, _ string, _ graph.Route, amount *big.Int) (Evaluation, error) {
	// Best profit is intentionally the interior point, not the largest amount.
	d := new(big.Int).Sub(amount, big.NewInt(500))
	p := new(big.Int).Mul(d, d)
	p.Neg(p)
	p.Add(p, big.NewInt(1_000))
	return Evaluation{Amount: new(big.Int).Set(amount), NetProfit: p, Executable: true}, nil
}

func TestOptimizerSelectsBestNonLargestSize(t *testing.T) {
	got, err := (Optimizer{Workers: 3}).Optimize(context.Background(), curve{}, "ARB", graph.Route{Assets: []string{"ARB", "X", "ARB"}}, Range{Min: big.NewInt(100), Max: big.NewInt(900), Samples: 5})
	if err != nil {
		t.Fatal(err)
	}
	if got.Asset != "ARB" || got.Amount.Cmp(big.NewInt(500)) != 0 || got.NetProfit.Cmp(big.NewInt(1_000)) != 0 {
		t.Fatalf("unexpected optimum: %+v", got)
	}
}
