package pricing

import (
	"math/big"
	"testing"
)

func n(v int64) *big.Int { return big.NewInt(v) }
func TestProfitability(t *testing.T) {
	profitable := Evaluate(Inputs{AmountIn: n(1000), AmountOut: n(1120), AavePremium: n(5), L2Fee: n(10), L1DataFee: n(5), MinProfit: n(50)})
	if !profitable.Profitable || profitable.ExpectedProfit.Cmp(n(100)) != 0 {
		t.Fatalf("unexpected profitable result: %+v", profitable)
	}
	unprofitable := Evaluate(Inputs{AmountIn: n(1000), AmountOut: n(1050), AavePremium: n(5), L2Fee: n(10), L1DataFee: n(5), MinProfit: n(50)})
	if unprofitable.Profitable {
		t.Fatal("expected minimum-profit rejection")
	}
	gasRejected := Evaluate(Inputs{AmountIn: n(1000), AmountOut: n(1200), AavePremium: n(5), L2Fee: n(140), L1DataFee: n(10), MinProfit: n(50)})
	if gasRejected.Profitable {
		t.Fatal("expected gas rejection")
	}
}
