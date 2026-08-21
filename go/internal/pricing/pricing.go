// Package pricing computes read-only opportunity economics in raw base-token units.
package pricing

import "math/big"

type Inputs struct {
	AmountIn    *big.Int
	AmountOut   *big.Int
	AavePremium *big.Int
	L2Fee       *big.Int
	L1DataFee   *big.Int
	MinProfit   *big.Int
}

type Result struct {
	GrossProfit    *big.Int
	ExpectedProfit *big.Int
	RequiredOutput *big.Int
	Profitable     bool
}

func Evaluate(in Inputs) Result {
	zero := big.NewInt(0)
	amountIn, amountOut := value(in.AmountIn), value(in.AmountOut)
	premium, l2, l1, minimum := value(in.AavePremium), value(in.L2Fee), value(in.L1DataFee), value(in.MinProfit)
	gross := new(big.Int).Sub(amountOut, amountIn)
	net := new(big.Int).Sub(gross, premium)
	net.Sub(net, l2)
	net.Sub(net, l1)
	required := new(big.Int).Add(amountIn, premium)
	required.Add(required, l2)
	required.Add(required, l1)
	required.Add(required, minimum)
	return Result{GrossProfit: gross, ExpectedProfit: net, RequiredOutput: required, Profitable: net.Cmp(minimum) > 0 && amountOut.Cmp(zero) > 0}
}

func value(v *big.Int) *big.Int {
	if v == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Set(v)
}
