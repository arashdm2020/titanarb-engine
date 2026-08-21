// Package optimizer finds the economically best safe loan size without assuming
// that the largest loan, a particular stablecoin, or a fixed historical size wins.
package optimizer

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"

	"github.com/titanarb/titanarb-go/internal/graph"
)

type Evaluation struct {
	Amount         *big.Int
	NetProfit      *big.Int
	Executable     bool
	Reason         string
	PriceImpactBPS uint64
}

type Evaluator interface {
	Evaluate(context.Context, string, graph.Route, *big.Int) (Evaluation, error)
}

// Range uses raw token units. A caller with pricing data resolves a neutral USD
// exploration range (for example 100k through multi-million notional) into raw
// units for every asset; the optimizer itself never identifies or favours a token.
type Range struct {
	Min, Max *big.Int
	Samples  int
}

type OptimalLoan struct {
	Asset      string
	Route      graph.Route
	Amount     *big.Int
	NetProfit  *big.Int
	Evaluated  int
	Confidence string
}

type Optimizer struct{ Workers int }

func (o Optimizer) Optimize(ctx context.Context, evaluator Evaluator, asset string, route graph.Route, r Range) (OptimalLoan, error) {
	if evaluator == nil || r.Min == nil || r.Max == nil || r.Min.Sign() <= 0 || r.Max.Cmp(r.Min) < 0 {
		return OptimalLoan{}, fmt.Errorf("valid evaluator and positive size range are required")
	}
	amounts := candidates(r)
	workers := o.Workers
	if workers < 1 {
		workers = 1
	}
	type outcome struct {
		evaluation Evaluation
		err        error
	}
	jobs := make(chan *big.Int)
	results := make(chan outcome, len(amounts))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for amount := range jobs {
				value, err := evaluator.Evaluate(ctx, asset, route, amount)
				results <- outcome{evaluation: value, err: err}
			}
		}()
	}
	go func() {
		for _, amount := range amounts {
			select {
			case jobs <- amount:
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				close(results)
				return
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()
	best := OptimalLoan{Asset: asset, Route: route, NetProfit: new(big.Int).Neg(big.NewInt(1))}
	for out := range results {
		best.Evaluated++
		if out.err != nil || !out.evaluation.Executable || out.evaluation.NetProfit == nil || out.evaluation.NetProfit.Sign() <= 0 {
			continue
		}
		if best.Amount == nil || out.evaluation.NetProfit.Cmp(best.NetProfit) > 0 {
			best.Amount = new(big.Int).Set(out.evaluation.Amount)
			best.NetProfit = new(big.Int).Set(out.evaluation.NetProfit)
			best.Confidence = "quote_and_fee_screened"
		}
	}
	if best.Amount == nil {
		return best, fmt.Errorf("no profitable executable size")
	}
	return best, nil
}

func candidates(r Range) []*big.Int {
	n := r.Samples
	if n < 2 {
		n = 2
	}
	values := make(map[string]*big.Int)
	// Integer linear samples are deliberate: quote curves can be non-monotonic
	// around tick boundaries, so this never assumes bigger capital is better.
	delta := new(big.Int).Sub(r.Max, r.Min)
	for i := 0; i < n; i++ {
		amount := new(big.Int).Mul(delta, big.NewInt(int64(i)))
		amount.Div(amount, big.NewInt(int64(n-1)))
		amount.Add(amount, r.Min)
		values[amount.String()] = amount
	}
	output := make([]*big.Int, 0, len(values))
	for _, amount := range values {
		output = append(output, amount)
	}
	sort.Slice(output, func(i, j int) bool { return output[i].Cmp(output[j]) < 0 })
	return output
}
