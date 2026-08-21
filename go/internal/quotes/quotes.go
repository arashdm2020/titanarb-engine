// Package quotes supplies executable, read-only DEX quote calls.
package quotes

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/pools"
)

type Request struct {
	TokenIn, TokenOut string
	AmountIn          *big.Int
	Pool              pools.Pool
}

type Result struct {
	AmountOut    *big.Int
	EstimatedGas uint64 // Uniswap QuoterV2 reports this; Camelot returns zero.
	Fee          uint32 // Dynamic Algebra fee for Camelot; static pool fee for Uniswap.
}

type Quoter interface {
	Quote(context.Context, Request) (Result, error)
}

type Caller interface {
	EthCall(context.Context, map[string]string) (string, error)
}

type UniswapV3Quoter struct {
	caller  Caller
	address string
	metrics *metrics.Metrics
}

func NewUniswapV3(c Caller, address string, m *metrics.Metrics) *UniswapV3Quoter {
	return &UniswapV3Quoter{c, address, m}
}

func (q *UniswapV3Quoter) Quote(ctx context.Context, req Request) (Result, error) {
	if req.Pool.DEX != pools.UniswapV3 || req.Pool.Fee == 0 {
		return Result{}, fmt.Errorf("invalid Uniswap V3 quote request")
	}
	in, err := dex.AddressWord(req.TokenIn)
	if err != nil {
		return Result{}, err
	}
	out, err := dex.AddressWord(req.TokenOut)
	if err != nil {
		return Result{}, err
	}
	data := dex.StaticCall("quoteExactInputSingle((address,address,uint256,uint24,uint160))", in, out, dex.UintWord(req.AmountIn), dex.Uint64Word(uint64(req.Pool.Fee)), dex.Uint64Word(0))
	raw, err := q.caller.EthCall(ctx, map[string]string{"to": q.address, "data": data})
	if err != nil {
		q.failed()
		return Result{}, fmt.Errorf("uniswap quote: %w", err)
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) < 4 {
		q.failed()
		return Result{}, fmt.Errorf("uniswap quote response: %w", err)
	}
	q.ok()
	return Result{AmountOut: dex.WordUint(words[0]), EstimatedGas: dex.WordUint(words[3]).Uint64(), Fee: req.Pool.Fee}, nil
}

type CamelotQuoter struct {
	caller  Caller
	address string
	metrics *metrics.Metrics
}

func NewCamelot(c Caller, address string, m *metrics.Metrics) *CamelotQuoter {
	return &CamelotQuoter{c, address, m}
}

func (q *CamelotQuoter) Quote(ctx context.Context, req Request) (Result, error) {
	if req.Pool.DEX != pools.CamelotV3 {
		return Result{}, fmt.Errorf("invalid Camelot quote request")
	}
	in, err := dex.AddressWord(req.TokenIn)
	if err != nil {
		return Result{}, err
	}
	out, err := dex.AddressWord(req.TokenOut)
	if err != nil {
		return Result{}, err
	}
	data := dex.StaticCall("quoteExactInputSingle(address,address,uint256,uint160)", in, out, dex.UintWord(req.AmountIn), dex.Uint64Word(0))
	raw, err := q.caller.EthCall(ctx, map[string]string{"to": q.address, "data": data})
	if err != nil {
		q.failed()
		return Result{}, fmt.Errorf("camelot quote: %w", err)
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) < 2 {
		q.failed()
		return Result{}, fmt.Errorf("camelot quote response: %w", err)
	}
	q.ok()
	return Result{AmountOut: dex.WordUint(words[0]), Fee: uint32(dex.WordUint(words[1]).Uint64())}, nil
}

func (q *UniswapV3Quoter) ok() {
	if q.metrics != nil {
		q.metrics.IncQuotes()
		q.metrics.IncUniswapQuotes()
	}
}
func (q *UniswapV3Quoter) failed() {
	if q.metrics != nil {
		q.metrics.IncQuoteFailures()
	}
}
func (q *CamelotQuoter) ok() {
	if q.metrics != nil {
		q.metrics.IncQuotes()
		q.metrics.IncCamelotQuotes()
	}
}
func (q *CamelotQuoter) failed() {
	if q.metrics != nil {
		q.metrics.IncQuoteFailures()
	}
}

// QuoteMany is bounded; it never starts an unbounded goroutine per route.
func QuoteMany(ctx context.Context, requests []Request, selectQuoter func(pools.Pool) Quoter, workers int) []Result {
	if workers < 1 {
		workers = 1
	}
	results := make([]Result, len(requests))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				result, _ := selectQuoter(requests[i].Pool).Quote(ctx, requests[i])
				results[i] = result
			}
		}()
	}
	for i := range requests {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break
		}
	}
	close(jobs)
	wg.Wait()
	return results
}
