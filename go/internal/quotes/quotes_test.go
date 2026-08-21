package quotes

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/pools"
	"math/big"
	"testing"
	"time"
)

type fakeCaller struct {
	response string
	err      error
	wait     bool
}

func (f fakeCaller) EthCall(ctx context.Context, _ map[string]string) (string, error) {
	if f.wait {
		<-ctx.Done()
		return "", ctx.Err()
	}
	return f.response, f.err
}
func quoteResponse(values ...int64) string {
	raw := "0x"
	for _, v := range values {
		raw += hex.EncodeToString(dex.UintWord(big.NewInt(v)))
	}
	return raw
}
func testRequest(d pools.DEX) Request {
	return Request{TokenIn: "0x0000000000000000000000000000000000000001", TokenOut: "0x0000000000000000000000000000000000000002", AmountIn: big.NewInt(10), Pool: pools.Pool{DEX: d, Fee: 500}}
}
func TestSuccessfulQuotes(t *testing.T) {
	u := NewUniswapV3(fakeCaller{response: quoteResponse(11, 0, 0, 123)}, "0x0000000000000000000000000000000000000003", nil)
	got, err := u.Quote(context.Background(), testRequest(pools.UniswapV3))
	if err != nil || got.AmountOut.Cmp(big.NewInt(11)) != 0 || got.EstimatedGas != 123 {
		t.Fatalf("%+v %v", got, err)
	}
	c := NewCamelot(fakeCaller{response: quoteResponse(12, 77)}, "0x0000000000000000000000000000000000000003", nil)
	got, err = c.Quote(context.Background(), testRequest(pools.CamelotV3))
	if err != nil || got.Fee != 77 {
		t.Fatalf("%+v %v", got, err)
	}
}
func TestQuoteFailureAndTimeout(t *testing.T) {
	u := NewUniswapV3(fakeCaller{err: errors.New("rpc down")}, "0x0000000000000000000000000000000000000003", nil)
	if _, err := u.Quote(context.Background(), testRequest(pools.UniswapV3)); err == nil {
		t.Fatal("expected failure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	u = NewUniswapV3(fakeCaller{wait: true}, "0x0000000000000000000000000000000000000003", nil)
	if _, err := u.Quote(ctx, testRequest(pools.UniswapV3)); err == nil {
		t.Fatal("expected timeout")
	}
}
