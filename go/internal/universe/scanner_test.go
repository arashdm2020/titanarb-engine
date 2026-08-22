package universe

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
)

type fakeDiscoverer struct {
	byToken map[string][]pools.Pool
}

func (f fakeDiscoverer) DiscoverPair(_ context.Context, a, b string) ([]pools.Pool, error) {
	if out := f.byToken[a]; len(out) > 0 {
		return out, nil
	}
	if out := f.byToken[b]; len(out) > 0 {
		return out, nil
	}
	return nil, nil
}

type fakeCaller struct{ reserve bool }

func (f fakeCaller) EthCall(context.Context, map[string]string) (string, error) {
	if !f.reserve {
		return "", errors.New("missing reserve")
	}
	words := make([]byte, 32*9)
	copy(words[32*8+12:32*9], []byte{1})
	return "0x" + fmtHex(words), nil
}

type fakeQuoter struct{ ok bool }

func (f fakeQuoter) Quote(context.Context, quotes.Request) (quotes.Result, error) {
	if !f.ok {
		return quotes.Result{}, errors.New("quote failed")
	}
	return quotes.Result{AmountOut: big.NewInt(1)}, nil
}

func TestScannerRanksConfiguredNonExecutionAssets(t *testing.T) {
	candidate := config.Token{Symbol: "USDC.e", Address: "0x00000000000000000000000000000000000000ee", Decimals: 6}
	market := config.MarketConfig{
		AavePool:            "0x0000000000000000000000000000000000000003",
		ExecutionAssetNames: []string{"USDC", "WETH"},
		Tokens: map[string]config.Token{
			"USDC":   {Symbol: "USDC", Address: "0x0000000000000000000000000000000000000001", Decimals: 6},
			"WETH":   {Symbol: "WETH", Address: "0x0000000000000000000000000000000000000002", Decimals: 18},
			"USDC_E": candidate,
		},
	}
	discoverer := fakeDiscoverer{byToken: map[string][]pools.Pool{
		candidate.Address: {
			{Address: "0x0000000000000000000000000000000000000010", Token0: candidate.Address, Token1: market.Tokens["USDC"].Address, DEX: pools.UniswapV3, Fee: 500, Liquidity: big.NewInt(100)},
			{Address: "0x0000000000000000000000000000000000000011", Token0: candidate.Address, Token1: market.Tokens["USDC"].Address, DEX: pools.CamelotV3, Liquidity: big.NewInt(200)},
		},
	}}
	scanner := Scanner{
		Market:     market,
		Discoverer: discoverer,
		Caller:     fakeCaller{reserve: true},
		Quoter:     func(pools.Pool) quotes.Quoter { return fakeQuoter{ok: true} },
	}

	report, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ScannedAssets != 1 || len(report.Candidates) != 1 {
		t.Fatalf("unexpected candidates: %#v", report)
	}
	got := report.Candidates[0]
	if got.Symbol != "USDC.e" || got.UniswapPools != 2 || got.CamelotPools != 2 || !got.AaveReserve || got.QuoteSuccesses != got.QuoteAttempts {
		t.Fatalf("candidate scoring inputs missing: %#v", got)
	}
	if got.Score <= 0 {
		t.Fatalf("candidate score was not positive: %#v", got)
	}
}

func TestScannerDoesNotScanExecutionAssets(t *testing.T) {
	market := config.MarketConfig{
		ExecutionAssetNames: []string{"USDC"},
		Tokens: map[string]config.Token{
			"USDC": {Symbol: "USDC", Address: "0x0000000000000000000000000000000000000001", Decimals: 6},
		},
	}
	report, err := (Scanner{Market: market, Discoverer: fakeDiscoverer{}}).Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ScannedAssets != 0 || len(report.Candidates) != 0 {
		t.Fatalf("execution asset entered read-only candidate scan: %#v", report)
	}
}

func fmtHex(input []byte) string {
	const alphabet = "0123456789abcdef"
	out := make([]byte, len(input)*2)
	for i, b := range input {
		out[i*2] = alphabet[b>>4]
		out[i*2+1] = alphabet[b&0x0f]
	}
	return string(out)
}
