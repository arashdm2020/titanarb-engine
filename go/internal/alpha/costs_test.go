package alpha

import (
	"context"
	"github.com/titanarb/titanarb-go/internal/config"
	"math/big"
	"strings"
	"testing"
)

type feedCaller struct{}

func (feedCaller) EthCall(_ context.Context, call map[string]string) (string, error) {
	if call["data"] == "0x313ce567" {
		return "0x0000000000000000000000000000000000000000000000000000000000000008", nil
	}
	return "0x" +
		"0000000000000000000000000000000000000000000000000000000000000001" +
		"0000000000000000000000000000000000000000000000000000000077359400" +
		strings.Repeat("0000000000000000000000000000000000000000000000000000000000000000", 2) +
		"0000000000000000000000000000000000000000000000000000000000000001", nil
}
func TestChainlinkPricesRequiresConfiguredFeed(t *testing.T) {
	p := ChainlinkPrices{Caller: feedCaller{}, Feeds: map[string]string{"A": "0x0000000000000000000000000000000000000001"}}
	price, err := p.USD(context.Background(), config.Token{Symbol: "A"})
	if err != nil || price.Cmp(big.NewRat(20, 1)) != 0 {
		t.Fatalf("price=%v err=%v", price, err)
	}
	if _, err := p.USD(context.Background(), config.Token{Symbol: "B"}); err == nil {
		t.Fatal("missing feed must reject asset pricing")
	}
}
