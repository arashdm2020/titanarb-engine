package fork

import (
	"context"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/rpc"
)

const (
	localUSDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
	// This is a real fork-state USDC holder used only after an explicit local
	// endpoint opt-in. It is never used in production code.
	forkUSDCSource = "0x724dc807b04555b71ed48a6896b6F41593b8C637"
)

func TestAnvilRealPoolConditioningWithoutEstimate(t *testing.T) {
	endpoint := os.Getenv("TITANARB_FORK_RPC_URL")
	adapter := os.Getenv("TITANARB_FORK_CAMELOT_ADAPTER")
	if endpoint == "" || adapter == "" {
		t.Skip("set local fork endpoint and Camelot adapter")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := rpc.New(endpoint, 15*time.Second, 0, nil)
	controller, err := NewController(endpoint, client)
	if err != nil {
		t.Fatal(err)
	}
	var accounts []string
	if err := client.Call(ctx, "eth_accounts", []any{}, &accounts); err != nil || len(accounts) == 0 {
		t.Fatalf("local accounts: %v", err)
	}
	in, err := dex.AddressWord(localUSDC)
	if err != nil {
		t.Fatal(err)
	}
	out, err := dex.AddressWord("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
	if err != nil {
		t.Fatal(err)
	}
	data := dex.DynamicBytesCall("executeSwap(address,address,uint256,uint256,uint256,bytes)", [][]byte{in, out, dex.UintWord(big.NewInt(100_000_000_000)), dex.Uint64Word(1), dex.Uint64Word(4_294_967_295)}, nil)
	hash, err := controller.SendLocal(ctx, accounts[0], adapter, data, 8_000_000)
	if err != nil {
		t.Fatalf("conditioning submit: %v", err)
	}
	receipt, err := controller.WaitReceipt(ctx, hash, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != "0x1" {
		t.Fatalf("conditioning receipt=%+v", receipt)
	}
}

func TestAnvilImpersonatedERC20Funding(t *testing.T) {
	endpoint := os.Getenv("TITANARB_FORK_RPC_URL")
	if endpoint == "" {
		t.Skip("set TITANARB_FORK_RPC_URL to an Anvil endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := rpc.New(endpoint, 15*time.Second, 0, nil)
	controller, err := NewController(endpoint, client)
	if err != nil {
		t.Fatal(err)
	}
	var accounts []string
	if err := client.Call(ctx, "eth_accounts", []any{}, &accounts); err != nil || len(accounts) == 0 {
		t.Fatalf("local accounts: %v", err)
	}
	recipient := accounts[0]
	before, err := erc20Balance(ctx, client, localUSDC, recipient)
	if err != nil {
		t.Fatal(err)
	}
	source, err := erc20Balance(ctx, client, localUSDC, forkUSDCSource)
	if err != nil {
		t.Fatal(err)
	}
	amount := big.NewInt(1_000_000)
	if raw := os.Getenv("TITANARB_FORK_FUND_RAW"); raw != "" {
		parsed, ok := new(big.Int).SetString(raw, 10)
		if !ok || parsed.Sign() <= 0 {
			t.Fatalf("invalid TITANARB_FORK_FUND_RAW")
		}
		amount = parsed
	}
	if source.Cmp(amount) < 0 {
		t.Skip("fork source does not hold enough USDC at this block")
	}
	receipt, hash, err := controller.FundERC20(ctx, localUSDC, forkUSDCSource, recipient, amount, big.NewInt(1_000_000_000_000_000_000), 300_000)
	if err != nil {
		t.Fatalf("impersonated funding failed before a protocol call: %v", err)
	}
	if receipt == nil || receipt.Status != "0x1" {
		t.Fatalf("funding receipt hash=%s receipt=%+v", hash, receipt)
	}
	after, err := erc20Balance(ctx, client, localUSDC, recipient)
	if err != nil {
		t.Fatal(err)
	}
	if new(big.Int).Sub(after, before).Cmp(amount) != 0 {
		t.Fatalf("unexpected recipient USDC delta")
	}
}

func erc20Balance(ctx context.Context, client *rpc.Client, token, account string) (*big.Int, error) {
	word, err := dex.AddressWord(account)
	if err != nil {
		return nil, err
	}
	raw, err := client.EthCall(ctx, map[string]string{"to": token, "data": dex.StaticCall("balanceOf(address)", word)})
	if err != nil {
		return nil, err
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) == 0 {
		return nil, err
	}
	return dex.WordUint(words[0]), nil
}
