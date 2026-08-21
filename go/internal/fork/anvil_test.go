package fork

import (
	"context"
	"testing"
)

type fakeCaller struct {
	calls   []string
	receipt bool
}

func (f *fakeCaller) Call(_ context.Context, method string, _ any, out any) error {
	f.calls = append(f.calls, method)
	if method == "eth_sendTransaction" {
		*(out.(*string)) = "0xabc"
	}
	if method == "eth_getTransactionReceipt" && f.receipt {
		*(out.(**struct{})) = nil
	}
	return nil
}

func TestControllerRefusesNonLocalEndpoint(t *testing.T) {
	if _, err := NewController("https://arb-mainnet.example", &fakeCaller{}); err == nil {
		t.Fatal("must reject non-local endpoint")
	}
	if _, err := NewController("http://127.0.0.1:8545", &fakeCaller{}); err != nil {
		t.Fatal(err)
	}
}
func TestImpersonatedSendUsesNodeTransaction(t *testing.T) {
	f := &fakeCaller{}
	c, err := NewController("http://localhost:8545", f)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := c.SendFromImpersonated(context.Background(), "0x0000000000000000000000000000000000000001", "0x0000000000000000000000000000000000000002", "0x", 500000)
	if err != nil || hash != "0xabc" {
		t.Fatalf("hash=%s err=%v", hash, err)
	}
	if len(f.calls) != 1 || f.calls[0] != "eth_sendTransaction" {
		t.Fatalf("wrong mechanism: %v", f.calls)
	}
}

func TestSyncTimestampUsesOnlyAnvilDevRPC(t *testing.T) {
	f := &fakeCaller{}
	c, err := NewController("http://127.0.0.1:8545", f)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SyncTimestamp(context.Background(), 1_700_000_000); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 2 || f.calls[0] != "evm_setNextBlockTimestamp" || f.calls[1] != "evm_mine" {
		t.Fatalf("unexpected timestamp calls: %v", f.calls)
	}
}
