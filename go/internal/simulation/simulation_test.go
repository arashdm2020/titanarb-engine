package simulation

import (
	"context"
	"errors"
	"testing"

	"github.com/titanarb/titanarb-go/internal/rpc"
)

type fakeClient struct {
	called, estimated bool
	err               error
}

func (f *fakeClient) EthCall(context.Context, map[string]string) (string, error) {
	f.called = true
	return "0x", f.err
}
func (f *fakeClient) EstimateGas(context.Context, rpc.CallMessage) (uint64, error) {
	f.estimated = true
	return 1, f.err
}
func TestCallFailurePreventsEstimate(t *testing.T) {
	f := &fakeClient{err: errors.New("revert")}
	_, err := Simulate(context.Background(), f, nil, "0x1", "0x2", []byte{1, 2, 3, 4}, nil)
	if err == nil || f.estimated {
		t.Fatal("estimate must not run after eth_call failure")
	}
}
