// Package simulation performs the mandatory pre-broadcast eth_call,
// estimateGas, and authoritative Arbitrum post-estimation fee repricing.
package simulation

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/titanarb/titanarb-go/internal/fees"
	"github.com/titanarb/titanarb-go/internal/rpc"
)

type Caller interface {
	EthCall(context.Context, map[string]string) (string, error)
	EstimateGas(context.Context, rpc.CallMessage) (uint64, error)
}

type Result struct {
	GasEstimate uint64
	Fee         fees.FeeEstimate
}

// Simulate executes both mandatory RPC simulations. It does not sign or send.
func Simulate(ctx context.Context, client Caller, feeService *fees.Service, from, to string, calldata []byte, value *big.Int) (Result, error) {
	if len(calldata) < 4 {
		return Result{}, fmt.Errorf("missing calldata")
	}
	call := rpc.CallMessage{From: from, To: to, Data: "0x" + hex.EncodeToString(calldata)}
	if value != nil && value.Sign() > 0 {
		call.Value = "0x" + value.Text(16)
	}
	if _, err := client.EthCall(ctx, map[string]string{"from": call.From, "to": call.To, "data": call.Data, "value": call.Value}); err != nil {
		return Result{}, fmt.Errorf("eth_call: %w", err)
	}
	gas, err := client.EstimateGas(ctx, call)
	if err != nil {
		return Result{}, fmt.Errorf("eth_estimateGas: %w", err)
	}
	fee, err := feeService.Estimate(ctx, gas, calldata)
	if err != nil {
		return Result{}, fmt.Errorf("Arbitrum fee repricing: %w", err)
	}
	return Result{GasEstimate: gas, Fee: fee}, nil
}
