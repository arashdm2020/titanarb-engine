// Package tx owns the narrow broadcast and receipt lifecycle. It never makes a
// profitability decision; callers must pass a transaction that has already
// completed simulation and final repricing.
package tx

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/titanarb/titanarb-go/internal/rpc"
	"github.com/titanarb/titanarb-go/internal/wallet"
)

type Client interface {
	SendRawTransaction(context.Context, string) (string, error)
	TransactionReceipt(context.Context, string) (*rpc.Receipt, error)
}
type Classification string

const (
	NonceTooLow            Classification = "nonce_too_low"
	ReplacementUnderpriced Classification = "replacement_underpriced"
	Timeout                Classification = "timeout"
	Reverted               Classification = "reverted"
	OutOfGas               Classification = "out_of_gas"
	Unknown                Classification = "unknown"
)

type Result struct {
	Hash           string
	Receipt        *rpc.Receipt
	Classification Classification
	Err            error
}

func Classify(err error) Classification {
	if err == nil {
		return Unknown
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "nonce too low"):
		return NonceTooLow
	case strings.Contains(s, "replacement") && strings.Contains(s, "underpriced"):
		return ReplacementUnderpriced
	case strings.Contains(s, "out of gas"):
		return OutOfGas
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline"):
		return Timeout
	default:
		return Unknown
	}
}

func Broadcast(ctx context.Context, client Client, signer *wallet.Wallet, chainID *big.Int, nonce, gas uint64, maxFeePerGas, maxPriorityFeePerGas *big.Int, to string, data []byte) (string, error) {
	if signer == nil {
		return "", fmt.Errorf("signer required")
	}
	if maxFeePerGas == nil || maxFeePerGas.Sign() <= 0 {
		return "", fmt.Errorf("max fee per gas is required")
	}
	if maxPriorityFeePerGas == nil || maxPriorityFeePerGas.Sign() < 0 {
		maxPriorityFeePerGas = big.NewInt(0)
	}
	transaction := types.NewTx(&types.DynamicFeeTx{ChainID: chainID, Nonce: nonce, To: ptrAddress(to), Gas: gas, GasFeeCap: maxFeePerGas, GasTipCap: maxPriorityFeePerGas, Value: big.NewInt(0), Data: data})
	signed, err := signer.Sign(chainID, transaction)
	if err != nil {
		return "", err
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		return "", err
	}
	return client.SendRawTransaction(ctx, "0x"+hex.EncodeToString(raw))
}

func ptrAddress(raw string) *common.Address { a := common.HexToAddress(raw); return &a }

func WaitReceipt(ctx context.Context, client Client, hash string, interval time.Duration) Result {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		receipt, err := client.TransactionReceipt(ctx, hash)
		if err != nil {
			return Result{Hash: hash, Classification: Classify(err), Err: err}
		}
		if receipt != nil {
			if receipt.Status == "0x0" {
				return Result{Hash: hash, Receipt: receipt, Classification: Reverted, Err: fmt.Errorf("transaction reverted")}
			}
			return Result{Hash: hash, Receipt: receipt}
		}
		select {
		case <-ctx.Done():
			return Result{Hash: hash, Classification: Timeout, Err: ctx.Err()}
		case <-ticker.C:
		}
	}
}
