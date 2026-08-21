// Package execution contains the Go-only transaction construction and safety
// boundary for the deployed FlashArbitrageExecutor.
package execution

import (
	_ "embed"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/titanarb/titanarb-go/internal/rpc"
)

//go:embed abi/flash_arbitrage_executor_v1.json
var flashExecutorABIV1 []byte

//go:embed abi/adapters_v1.json
var adaptersABIV1 []byte

var executorABI abi.ABI
var adapterABI abi.ABI

func init() {
	var err error
	executorABI, err = abi.JSON(strings.NewReader(string(flashExecutorABIV1)))
	if err != nil {
		panic(fmt.Sprintf("embedded FlashArbitrageExecutor ABI: %v", err))
	}
	adapterABI, err = abi.JSON(strings.NewReader(string(adaptersABIV1)))
	if err != nil {
		panic(fmt.Sprintf("embedded adapter ABI: %v", err))
	}
}

func ExecutorABI() abi.ABI { return executorABI }

func AdapterABI() abi.ABI { return adapterABI }

// DecodeError decodes the real custom errors declared by the current Solidity
// source. It intentionally does not invent an InsufficientRepayment error.
func DecodeError(data []byte) (string, map[string]any, bool) {
	if len(data) < 4 {
		return "", nil, false
	}
	for name, definition := range executorABI.Errors {
		if string(definition.ID[:4]) != string(data[:4]) {
			continue
		}
		values, err := definition.Inputs.Unpack(data[4:])
		if err != nil {
			return name, nil, false
		}
		result := make(map[string]any, len(values))
		for i, input := range definition.Inputs {
			result[input.Name] = values[i]
		}
		return name, result, true
	}
	return "", nil, false
}

// RealizedProfit reads the executor's own ArbitrageExecuted event from a
// successful receipt. This is the on-chain reported profit before gas, in the
// borrowed token's raw units; gas is tracked separately from the receipt.
func RealizedProfit(logs []rpc.Log, executor string) (*big.Int, bool) {
	event := executorABI.Events["ArbitrageExecuted"]
	for _, log := range logs {
		if !strings.EqualFold(log.Address, executor) || len(log.Topics) == 0 || !strings.EqualFold(log.Topics[0], event.ID.Hex()) {
			continue
		}
		data, err := hex.DecodeString(strings.TrimPrefix(log.Data, "0x"))
		if err != nil {
			continue
		}
		values, err := event.Inputs.NonIndexed().Unpack(data)
		if err != nil || len(values) != 5 {
			continue
		}
		profitValue, ok := values[3].(*big.Int)
		if !ok {
			continue
		}
		return profitValue, true
	}
	return nil, false
}

func executorAddress(raw string) (common.Address, error) {
	if !common.IsHexAddress(raw) {
		return common.Address{}, fmt.Errorf("invalid address %q", raw)
	}
	return common.HexToAddress(raw), nil
}
