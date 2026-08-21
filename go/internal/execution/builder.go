package execution

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// SwapStep maps 1:1 to FlashArbitrageExecutor.SwapStep. Field names are ABI
// compatible with the Solidity tuple fields.
type SwapStep struct {
	Adapter          common.Address `abi:"adapter"`
	TokenIn          common.Address `abi:"tokenIn"`
	TokenOut         common.Address `abi:"tokenOut"`
	AmountOutMinimum *big.Int       `abi:"amountOutMinimum"`
	Data             []byte         `abi:"data"`
}

type Request struct {
	Asset     common.Address
	Amount    *big.Int
	Steps     []SwapStep
	Deadline  *big.Int
	MinProfit *big.Int
}

func (r Request) Validate() error {
	if r.Asset == (common.Address{}) || r.Amount == nil || r.Amount.Sign() <= 0 {
		return fmt.Errorf("asset and amount are required")
	}
	if len(r.Steps) < 2 || len(r.Steps) > 4 {
		return fmt.Errorf("route requires 2 to 4 hops")
	}
	if r.Deadline == nil || r.Deadline.Sign() <= 0 || r.MinProfit == nil || r.MinProfit.Sign() <= 0 {
		return fmt.Errorf("positive deadline and minProfit are required")
	}
	if r.Steps[0].TokenIn != r.Asset || r.Steps[len(r.Steps)-1].TokenOut != r.Asset {
		return fmt.Errorf("route must begin and end in borrowed asset")
	}
	for i, step := range r.Steps {
		if step.Adapter == (common.Address{}) || step.TokenIn == (common.Address{}) || step.TokenOut == (common.Address{}) || step.AmountOutMinimum == nil || step.AmountOutMinimum.Sign() <= 0 {
			return fmt.Errorf("invalid step %d", i)
		}
		if i > 0 && step.TokenIn != r.Steps[i-1].TokenOut {
			return fmt.Errorf("token discontinuity at step %d", i)
		}
	}
	return nil
}

func (r Request) Calldata() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return executorABI.Pack("executeArbitrage", r.Asset, r.Amount, r.Steps, r.Deadline, r.MinProfit)
}

// EncodeUniswapFee exactly mirrors abi.encode(uint24(fee)) used by the
// UniswapV3Adapter; Camelot deliberately receives empty data.
func EncodeUniswapFee(fee uint32) ([]byte, error) {
	if fee > 0xFFFFFF {
		return nil, fmt.Errorf("uint24 fee out of range")
	}
	data := make([]byte, 32)
	data[29], data[30], data[31] = byte(fee>>16), byte(fee>>8), byte(fee)
	return data, nil
}
