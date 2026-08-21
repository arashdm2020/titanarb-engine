package execution

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func addr(v string) common.Address { return common.HexToAddress(v) }
func requestWithHops(t *testing.T, hops int) Request {
	t.Helper()
	asset := addr("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
	weth := addr("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
	arb := addr("0x912ce59144191c1204e64559fe8253a0e49e6548")
	usdt := addr("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9")
	paths := map[int][]common.Address{
		2: {asset, weth, asset},
		3: {asset, weth, arb, asset},
		4: {asset, weth, arb, usdt, asset},
	}
	path := paths[hops]
	steps := make([]SwapStep, 0, hops)
	for i := 0; i < hops; i++ {
		data := []byte{}
		if i%2 == 0 {
			data, _ = EncodeUniswapFee(3000)
		}
		steps = append(steps, SwapStep{Adapter: addr("0xD03BC6e7331E726dA03De74b37437F1ACa2dFD95"), TokenIn: path[i], TokenOut: path[i+1], AmountOutMinimum: big.NewInt(int64(900_000_000 - i)), Data: data})
	}
	return Request{Asset: asset, Amount: big.NewInt(1_000_000_000), Steps: steps, Deadline: big.NewInt(1_800_000_000), MinProfit: big.NewInt(5_000_000)}
}

func TestCalldataRoundTripForTwoThreeAndFourHopRoutes(t *testing.T) {
	for _, hops := range []int{2, 3, 4} {
		r := requestWithHops(t, hops)
		data, err := r.Calldata()
		if err != nil {
			t.Fatalf("%d hop: %v", hops, err)
		}
		if !bytes.Equal(data[:4], executorABI.Methods["executeArbitrage"].ID) {
			t.Fatalf("%d hop selector mismatch", hops)
		}
		decoded, err := executorABI.Methods["executeArbitrage"].Inputs.Unpack(data[4:])
		if err != nil {
			t.Fatalf("%d hop decode: %v", hops, err)
		}
		if decoded[1].(*big.Int).Cmp(r.Amount) != 0 || decoded[4].(*big.Int).Cmp(r.MinProfit) != 0 {
			t.Fatalf("%d hop values changed", hops)
		}
	}
}

func TestRouteSafetyValidation(t *testing.T) {
	r := requestWithHops(t, 3)
	r.Steps[1].TokenIn = r.Asset
	if err := r.Validate(); err == nil {
		t.Fatal("expected discontinuous route rejection")
	}
}
