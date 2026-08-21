package fees

import (
	"context"
	"encoding/hex"
	"github.com/titanarb/titanarb-go/internal/dex"
	"math/big"
	"testing"
)

type fake struct{ responses map[string]string }

func (f fake) EthCall(_ context.Context, c map[string]string) (string, error) {
	return f.responses[c["to"]+"|"+c["data"]], nil
}
func words(v ...int64) string {
	r := "0x"
	for _, x := range v {
		r += hex.EncodeToString(dex.UintWord(big.NewInt(x)))
	}
	return r
}
func TestParityFormula(t *testing.T) {
	gas := "0x000000000000000000000000000000000000006C"
	feed := "0x0000000000000000000000000000000000000001"
	dead, _ := dex.AddressWord(feeDestination)
	payload := []byte{1, 2, 3}
	node := dex.DynamicBytesCall("gasEstimateComponents(address,bool,bytes)", [][]byte{dead, dex.BoolWord(false)}, payload)
	f := fake{map[string]string{gas + "|" + dex.StaticCall("getPricesInWei()"): words(10, 0, 0, 0, 0, 2), NodeInterface + "|" + node: words(0, 3, 4, 0), feed + "|" + dex.StaticCall("decimals()"): words(8), feed + "|" + dex.StaticCall("latestRoundData()"): words(1, 200000000000, 0, 0, 1)}}
	e, err := New(f, gas, feed, 1000).Estimate(context.Background(), 5, payload)
	if err != nil {
		t.Fatal(err)
	}
	if e.L2Cost.Cmp(big.NewInt(20)) != 0 || e.L1Cost.Cmp(big.NewInt(12)) != 0 || e.TotalWei.Cmp(big.NewInt(35)) != 0 {
		t.Fatalf("%+v", e)
	}
}
