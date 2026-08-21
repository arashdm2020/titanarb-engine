package pools

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/titanarb/titanarb-go/internal/dex"
)

type fakeCaller struct {
	responses map[string]string
	err       error
}

func (f fakeCaller) EthCall(_ context.Context, call map[string]string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.responses[call["to"]+"|"+call["data"]], nil
}
func (f fakeCaller) BlockNumber(context.Context) (uint64, error) { return 42, nil }
func word(v *big.Int) string                                     { return hex.EncodeToString(dex.UintWord(v)) }
func addressWord(a string) string                                { w, _ := dex.AddressWord(a); return hex.EncodeToString(w) }
func response(words ...string) string {
	out := "0x"
	for _, w := range words {
		out += w
	}
	return out
}

func TestDiscoverUniswapPool(t *testing.T) {
	a := "0x0000000000000000000000000000000000000001"
	b := "0x0000000000000000000000000000000000000002"
	factory := "0x0000000000000000000000000000000000000010"
	camelot := "0x0000000000000000000000000000000000000011"
	pool := "0x0000000000000000000000000000000000000020"
	aw, _ := dex.AddressWord(a)
	bw, _ := dex.AddressWord(b)
	lookup := map[string]string{}
	lookup[factory+"|"+dex.StaticCall("getPool(address,address,uint24)", aw, bw, dex.Uint64Word(500))] = response(addressWord(pool))
	lookup[camelot+"|"+dex.StaticCall("poolByPair(address,address)", aw, bw)] = response(word(big.NewInt(0)))
	lookup[pool+"|"+dex.StaticCall("token0()")] = response(addressWord(a))
	lookup[pool+"|"+dex.StaticCall("token1()")] = response(addressWord(b))
	lookup[pool+"|"+dex.StaticCall("liquidity()")] = response(word(big.NewInt(99)))
	lookup[pool+"|"+dex.StaticCall("fee()")] = response(word(big.NewInt(500)))
	lookup[pool+"|"+dex.StaticCall("slot0()")] = response(word(big.NewInt(7)))
	d := NewDiscoverer(fakeCaller{responses: lookup}, factory, camelot, []uint32{500})
	found, err := d.DiscoverPair(context.Background(), a, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].Fee != 500 || found[0].Liquidity.Cmp(big.NewInt(99)) != 0 {
		t.Fatalf("unexpected pools %+v", found)
	}
}

func TestInvalidPoolResponse(t *testing.T) {
	d := NewDiscoverer(fakeCaller{responses: map[string]string{}}, "factory", "camelot", []uint32{500})
	if _, err := d.DiscoverPair(context.Background(), "0x0000000000000000000000000000000000000001", "0x0000000000000000000000000000000000000002"); err == nil {
		t.Fatal("expected invalid RPC response")
	}
}

func TestRefreshPoolReadsOnlyMutableState(t *testing.T) {
	address := "0x0000000000000000000000000000000000000020"
	lookup := map[string]string{
		address + "|" + dex.StaticCall("liquidity()"): response(word(big.NewInt(123))),
		address + "|" + dex.StaticCall("slot0()"):     response(word(big.NewInt(456))),
	}
	d := NewDiscoverer(fakeCaller{responses: lookup}, "factory", "camelot", nil)
	updated, err := d.RefreshPoolAt(context.Background(), Pool{Address: address, DEX: UniswapV3}, 99)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Liquidity.Cmp(big.NewInt(123)) != 0 || updated.SqrtPriceX96.Cmp(big.NewInt(456)) != 0 || updated.LastUpdatedBlock != 99 {
		t.Fatalf("unexpected refreshed state: %+v", updated)
	}
}
