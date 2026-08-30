package pools

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/titanarb/titanarb-go/internal/dex"
)

func TestDecodeSwapLogSignedAmounts(t *testing.T) {
	negOne := strings.Repeat("f", 64)
	data := "0x" + negOne + fmt.Sprintf("%064x%064x%064x", 2, 3, 4)
	swap, ok := decodeSwapLog("0x0000000000000000000000000000000000000001", data, []string{swapTopic}, "0xa")
	if !ok || swap.Amount0.Int64() != -1 || swap.Amount1.Int64() != 2 || swap.Block != 10 {
		t.Fatalf("swap=%+v ok=%t", swap, ok)
	}
}

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

type logFakeCaller struct {
	fakeCaller
	errAtChunk       int
	duplicateAddress string
	calls            []map[string]any
}

func (f *logFakeCaller) Call(ctx context.Context, method string, params any, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if method != "eth_getLogs" {
		return fmt.Errorf("unexpected method %s", method)
	}
	args := params.([]any)
	filter := args[0].(map[string]any)
	copied := make(map[string]any, len(filter))
	for key, value := range filter {
		copied[key] = value
	}
	f.calls = append(f.calls, copied)
	if f.errAtChunk > 0 && len(f.calls) == f.errAtChunk {
		return errors.New("range limit")
	}
	address := fmt.Sprintf("0x%040x", len(f.calls))
	if f.duplicateAddress != "" {
		address = f.duplicateAddress
	}
	logs := []struct {
		Address     string   `json:"address"`
		Data        string   `json:"data"`
		Topics      []string `json:"topics"`
		BlockNumber string   `json:"blockNumber"`
	}{
		{Address: address},
	}
	target := out.(*[]struct {
		Address     string   `json:"address"`
		Data        string   `json:"data"`
		Topics      []string `json:"topics"`
		BlockNumber string   `json:"blockNumber"`
	})
	*target = logs
	return nil
}

func TestChangedPoolAddressesAtChunksInclusiveRanges(t *testing.T) {
	tests := []struct {
		name    string
		from    uint64
		to      uint64
		want    [][2]string
		chunks  uint64
		scanned uint64
	}{
		{name: "one block", from: 7, to: 7, want: [][2]string{{"0x7", "0x7"}}, chunks: 1, scanned: 1},
		{name: "exactly five blocks", from: 10, to: 14, want: [][2]string{{"0xa", "0xe"}}, chunks: 1, scanned: 5},
		{name: "six blocks", from: 10, to: 15, want: [][2]string{{"0xa", "0xe"}, {"0xf", "0xf"}}, chunks: 2, scanned: 6},
		{name: "twenty one blocks", from: 1, to: 21, want: [][2]string{{"0x1", "0x5"}, {"0x6", "0xa"}, {"0xb", "0xf"}, {"0x10", "0x14"}, {"0x15", "0x15"}}, chunks: 5, scanned: 21},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caller := &logFakeCaller{}
			d := NewDiscoverer(caller, "", "", nil)
			d.getLogsBlockChunkSize = 5
			changed, stats, err := d.ChangedPoolAddressesAtWithStats(context.Background(), []string{"0x0000000000000000000000000000000000000001"}, tt.from, tt.to)
			if err != nil {
				t.Fatal(err)
			}
			if stats.ChunksAttempted != tt.chunks || stats.ChunksSucceeded != tt.chunks || stats.BlocksScanned != tt.scanned {
				t.Fatalf("stats=%+v", stats)
			}
			if len(caller.calls) != len(tt.want) || len(changed) != int(tt.chunks) {
				t.Fatalf("calls=%d changed=%d", len(caller.calls), len(changed))
			}
			for i, want := range tt.want {
				if caller.calls[i]["fromBlock"] != want[0] || caller.calls[i]["toBlock"] != want[1] {
					t.Fatalf("chunk %d=%+v want=%+v", i, caller.calls[i], want)
				}
			}
		})
	}
}

func TestChangedPoolAddressesAtDeduplicatesAcrossChunks(t *testing.T) {
	caller := &logFakeCaller{duplicateAddress: "0x00000000000000000000000000000000000000aa"}
	d := NewDiscoverer(caller, "", "", nil)
	d.getLogsBlockChunkSize = 1
	changed, stats, err := d.ChangedPoolAddressesAtWithStats(context.Background(), []string{"0x0000000000000000000000000000000000000001"}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksSucceeded != 2 || len(changed) != 1 {
		t.Fatalf("stats=%+v changed=%+v", stats, changed)
	}
}

func TestChangedPoolAddressesAtStopsOnMiddleChunkFailure(t *testing.T) {
	caller := &logFakeCaller{errAtChunk: 2}
	d := NewDiscoverer(caller, "", "", nil)
	d.getLogsBlockChunkSize = 5
	_, stats, err := d.ChangedPoolAddressesAtWithStats(context.Background(), []string{"0x0000000000000000000000000000000000000001"}, 1, 11)
	if err == nil {
		t.Fatal("expected middle chunk failure")
	}
	if stats.ChunksAttempted != 2 || stats.ChunksSucceeded != 1 || stats.ChunksFailed != 1 || len(caller.calls) != 2 {
		t.Fatalf("stats=%+v calls=%d", stats, len(caller.calls))
	}
}

func TestChangedPoolAddressesAtHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	caller := &logFakeCaller{}
	d := NewDiscoverer(caller, "", "", nil)
	d.getLogsBlockChunkSize = 5
	_, stats, err := d.ChangedPoolAddressesAtWithStats(ctx, []string{"0x0000000000000000000000000000000000000001"}, 1, 5)
	if !errors.Is(err, context.Canceled) || stats.ChunksAttempted != 0 || len(caller.calls) != 0 {
		t.Fatalf("err=%v stats=%+v calls=%d", err, stats, len(caller.calls))
	}
}
