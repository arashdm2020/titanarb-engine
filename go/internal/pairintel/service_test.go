package pairintel

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/pools"
)

type fakeCaller struct {
	code     string
	decimals string
}

func (f fakeCaller) Call(_ context.Context, method string, _ any, out any) error {
	if method == "eth_getCode" {
		*(out.(*string)) = f.code
	}
	return nil
}
func (f fakeCaller) EthCall(context.Context, map[string]string) (string, error) {
	if f.decimals == "error" {
		return "", fmt.Errorf("bad")
	}
	return f.decimals, nil
}
func (fakeCaller) BlockNumber(context.Context) (uint64, error) { return 100, nil }

func TestValidateTokenRejectsNoCodeAndInvalidDecimals(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRPS = 1000
	m := NewMemory(cfg)
	s := NewService(m, fakeCaller{code: "0x", decimals: wordHex(6)}, nil, nil, nil, "", nil)
	if s.validateToken(context.Background(), tokenA) {
		t.Fatal("no-code token accepted")
	}
	s = NewService(m, fakeCaller{code: "0x1234", decimals: wordHex(37)}, nil, nil, nil, "", nil)
	if s.validateToken(context.Background(), tokenA) {
		t.Fatal("invalid decimals accepted")
	}
}

func TestDuplicateFactoryEventHandling(t *testing.T) {
	s := NewService(NewMemory(DefaultConfig()), nil, nil, nil, nil, "", nil)
	if s.duplicate(poolA) {
		t.Fatal("first event duplicate")
	}
	if !s.duplicate(strings.ToUpper(poolA)) {
		t.Fatal("duplicate not detected")
	}
}

func TestServiceReusesNewestObservedHead(t *testing.T) {
	s := NewService(NewMemory(DefaultConfig()), nil, nil, nil, nil, "", nil)
	s.ObservePool(pools.Pool{LastUpdatedBlock: 101})
	s.ObserveSwap(pools.Swap{Block: 103})
	s.ObservePool(pools.Pool{LastUpdatedBlock: 102})
	if got := s.observedHead.Load(); got != 103 {
		t.Fatalf("observed head=%d want 103", got)
	}
}

func TestFactoryEventDecoding(t *testing.T) {
	topicAddress := func(a string) string { return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(a, "0x") }
	data := "0x" + strings.Repeat("0", 64) + strings.Repeat("0", 24) + strings.TrimPrefix(poolA, "0x")
	entry := factoryLog{Topics: []string{uniPoolCreated, topicAddress(tokenA), topicAddress(tokenB), "0x1f4"}, Data: data, BlockNumber: "0x64"}
	got, ok := decodeFactoryLog(Factory{DEX: "uniswap_v3"}, entry)
	if !ok || got.Pool != poolA || got.Fee != 500 || got.Block != 100 {
		t.Fatalf("decoded=%+v ok=%t", got, ok)
	}
}

func TestPairRPCPacing(t *testing.T) {
	l := newPacedLimiter(100, 1)
	ctx := context.Background()
	start := time.Now()
	if err := l.Wait(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 8*time.Millisecond {
		t.Fatalf("limiter did not pace: %v", time.Since(start))
	}
}

func TestPairConfigDefaultsToBoundedShadow(t *testing.T) {
	cfg := ConfigFromLookup(func(string) string { return "" })
	if !cfg.Enabled || cfg.Mode != "shadow" || cfg.MaxTrackedPairs != 16 || cfg.MaxShadowPairs != 8 || cfg.MaxDynamicAssets != 4 || cfg.MaxLivePairs != 2 || cfg.MaxLiveDynamicAssets != 2 || cfg.MinScore != 65 || cfg.MinConfidence != .70 || cfg.MinObservation != time.Hour || cfg.MaxRPS != .5 || cfg.Burst != 1 {
		t.Fatalf("defaults=%+v", cfg)
	}
}

func TestFactoryLogsChunksProviderSafeRanges(t *testing.T) {
	t.Setenv("TITANARB_GETLOGS_BLOCK_CHUNK_SIZE", "5")
	caller := &factoryLogCaller{}
	cfg := DefaultConfig()
	cfg.MaxRPS = 1000
	s := NewService(NewMemory(cfg), caller, nil, nil, nil, "", nil)
	got, err := s.factoryLogs(context.Background(), Factory{Name: "uni", Address: poolA, DEX: pools.UniswapV3}, 10, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("logs=%d want 5", len(got))
	}
	want := [][2]string{{"0xa", "0xe"}, {"0xf", "0x13"}, {"0x14", "0x18"}, {"0x19", "0x1d"}, {"0x1e", "0x1e"}}
	if !reflect.DeepEqual(caller.ranges, want) {
		t.Fatalf("ranges=%v want %v", caller.ranges, want)
	}
}

func TestFactoryLogsStopsOnChunkFailureWithoutSkippingCheckpointRange(t *testing.T) {
	t.Setenv("TITANARB_GETLOGS_BLOCK_CHUNK_SIZE", "5")
	caller := &factoryLogCaller{failAt: 2}
	cfg := DefaultConfig()
	cfg.MaxRPS = 1000
	s := NewService(NewMemory(cfg), caller, nil, nil, nil, "", nil)
	got, err := s.factoryLogs(context.Background(), Factory{Name: "uni", Address: poolA, DEX: pools.UniswapV3}, 1, 11)
	if err == nil {
		t.Fatal("expected middle chunk failure")
	}
	if len(got) != 1 {
		t.Fatalf("logs before failure=%d want 1", len(got))
	}
	want := [][2]string{{"0x1", "0x5"}, {"0x6", "0xa"}}
	if !reflect.DeepEqual(caller.ranges, want) {
		t.Fatalf("ranges=%v want %v", caller.ranges, want)
	}
}

type factoryLogCaller struct {
	ranges [][2]string
	failAt int
}

func (f *factoryLogCaller) Call(_ context.Context, method string, params any, out any) error {
	if method != "eth_getLogs" {
		return nil
	}
	filter := params.([]any)[0].(map[string]any)
	f.ranges = append(f.ranges, [2]string{filter["fromBlock"].(string), filter["toBlock"].(string)})
	if f.failAt > 0 && len(f.ranges) == f.failAt {
		return fmt.Errorf("range limit")
	}
	*(out.(*[]factoryLog)) = []factoryLog{{Address: poolA, BlockNumber: filter["fromBlock"].(string)}}
	return nil
}

func (f *factoryLogCaller) EthCall(context.Context, map[string]string) (string, error) {
	return "0x", nil
}

func (f *factoryLogCaller) BlockNumber(context.Context) (uint64, error) {
	return 0, nil
}

func wordHex(v uint64) string { return fmt.Sprintf("0x%064x", v) }
