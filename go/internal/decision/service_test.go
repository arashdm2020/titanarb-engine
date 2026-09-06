package decision

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/nearmiss"
	"github.com/titanarb/titanarb-go/internal/pairintel"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
)

func fixture() (config.MarketConfig, routes.Route) {
	m := config.MarketConfig{Tokens: map[string]config.Token{"A": {Address: testUSD.Address, Decimals: 6}, "B": {Address: testETH.Address, Decimals: 18}}, ExecutionAssetNames: []string{"A"}, MarketAssetNames: []string{"A", "B"}}
	p := pools.Pool{Address: "0x0000000000000000000000000000000000000011", Token0: testUSD.Address, Token1: testETH.Address, DEX: pools.UniswapV3, Fee: 500, Liquidity: bi("10")}
	q := p
	q.Address = "0x0000000000000000000000000000000000000012"
	q.DEX = pools.CamelotV3
	q.Fee = 0
	return m, routes.Route{Symbols: []string{"A", "B", "A"}, Hops: []pools.Pool{p, q}}
}
func TestAddressIdentityNotSymbolAndDirection(t *testing.T) {
	m, r := fixture()
	k := RouteKey(r, m)
	m.Tokens["alias"] = m.Tokens["A"]
	r.Symbols = []string{"alias", "B", "alias"}
	if RouteKey(r, m) != k {
		t.Fatal("symbol identity")
	}
	r.Hops[0], r.Hops[1] = r.Hops[1], r.Hops[0]
	if RouteKey(r, m) == k {
		t.Fatal("venue path ignored")
	}
}
func TestUnknownDepthIsNotTVLAndVenueDedupe(t *testing.T) {
	now := time.Now()
	v := pairintel.Venue{DEX: pools.UniswapV3, QuoteSuccessEMA: 1, QuoteObservations: 10}
	p := pairintel.Pair{Key: pairintel.PairKey{Token0: testUSD.Address, Token1: testETH.Address}, Venues: map[string]pairintel.Venue{"p": v, "p2": v}, FirstObserved: now.Add(-2 * time.Hour), LastObserved: now, Confidence: 1}
	q := PairScore(p, now)
	if q.Venues != 1 || q.ShadowAdmitted {
		t.Fatalf("%+v", q)
	}
	if PoolScore(v, now).Depth != 0 {
		t.Fatal("invented depth")
	}
}
func TestWeakestEdgeCannotHideAndSelectionExplores(t *testing.T) {
	if weakestHarmonic([]float64{100, 1}) >= 2 {
		t.Fatal("strong edge hid weak edge")
	}
	m, r := fixture()
	rs := []routes.Route{r, r}
	q := r
	q.Hops = append([]pools.Pool(nil), r.Hops...)
	q.Hops[1] = q.Hops[0]
	rs = append(rs, q)
	h := map[string]History{RouteKey(r, m): {Evaluations: 10, LastSeen: time.Now(), GrossBPS: 10}}
	a := Rank(rs, m, nil, h, nil, time.Now())
	b := Rank(rs, m, nil, h, nil, time.Now())
	if len(a) != 2 || a[0].Key != b[0].Key {
		t.Fatal("dedupe/ranking")
	}
	out := Select(a, 2, 0)
	if len(out) != 2 || out[0].Bucket != "explore" || out[0].Observations != 0 || out[0].Key == out[1].Key {
		t.Fatalf("%+v", out)
	}
}
func TestHistoryNormalizedAndRecovers(t *testing.T) {
	now := time.Now()
	h := UpdateHistory(History{}, nearmiss.Record{AmountIn: bi("1000000"), GrossProfit: bi("-900000"), NetProfit: bi("-910000"), QuoteSuccessful: true}, now)
	if h.GrossBPS != -9000 {
		t.Fatal(h)
	}
	// A .2 EMA needs > log(100/9100)/log(.8) = 20.2 observations
	// to cross zero after a -9000 bps loss and subsequent +100 bps samples.
	for i := 0; i < 30; i++ {
		h = UpdateHistory(h, nearmiss.Record{AmountIn: bi("1000000000000000000"), GrossProfit: bi("10000000000000000"), NetProfit: bi("5000000000000000"), QuoteSuccessful: true}, now)
	}
	if h.GrossBPS <= 0 {
		t.Fatal("permanent blacklist")
	}
}
func TestSnapshotDetachedAndQueuesBounded(t *testing.T) {
	m, r := fixture()
	s := NewService(nil, pairintel.NewMemory(pairintel.DefaultConfig()), t.TempDir(), nil)
	s.Offer(Input{Block: 5, Market: m, Routes: []routes.Route{r}, Amounts: map[string]*big.Int{"A": bi("100")}})
	m.Tokens["A"] = config.Token{}
	r.Hops[0].Liquidity.SetInt64(0)
	in := <-s.in
	if in.Market.Tokens["A"].Address == "" || in.Routes[0].Hops[0].Liquidity.Sign() == 0 {
		t.Fatal("aliased live input")
	}
	for i := 0; i < 100; i++ {
		s.Offer(Input{Block: uint64(i)})
	}
	if len(s.in) != 1 || s.dropped.Load() == 0 {
		t.Fatal("queue bound")
	}
}

type fakeReader struct {
	calls int
	block string
}

func (f *fakeReader) EthCallAt(_ context.Context, _ map[string]string, b string) (string, error) {
	f.calls++
	f.block = b
	return "0x", nil
}
func TestPinnedReadPriorityBudgetAndCancel(t *testing.T) {
	f := &fakeReader{}
	busy := true
	s := NewService(f, nil, t.TempDir(), func() bool { return busy })
	s.ReadLimit = 1
	p := pinnedReader{s: s, block: 123}
	if _, err := p.EthCall(context.Background(), nil); err == nil || f.calls != 0 {
		t.Fatal("starves live")
	}
	busy = false
	if _, err := p.EthCall(context.Background(), nil); err != nil || f.calls != 1 || f.block != "0x7b" {
		t.Fatal("pinned block", err)
	}
	if _, err := p.EthCall(context.Background(), nil); err == nil || f.calls != 1 {
		t.Fatal("budget")
	}
	s.reads = 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.EthCall(ctx, nil); err == nil || f.calls != 1 {
		t.Fatal("cancel")
	}
}
func TestNoClientIsRequiredForShadowAndPersistence(t *testing.T) {
	dir := t.TempDir()
	s := NewService(nil, pairintel.NewMemory(pairintel.DefaultConfig()), dir, nil)
	m, r := fixture()
	s.history["x"] = History{Evaluations: 2, LastSeen: time.Now()}
	s.saveHistory()
	n := NewService(nil, s.pairs, dir, nil)
	n.loadHistory()
	if n.history["x"].Evaluations != 2 {
		t.Fatal("restart")
	}
	s.analyze(context.Background(), Input{Block: 100, Market: m, Routes: []routes.Route{r}})
	b, err := os.ReadFile(filepath.Join(dir, "TITANARB_PHASE2_MARKET_QUALITY.json"))
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if json.Unmarshal(b, &v) != nil || v["mode"] != "shadow" || v["inventory_execution_enabled"] != false {
		t.Fatal(string(b))
	}
}
func TestConcurrentObservationsDoNotBlockOrRace(t *testing.T) {
	m, r := fixture()
	s := NewService(nil, pairintel.NewMemory(pairintel.DefaultConfig()), t.TempDir(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				s.Observe(r, m, nearmiss.Record{AmountIn: bi("100"), AmountOut: bi("99"), GrossProfit: bi("-1"), NetProfit: bi("-2")}, 1)
			}
		}()
	}
	wg.Wait()
	cancel()
	<-done
}
func TestUnresolvedPoolExcludedFromShadow(t *testing.T) {
	m, r := fixture()
	out, _ := shadowRoutes(Input{Market: m, Routes: []routes.Route{r}, Unresolved: map[string]bool{r.Hops[0].Address: true}}, nil, nil, nil)
	if len(out) != 0 {
		t.Fatal("unresolved pool used")
	}
}

func TestSamplingMemoryBounded(t *testing.T) {
	s := NewService(nil, nil, t.TempDir(), nil)
	for i := 0; i < 5000; i++ {
		s.rememberShadow(fmt.Sprint(i))
	}
	if len(s.shadowSeen) != 4096 {
		t.Fatal(len(s.shadowSeen))
	}
}
