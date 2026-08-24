package market

import (
	"math/big"
	"strings"
	"testing"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/nearmiss"
	"github.com/titanarb/titanarb-go/internal/optimizer"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/routes"
)

func TestUniverseFeedbackUsesDynamicMarketAssetsOnly(t *testing.T) {
	engine := &Engine{market: config.MarketConfig{ExecutionAssetNames: []string{"USDC"}, Tokens: map[string]config.Token{"USDC": {Symbol: "USDC"}, "DYN": {Symbol: "DYN"}}}}
	var asset string
	var evaluations, useful uint64
	engine.SetUniverseFeedback(func(a string, e, u uint64) { asset, evaluations, useful = a, e, u })
	engine.publishUniverseFeedback([]evaluatedRoute{{route: routes.Route{Symbols: []string{"USDC", "DYN", "USDC"}}, quoteSuccessful: true, nearMiss: &nearmiss.Record{Score: 3000}}})
	if asset != "DYN" || evaluations != 1 || useful != 1 {
		t.Fatalf("feedback=%s %d %d", asset, evaluations, useful)
	}
}

func TestQueuedMarketConfigAppliesAtomicallyWithoutExecutionExpansion(t *testing.T) {
	base := config.MarketConfig{ExecutionAssetNames: []string{"USDC"}, Tokens: map[string]config.Token{"USDC": {Symbol: "USDC"}}}
	e := &Engine{market: base, executionAssets: []string{"USDC"}}
	next := base
	next.Tokens = map[string]config.Token{"USDC": {Symbol: "USDC"}, "DYN": {Symbol: "DYN"}}
	next.MarketAssetNames = []string{"USDC", "DYN"}
	e.QueueMarketConfig(next)
	if e.market.Tokens["DYN"].Symbol != "" {
		t.Fatal("pending config leaked before cycle boundary")
	}
	if !e.applyPendingMarket() {
		t.Fatal("pending config not applied")
	}
	if got := strings.Join(e.market.ExecutionAssets(), ","); got != "USDC" {
		t.Fatalf("execution assets changed: %s", got)
	}
	if got := strings.Join(e.market.MarketAssets(), ","); got != "DYN,USDC" {
		t.Fatalf("market assets=%s", got)
	}
}

func TestLiveMarketAllowsOnlyAdmittedDynamicPairEdges(t *testing.T) {
	base := config.MarketConfig{
		ExecutionAssetNames: []string{"USDC"},
		Tokens: map[string]config.Token{
			"USDC": {Symbol: "USDC", Address: "0x0000000000000000000000000000000000000001"},
		},
	}
	e := &Engine{market: base, executionAssets: []string{"USDC"}}
	next := cloneTestMarket(base)
	next.Tokens["DYN_A"] = config.Token{Symbol: "DYN_A", Address: "0x0000000000000000000000000000000000000002"}
	next.Tokens["DYN_B"] = config.Token{Symbol: "DYN_B", Address: "0x0000000000000000000000000000000000000003"}
	next.MarketAssetNames = []string{"USDC", "DYN_A", "DYN_B"}
	e.QueueLiveMarket(next, []string{"0x0000000000000000000000000000000000000001:0x0000000000000000000000000000000000000002"}, []string{"DYN_A", "DYN_B"})
	if !e.applyPendingMarket() {
		t.Fatal("pending live graph not applied")
	}
	if !e.marketPairAllowed("USDC", "DYN_A") {
		t.Fatal("admitted edge rejected")
	}
	if e.marketPairAllowed("USDC", "DYN_B") || e.marketPairAllowed("DYN_A", "DYN_B") {
		t.Fatal("unadmitted dynamic edge accepted")
	}
}

func TestRoutePairQualityPenalizesWeakestEdge(t *testing.T) {
	e := &Engine{market: config.MarketConfig{Tokens: map[string]config.Token{
		"A": {Address: "a"}, "B": {Address: "b"}, "C": {Address: "c"},
	}}}
	e.pairScore = func(a, b string) (float64, bool) {
		key := a + b
		if key == "ca" || key == "ac" {
			return 10, true
		}
		return 90, true
	}
	weak := e.routePairQuality(routes.Route{Symbols: []string{"A", "B", "C", "A"}})
	strong := e.routePairQuality(routes.Route{Symbols: []string{"A", "B", "A"}})
	if weak >= strong || weak >= 50 {
		t.Fatalf("weak edge was hidden: weak=%f strong=%f", weak, strong)
	}
}

func cloneTestMarket(in config.MarketConfig) config.MarketConfig {
	out := in
	out.Tokens = make(map[string]config.Token, len(in.Tokens))
	for key, token := range in.Tokens {
		out.Tokens[key] = token
	}
	return out
}

func TestRoutesAffectedByDirtyPool(t *testing.T) {
	p1 := pools.Pool{Address: "0x0000000000000000000000000000000000000001"}
	p2 := pools.Pool{Address: "0x0000000000000000000000000000000000000002"}
	all := []routes.Route{
		{Symbols: []string{"A", "B", "A"}, Hops: []pools.Pool{p1, p1}},
		{Symbols: []string{"C", "D", "C"}, Hops: []pools.Pool{p2, p2}},
	}
	affected := routesAffectedBy(all, map[string]struct{}{p1.Address: {}}, false)
	if len(affected) != 1 || affected[0].Symbols[0] != "A" {
		t.Fatalf("dirty routing mismatch: %#v", affected)
	}
	if got := routesAffectedBy(all, nil, true); len(got) != 2 {
		t.Fatalf("full reconciliation returned %d routes", len(got))
	}
}

func TestPoolChangedUsesMutableState(t *testing.T) {
	before := pools.Pool{Liquidity: big.NewInt(10), SqrtPriceX96: big.NewInt(20)}
	if poolChanged(before, before) {
		t.Fatal("identical state marked dirty")
	}
	after := before
	after.SqrtPriceX96 = big.NewInt(21)
	if !poolChanged(before, after) {
		t.Fatal("price change not marked dirty")
	}
}

func TestChangedPoolAddressesOnlyInvalidatesChangedState(t *testing.T) {
	unchanged := pools.Pool{Address: "0x1", Liquidity: big.NewInt(10), SqrtPriceX96: big.NewInt(20)}
	changedBefore := pools.Pool{Address: "0x2", Liquidity: big.NewInt(10), SqrtPriceX96: big.NewInt(20)}
	changedAfter := changedBefore
	changedAfter.SqrtPriceX96 = big.NewInt(21)
	added := pools.Pool{Address: "0x3", Liquidity: big.NewInt(1), SqrtPriceX96: big.NewInt(1)}
	removed := pools.Pool{Address: "0x4", Liquidity: big.NewInt(1), SqrtPriceX96: big.NewInt(1)}
	dirty := changedPoolAddresses([]pools.Pool{unchanged, changedBefore, removed}, []pools.Pool{unchanged, changedAfter, added})
	if len(dirty) != 3 {
		t.Fatalf("unexpected dirty set: %+v", dirty)
	}
	if _, ok := dirty["0x1"]; ok {
		t.Fatal("unchanged pool was invalidated")
	}
	for _, address := range []string{"0x2", "0x3", "0x4"} {
		if _, ok := dirty[address]; !ok {
			t.Fatalf("missing dirty pool %s", address)
		}
	}
}

func TestUnchangedLoggedPoolCanBeRemovedFromDirtySet(t *testing.T) {
	p := pools.Pool{Address: "0x0000000000000000000000000000000000000001", Liquidity: big.NewInt(10), SqrtPriceX96: big.NewInt(20)}
	dirty := map[string]struct{}{p.Address: {}}
	if !poolChanged(p, p) {
		delete(dirty, p.Address)
	}
	if len(dirty) != 0 {
		t.Fatalf("unchanged logged pool remained dirty: %+v", dirty)
	}
}

func TestIdleCycleDoesNotAdvanceFullReconcileCounter(t *testing.T) {
	if shouldAdvanceFullReconcileCounter(nil) {
		t.Fatal("nil dirty set advanced full reconcile counter")
	}
	if shouldAdvanceFullReconcileCounter(map[string]struct{}{}) {
		t.Fatal("empty dirty set advanced full reconcile counter")
	}
	if !shouldAdvanceFullReconcileCounter(map[string]struct{}{"0x1": {}}) {
		t.Fatal("dirty set did not advance full reconcile counter")
	}
}

func TestIncrementalRefreshBatchesSmallBlockDeltas(t *testing.T) {
	if !shouldDeferIncrementalRefresh(100, 107, 8) {
		t.Fatal("small block delta should be batched")
	}
	if shouldDeferIncrementalRefresh(100, 108, 8) {
		t.Fatal("batch boundary should refresh")
	}
	if shouldDeferIncrementalRefresh(0, 1, 8) {
		t.Fatal("first observed block must not be deferred")
	}
}

func TestRefreshRoutesDropsInactivePool(t *testing.T) {
	p := pools.Pool{Address: "0x0000000000000000000000000000000000000001", Liquidity: big.NewInt(10)}
	route := routes.Route{Symbols: []string{"A", "B", "A"}, Hops: []pools.Pool{p, p}}
	if got := refreshRoutes([]routes.Route{route}, nil); len(got) != 0 {
		t.Fatalf("route using inactive pool remained executable: %#v", got)
	}
	if got := refreshRoutes([]routes.Route{route}, []pools.Pool{p}); len(got) != 1 {
		t.Fatalf("active route was removed: %#v", got)
	}
}

func TestCapLoanMaxUsesAaveLiquidityAsUpperBound(t *testing.T) {
	configured := big.NewInt(1_000)
	if got := capLoanMax(configured, big.NewInt(250)); got.Cmp(big.NewInt(250)) != 0 {
		t.Fatalf("available liquidity did not cap max: %s", got)
	}
	if got := capLoanMax(configured, big.NewInt(2_000)); got.Cmp(configured) != 0 {
		t.Fatalf("larger liquidity expanded max: %s", got)
	}
	if got := capLoanMax(configured, nil); got.Cmp(configured) != 0 {
		t.Fatalf("nil liquidity changed max: %s", got)
	}
}

func TestBoundedRoutesCapsHotPathWork(t *testing.T) {
	var candidates []routes.Route
	for i := 0; i < maxEvaluationRoutesPerAsset+5; i++ {
		candidates = append(candidates, routes.Route{Symbols: []string{"A", "B", "A"}})
	}
	if got := boundedRoutes(candidates, maxEvaluationRoutesPerAsset); len(got) != maxEvaluationRoutesPerAsset {
		t.Fatalf("evaluation candidates were not capped: %d", len(got))
	}
	if got := boundedRoutes(candidates, maxOptimizerRoutesPerAsset); len(got) != maxOptimizerRoutesPerAsset {
		t.Fatalf("optimizer candidates were not capped: %d", len(got))
	}
}

func TestSelectOptimizerCandidatesPrefersBestNearMisses(t *testing.T) {
	closeMiss := &nearmiss.Record{GapToProfit: big.NewInt(1), MinProfit: big.NewInt(10), NetProfit: big.NewInt(-9)}
	evaluated := []evaluatedRoute{
		{route: routes.Route{Symbols: []string{"ARB", "USDC", "ARB"}}, score: big.NewInt(-50), nearMiss: closeMiss},
		{route: routes.Route{Symbols: []string{"WETH", "ARB", "WETH"}}, score: big.NewInt(10), nearMiss: closeMiss},
		{route: routes.Route{Symbols: []string{"USDC", "WETH", "USDC"}}, score: big.NewInt(10), nearMiss: closeMiss},
		{route: routes.Route{Symbols: []string{"USDT", "ARB", "USDT"}}, score: big.NewInt(-5), nearMiss: closeMiss},
	}

	selected := selectOptimizerCandidates(evaluated, 2)
	if len(selected) != 2 {
		t.Fatalf("selected %d optimizer candidates", len(selected))
	}

	got := []string{selected[0].route.String(), selected[1].route.String()}
	want := []string{"USDC -> WETH -> USDC", "WETH -> ARB -> WETH"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate %d mismatch: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestSelectOptimizerCandidatesUsesRouteScoreBeforeRawProfit(t *testing.T) {
	closeMiss := &nearmiss.Record{GapToProfit: big.NewInt(1), MinProfit: big.NewInt(10), NetProfit: big.NewInt(-9)}
	evaluated := []evaluatedRoute{
		{route: routes.Route{Symbols: []string{"USDC", "WETH", "USDC"}}, score: big.NewInt(100), routeScore: 1_000, nearMiss: closeMiss},
		{route: routes.Route{Symbols: []string{"ARB", "WETH", "ARB"}}, score: big.NewInt(50), routeScore: 5_000, nearMiss: closeMiss},
	}
	selected := selectOptimizerCandidates(evaluated, 1)
	if len(selected) != 1 || selected[0].route.String() != "ARB -> WETH -> ARB" {
		t.Fatalf("route score did not drive optimizer priority: %+v", selected)
	}
}

func TestSamplesForScoreAllocatesMoreBudgetToHighScoreRoutes(t *testing.T) {
	base := 8
	high := samplesForScore(4_500, base)
	low := samplesForScore(1_000, base)
	if high != base || low != 2 {
		t.Fatalf("unexpected sample allocation high=%d base=%d low=%d", high, base, low)
	}
}

func TestOptimizerProbeStopsStructurallyBadRoute(t *testing.T) {
	candidate := optimizerCandidate{score: 1_000, minProfit: big.NewInt(1_000_000)}
	best := optimizer.OptimalLoan{NetProfit: big.NewInt(-1)}
	if !shouldStopOptimizerAfterProbe(candidate, best, big.NewInt(1_000_000_000)) {
		t.Fatal("low-score losing route should stop after probe")
	}
	best.NetProfit = big.NewInt(1)
	if shouldStopOptimizerAfterProbe(candidate, best, big.NewInt(1_000_000_000)) {
		t.Fatal("positive probe result must remain eligible")
	}
}

func TestSelectOptimizerCandidatesSkipsFarBelowThreshold(t *testing.T) {
	route := routes.Route{Symbols: []string{"USDC", "ARB", "USDC"}}
	far := &nearmiss.Record{GapToProfit: big.NewInt(100), MinProfit: big.NewInt(10), NetProfit: big.NewInt(-90)}
	if got := selectOptimizerCandidates([]evaluatedRoute{{route: route, score: big.NewInt(-90), routeScore: 6_000, nearMiss: far}}, 1); len(got) != 0 {
		t.Fatalf("far-below route received optimizer budget: %+v", got)
	}
	close := &nearmiss.Record{GapToProfit: big.NewInt(20), MinProfit: big.NewInt(10), NetProfit: big.NewInt(-10)}
	if got := selectOptimizerCandidates([]evaluatedRoute{{route: route, score: big.NewInt(-10), routeScore: 6_000, nearMiss: close}}, 1); len(got) != 1 {
		t.Fatalf("near-threshold route did not receive optimizer budget: %+v", got)
	}
}

func TestSearchOptionsNormalizeToSafeDefaults(t *testing.T) {
	got := (SearchOptions{}).Normalized()
	defaults := DefaultSearchOptions()
	if got != defaults {
		t.Fatalf("zero search options did not normalize to defaults: got %#v want %#v", got, defaults)
	}
}

func TestSearchOptionsPreserveExplicitRollbackFlags(t *testing.T) {
	got := (SearchOptions{OptimizationFlagsSet: true, PersistentQuoteCache: false, AdaptiveOptimizer: false, EarlyStop: false}).Normalized()
	if got.PersistentQuoteCache || got.AdaptiveOptimizer || got.EarlyStop {
		t.Fatalf("rollback flags were overwritten: %#v", got)
	}
}

func TestRefineOptimizerRangeNarrowsAroundProbeWinner(t *testing.T) {
	minimum, maximum := refineOptimizerRange(big.NewInt(100), big.NewInt(900), big.NewInt(500))
	if minimum.Cmp(big.NewInt(300)) != 0 || maximum.Cmp(big.NewInt(700)) != 0 {
		t.Fatalf("unexpected refined range: %s..%s", minimum, maximum)
	}
}

func TestScoreDecilesRelatePreQuoteScoreToGap(t *testing.T) {
	input := []evaluatedRoute{
		{route: routes.Route{Symbols: []string{"A", "B", "A"}}, preQuoteScore: 9_000, quoteSuccessful: true, nearMiss: &nearmiss.Record{GapToProfit: big.NewInt(1), MinProfit: big.NewInt(10)}},
		{route: routes.Route{Symbols: []string{"C", "D", "C"}}, preQuoteScore: 1_000, quoteSuccessful: true, nearMiss: &nearmiss.Record{GapToProfit: big.NewInt(9), MinProfit: big.NewInt(10)}},
	}
	quality := scoreDecileQuality(input)
	if quality["d1"]["best_gap_to_threshold_bps"].(float64) >= quality["d6"]["best_gap_to_threshold_bps"].(float64) {
		t.Fatalf("score deciles did not preserve economic ordering: %+v", quality)
	}
}

func TestSelectEvaluationCandidatesCrossVenueRanksHigherWhenSignalsEqual(t *testing.T) {
	engine := &Engine{routeMemory: nearmiss.NewRouteMemory(16)}
	same := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.UniswapV3}, "1", "2")
	cross := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.CamelotV3}, "3", "4")
	selected, stats := engine.selectEvaluationCandidates([]routes.Route{same, cross}, SearchOptions{EvaluationRoutesPerAsset: 1, ExploreRatioBPS: 0}.Normalized(), nil, 1, 100)
	if len(selected) != 1 || routeKey(selected[0]) != routeKey(cross) {
		t.Fatalf("cross-venue route was not selected first: %+v", selected)
	}
	if stats.CrossVenue != 1 || stats.SameDEX != 0 || stats.Exploit != 1 {
		t.Fatalf("selection telemetry mismatch: %+v", stats)
	}
}

func TestSelectEvaluationCandidatesSameDEXRemainsEligible(t *testing.T) {
	engine := &Engine{routeMemory: nearmiss.NewRouteMemory(16)}
	same := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.UniswapV3}, "1", "2")
	selected, stats := engine.selectEvaluationCandidates([]routes.Route{same}, SearchOptions{EvaluationRoutesPerAsset: 4, ExploreRatioBPS: 0}.Normalized(), nil, 1, 100)
	if len(selected) != 1 || routeKey(selected[0]) != routeKey(same) {
		t.Fatalf("same-DEX route was incorrectly excluded: %+v", selected)
	}
	if stats.SameDEX != 1 {
		t.Fatalf("same-DEX telemetry mismatch: %+v", stats)
	}
}

func TestSelectEvaluationCandidatesHistoricalNearMissImprovesPriority(t *testing.T) {
	engine := &Engine{routeMemory: nearmiss.NewRouteMemory(16)}
	near := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.CamelotV3}, "1", "2")
	unknown := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.CamelotV3}, "3", "4")
	engine.routeMemory.Observe(routeKey(near), nearmiss.Record{CrossVenue: true, QuoteSuccessful: true, SufficientLiquidity: true, HopCount: 2, GapToProfit: big.NewInt(1_000), Score: 5_000}, 90)
	selected, _ := engine.selectEvaluationCandidates([]routes.Route{unknown, near}, SearchOptions{EvaluationRoutesPerAsset: 1, ExploreRatioBPS: 0}.Normalized(), nil, 1, 100)
	if len(selected) != 1 || routeKey(selected[0]) != routeKey(near) {
		t.Fatalf("near-miss history did not drive pre-quote priority: %+v", selected)
	}
}

func TestSelectEvaluationCandidatesExploresUnseenRoutes(t *testing.T) {
	engine := &Engine{routeMemory: nearmiss.NewRouteMemory(16)}
	known := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.CamelotV3}, "1", "2")
	unseen := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.UniswapV3}, "3", "4")
	engine.routeMemory.Observe(routeKey(known), nearmiss.Record{CrossVenue: true, QuoteSuccessful: true, SufficientLiquidity: true, HopCount: 2, GapToProfit: big.NewInt(1_000), Score: 6_000}, 90)
	selected, stats := engine.selectEvaluationCandidates([]routes.Route{known, unseen}, SearchOptions{EvaluationRoutesPerAsset: 2, ExploreRatioBPS: 5_000}.Normalized(), nil, 1, 100)
	if len(selected) != 2 {
		t.Fatalf("selection did not fill budget: %+v", selected)
	}
	if stats.Explore != 1 || routeKey(selected[1]) != routeKey(unseen) {
		t.Fatalf("unseen route did not receive exploration slot: selected=%+v stats=%+v", selected, stats)
	}
}

func TestSelectEvaluationCandidatesNoDuplicateRoutes(t *testing.T) {
	engine := &Engine{routeMemory: nearmiss.NewRouteMemory(16)}
	route := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.CamelotV3}, "1", "2")
	selected, _ := engine.selectEvaluationCandidates([]routes.Route{route, route, route}, SearchOptions{EvaluationRoutesPerAsset: 3, ExploreRatioBPS: 5_000}.Normalized(), nil, 1, 100)
	if len(selected) != 1 {
		t.Fatalf("duplicate route admitted: %d", len(selected))
	}
}

func TestSelectEvaluationCandidatesDisabledUsesLegacyOrder(t *testing.T) {
	engine := &Engine{routeMemory: nearmiss.NewRouteMemory(16)}
	same := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.UniswapV3}, "1", "2")
	cross := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.CamelotV3}, "3", "4")
	selected, _ := engine.selectEvaluationCandidates([]routes.Route{same, cross}, SearchOptions{EvaluationRoutesPerAsset: 1, DisablePreQuoteRanking: true, ExploreRatioBPS: 5_000}.Normalized(), nil, 1, 100)
	if len(selected) != 1 || routeKey(selected[0]) != routeKey(same) {
		t.Fatalf("disabled scorer did not preserve legacy first route: %+v", selected)
	}
}

func TestPreQuoteSelectionImprovesCandidateClassWithoutIncreasingBudget(t *testing.T) {
	engine := &Engine{routeMemory: nearmiss.NewRouteMemory(128)}
	var candidates []routes.Route
	for i := 0; i < 12; i++ {
		candidates = append(candidates, testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.UniswapV3}, string(rune('a'+i)), string(rune('m'+i))))
	}
	for i := 0; i < 12; i++ {
		route := testRoute("USDC", []pools.DEX{pools.UniswapV3, pools.CamelotV3}, string(rune('A'+i)), string(rune('M'+i)))
		engine.routeMemory.Observe(routeKey(route), nearmiss.Record{CrossVenue: true, QuoteSuccessful: true, SufficientLiquidity: true, HopCount: 2, GapToProfit: big.NewInt(1_000), Score: 5_000}, 90)
		candidates = append(candidates, route)
	}
	legacy, legacyStats := engine.selectEvaluationCandidates(candidates, SearchOptions{EvaluationRoutesPerAsset: 8, DisablePreQuoteRanking: true, ExploreRatioBPS: 2_000}.Normalized(), nil, 1, 100)
	updated, updatedStats := engine.selectEvaluationCandidates(candidates, SearchOptions{EvaluationRoutesPerAsset: 8, ExploreRatioBPS: 2_000}.Normalized(), nil, 1, 100)
	if len(legacy) != len(updated) || len(updated) != 8 {
		t.Fatalf("selection changed quote budget: legacy=%d updated=%d", len(legacy), len(updated))
	}
	if updatedStats.CrossVenue <= legacyStats.CrossVenue {
		t.Fatalf("pre-quote selection did not improve cross-venue share: legacy=%+v updated=%+v", legacyStats, updatedStats)
	}
	if updatedStats.Explore == 0 {
		t.Fatalf("exploration slots were not used: %+v", updatedStats)
	}
}

func testRoute(start string, dexes []pools.DEX, suffixes ...string) routes.Route {
	symbols := []string{start}
	for i := range dexes {
		symbols = append(symbols, string(rune('A'+i)))
	}
	symbols[len(symbols)-1] = start
	hops := make([]pools.Pool, len(dexes))
	for i, dexName := range dexes {
		suffix := "0"
		if i < len(suffixes) {
			suffix = suffixes[i]
		}
		hops[i] = pools.Pool{Address: "0x000000000000000000000000000000000000000" + suffix, DEX: dexName, Liquidity: big.NewInt(1)}
	}
	return routes.Route{Symbols: symbols, Hops: hops}
}
