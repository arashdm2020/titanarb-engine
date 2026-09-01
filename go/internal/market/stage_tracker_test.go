package market

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/titanarb/titanarb-go/internal/cache"
	"github.com/titanarb/titanarb-go/internal/pools"
)

func TestDetectorStageTrackerStartsAtCycleStart(t *testing.T) {
	tracker := newDetectorStageTracker(123, 99, func() uint64 { return 124 })
	snapshot := tracker.Snapshot()
	if snapshot.CurrentStage != stageCycleStart {
		t.Fatalf("stage=%s", snapshot.CurrentStage)
	}
	if snapshot.SourceBlock != 123 || snapshot.LastStateBlockBeforeCycle != 99 {
		t.Fatalf("unexpected block snapshot: %+v", snapshot)
	}
	if snapshot.LatestBlockAtStageStart != 124 {
		t.Fatalf("latest at stage start=%d", snapshot.LatestBlockAtStageStart)
	}
}

func TestDetectorStageTransitionsPreserveDuration(t *testing.T) {
	tracker := newDetectorStageTracker(1, 0, nil)
	time.Sleep(time.Millisecond)
	tracker.Stage(stageGetLogsDirtyDetection)
	time.Sleep(time.Millisecond)
	tracker.Stage(stagePoolRefresh)
	snapshot := tracker.Snapshot()
	if snapshot.CurrentStage != stagePoolRefresh {
		t.Fatalf("stage=%s", snapshot.CurrentStage)
	}
	if snapshot.LastCompletedStage != stageGetLogsDirtyDetection {
		t.Fatalf("last completed=%s", snapshot.LastCompletedStage)
	}
	if snapshot.LastCompletedStageDuration <= 0 {
		t.Fatalf("duration was not recorded: %+v", snapshot)
	}
}

func TestDetectorStageTrackerCapturesPartialGetLogsAndPoolRefresh(t *testing.T) {
	tracker := newDetectorStageTracker(12, 10, nil)
	tracker.StartGetLogs(11, 12, 5)
	stats := refreshStats{Logs: pools.LogQueryStats{ChunksAttempted: 1, ChunksSucceeded: 0, ChunksFailed: 1, BlocksScanned: 2}}
	tracker.FinishGetLogs(stats)
	tracker.StartPoolRefresh(2)
	tracker.PoolRefreshResult(true, 2*time.Millisecond)
	tracker.PoolRefreshResult(false, 3*time.Millisecond)
	snapshot := tracker.Snapshot()
	if snapshot.GetLogsFromBlock != 11 || snapshot.GetLogsToBlock != 12 || snapshot.GetLogsBlocksToScan != 2 {
		t.Fatalf("getLogs range not captured: %+v", snapshot)
	}
	if snapshot.GetLogsChunksAttempted != 1 || snapshot.GetLogsChunksFailed != 1 {
		t.Fatalf("getLogs progress not captured: %+v", snapshot)
	}
	if snapshot.PoolRefreshRequested != 2 || snapshot.PoolRefreshAttempted != 2 || snapshot.PoolRefreshSucceeded != 1 || snapshot.PoolRefreshFailed != 1 {
		t.Fatalf("pool refresh progress not captured: %+v", snapshot)
	}
	if snapshot.MaxSinglePoolRefresh < 3*time.Millisecond {
		t.Fatalf("max pool refresh duration not captured: %+v", snapshot)
	}
}

func TestDetectorStageTrackerCapturesPartialRouteQuoteProgress(t *testing.T) {
	tracker := newDetectorStageTracker(20, 19, nil)
	tracker.SetAffectedRoutes(7)
	tracker.AddRoutesSelected(3)
	tracker.RouteEvaluationStarted()
	tracker.RouteEvaluationFinished(false)
	tracker.RouteEvaluationStarted()
	tracker.RouteEvaluationFinished(true)
	snapshot := tracker.Snapshot()
	if snapshot.AffectedRoutesFound != 7 || snapshot.RoutesSelected != 3 {
		t.Fatalf("route selection progress not captured: %+v", snapshot)
	}
	if snapshot.RoutesEvaluationStarted != 2 || snapshot.RoutesEvaluated != 2 || snapshot.QuoteAttempts != 2 || snapshot.QuoteSuccesses != 1 || snapshot.QuoteFailures != 1 {
		t.Fatalf("quote progress not captured: %+v", snapshot)
	}
}

func TestCanceledCycleReportsActiveStage(t *testing.T) {
	tracker := newDetectorStageTracker(100, 90, nil)
	tracker.Stage(stagePoolRefresh)
	tracker.SetLastStateBlock(90)
	snapshot := tracker.Snapshot()
	if snapshot.CurrentStage != stagePoolRefresh {
		t.Fatalf("stage=%s", snapshot.CurrentStage)
	}
	if snapshot.ActiveStageElapsed < 0 || snapshot.CycleElapsed < 0 {
		t.Fatalf("invalid elapsed values: %+v", snapshot)
	}
	if snapshot.LastStateBlockAtCancel != 90 {
		t.Fatalf("last state block at cancel=%d", snapshot.LastStateBlockAtCancel)
	}
}

func TestIncrementalRefreshCancellationExposesPartialLogProgress(t *testing.T) {
	pool := pools.Pool{
		Address:          "0x0000000000000000000000000000000000000001",
		DEX:              pools.UniswapV3,
		Token0:           "0x0000000000000000000000000000000000000002",
		Token1:           "0x0000000000000000000000000000000000000003",
		Liquidity:        big.NewInt(1),
		SqrtPriceX96:     big.NewInt(1),
		LastUpdatedBlock: 1,
	}
	c := cache.NewPoolCache()
	c.Put(pool)
	ctx, cancel := context.WithCancel(context.Background())
	caller := &cancelOnLogsCaller{cancel: cancel}
	discoverer := pools.NewDiscoverer(caller, "", "", nil)
	engine := &Engine{discoverer: discoverer, cache: c, discoveryWorkers: 1}
	tracker := newDetectorStageTracker(12, 1, nil)
	_, stats, err := engine.incrementalRefresh(ctx, 1, 12, tracker)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	snapshot := tracker.Snapshot()
	if stats.Logs.ChunksAttempted == 0 || snapshot.GetLogsChunksAttempted == 0 {
		t.Fatalf("partial getLogs progress missing: stats=%+v snapshot=%+v", stats, snapshot)
	}
}

type cancelOnLogsCaller struct {
	cancel context.CancelFunc
}

func (c *cancelOnLogsCaller) EthCall(ctx context.Context, call map[string]string) (string, error) {
	return "0x", nil
}

func (c *cancelOnLogsCaller) BlockNumber(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (c *cancelOnLogsCaller) Call(ctx context.Context, method string, params any, out any) error {
	if method == "eth_getLogs" {
		if c.cancel != nil {
			c.cancel()
		}
		return context.Canceled
	}
	return nil
}
