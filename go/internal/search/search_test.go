package search

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestRunIsBoundedAndCompletes(t *testing.T) {
	var active, max atomic.Int64
	result := Run(context.Background(), []int{1, 2, 3, 4, 5}, 2, func(_ context.Context, _ int) error {
		now := active.Add(1)
		for {
			old := max.Load()
			if now <= old || max.CompareAndSwap(old, now) {
				break
			}
		}
		active.Add(-1)
		return nil
	})
	if len(result) != 5 || max.Load() > 2 {
		t.Fatalf("unexpected bounded results=%d max=%d", len(result), max.Load())
	}
}

func TestMapReturnsEachComputedValueOnce(t *testing.T) {
	var calls atomic.Int64
	got := Map(context.Background(), []int{1, 2, 3}, 2, func(_ context.Context, value int) (int, error) {
		calls.Add(1)
		return value * 2, nil
	})
	if len(got) != 3 || calls.Load() != 3 {
		t.Fatalf("results=%v calls=%d", got, calls.Load())
	}
}
