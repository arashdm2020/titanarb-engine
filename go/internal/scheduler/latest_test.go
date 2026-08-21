package scheduler

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestLatestStateWinsWithoutBlockQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var mu sync.Mutex
	var processed []uint64
	s := New(func(_ context.Context, trigger Trigger) {
		mu.Lock()
		processed = append(processed, trigger.Block)
		first := len(processed) == 1
		mu.Unlock()
		if first {
			close(started)
			<-release
		} else {
			close(done)
		}
	})
	go s.Run(ctx)
	s.Submit(Trigger{Block: 100})
	<-started
	s.Submit(Trigger{Block: 101})
	s.Submit(Trigger{Block: 102})
	s.Submit(Trigger{Block: 103})
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("latest trigger was not processed")
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(processed, []uint64{100, 103}) {
		t.Fatalf("intermediate blocks were queued: %v", processed)
	}
	if s.BlocksCoalesced() != 2 || s.LatestBlock() != 103 {
		t.Fatalf("scheduler stats coalesced=%d latest=%d", s.BlocksCoalesced(), s.LatestBlock())
	}
}
