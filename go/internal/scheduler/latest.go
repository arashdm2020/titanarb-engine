// Package scheduler coalesces high-frequency chain-head triggers. It retains
// at most one pending trigger and always replaces it with the newest state.
package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Trigger struct {
	Block      uint64
	Hash       string
	ObservedAt time.Time
}

type Handler func(context.Context, Trigger)

type Latest struct {
	mu        sync.Mutex
	pending   *Trigger
	wake      chan struct{}
	handler   Handler
	coalesced atomic.Uint64
	latest    atomic.Uint64
}

func New(handler Handler) *Latest {
	return &Latest{wake: make(chan struct{}, 1), handler: handler}
}

// Submit never blocks and never creates a per-block queue. If a trigger is
// already pending, it is superseded by this newer state.
func (s *Latest) Submit(trigger Trigger) {
	if trigger.ObservedAt.IsZero() {
		trigger.ObservedAt = time.Now().UTC()
	}
	for {
		current := s.latest.Load()
		if trigger.Block <= current {
			return
		}
		if s.latest.CompareAndSwap(current, trigger.Block) {
			break
		}
	}
	s.mu.Lock()
	if s.pending != nil {
		s.coalesced.Add(1)
	}
	copy := trigger
	s.pending = &copy
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Latest) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
			for {
				trigger, ok := s.take()
				if !ok {
					break
				}
				if s.handler != nil {
					s.handler(ctx, trigger)
				}
			}
		}
	}
}

func (s *Latest) LatestBlock() uint64     { return s.latest.Load() }
func (s *Latest) BlocksCoalesced() uint64 { return s.coalesced.Load() }

func (s *Latest) take() (Trigger, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		return Trigger{}, false
	}
	trigger := *s.pending
	s.pending = nil
	return trigger, true
}
