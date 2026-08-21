// Package operations asynchronously persists and forwards operational events.
// It has no dependency on the execution pipeline and is deliberately fail-open.
package operations

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/titanarb/titanarb-go/internal/observability"
	"github.com/titanarb/titanarb-go/internal/telegram"
)

type Event struct {
	Category, Name, Message string
	Severity                telegram.Severity
	Fields                  map[string]any
	Notify                  bool
}
type Sink struct {
	writer   *observability.Writer
	telegram *telegram.Client
	queue    chan Event
	done     chan struct{}
	dropped  atomic.Uint64
	closeMu  sync.RWMutex
	closed   bool
}

func New(dir string, notifier *telegram.Client) (*Sink, error) {
	writer, err := observability.New(dir)
	if err != nil {
		return nil, err
	}
	s := &Sink{writer: writer, telegram: notifier, queue: make(chan Event, 256), done: make(chan struct{})}
	go s.run()
	return s, nil
}
func (s *Sink) Publish(event Event) {
	if s == nil {
		return
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return
	}
	if event.Category == "" {
		event.Category = observability.Performance
	}
	select {
	case s.queue <- event:
	default:
		s.dropped.Add(1)
	}
}
func (s *Sink) Dropped() uint64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}
func (s *Sink) run() {
	for event := range s.queue {
		_ = s.writer.Write(event.Category, event.Name, event.Fields)
		if event.Notify && s.telegram != nil {
			s.telegram.Notify(telegram.Message{Severity: event.Severity, Event: event.Name, Text: event.Message, Fields: stringFields(event.Fields)})
		}
	}
	close(s.done)
}
func (s *Sink) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.closeMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.closeMu.Unlock()
	select {
	case <-s.done:
		if s.telegram != nil {
			return s.telegram.Close(ctx)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func stringFields(input map[string]any) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for k, v := range input {
		out[k] = strings.TrimSpace(toString(v))
	}
	return out
}
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
