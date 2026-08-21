package operations

import (
	"context"
	"github.com/titanarb/titanarb-go/internal/observability"
	"github.com/titanarb/titanarb-go/internal/telegram"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestSinkPersistsWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir, telegram.New(telegram.Config{}))
	if err != nil {
		t.Fatal(err)
	}
	s.Publish(Event{Category: observability.Errors, Name: "rpc_failure", Message: "safe", Fields: map[string]any{"rpc_url": "https://credential"}})
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dir + "/errors.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "credential") {
		t.Fatalf("secret leaked: %s", data)
	}
}

func TestConcurrentPublishAndCloseIsSafe(t *testing.T) {
	s, err := New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Publish(Event{Category: observability.Performance, Name: "cycle"})
			}
		}()
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	// A late asynchronous producer must be ignored rather than panic.
	s.Publish(Event{Category: observability.Performance, Name: "late"})
}
