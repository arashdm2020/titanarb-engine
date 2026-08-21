package operations

import (
	"context"
	"github.com/titanarb/titanarb-go/internal/observability"
	"github.com/titanarb/titanarb-go/internal/telegram"
	"os"
	"strings"
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
