package profit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrackerWritesJSONL(t *testing.T) {
	p := filepath.Join(t.TempDir(), "runtime", "trades.jsonl")
	if err := New(p).Write(Record{Event: "trade_failed", Reason: "simulation"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "trade_failed") {
		t.Fatal("missing record")
	}
}
