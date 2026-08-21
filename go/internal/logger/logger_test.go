package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestJSONIncludesRequiredFieldsAndRedacts(t *testing.T) {
	t.Setenv("LOG_LEVEL", "INFO")
	var output bytes.Buffer
	New(true, &output).Event(Info, "rpc_connected", "rpc", "connected", map[string]any{"rpc_url": "https://secret.example"})
	text := output.String()
	for _, required := range []string{"timestamp", "level", "event", "component", "message"} {
		if !strings.Contains(text, `"`+required+`"`) {
			t.Fatalf("missing %s in %s", required, text)
		}
	}
	if strings.Contains(text, "secret.example") {
		t.Fatal("credential-bearing endpoint leaked")
	}
}

func TestInfoLevelSuppressesDebugAndUsesOneLineFields(t *testing.T) {
	t.Setenv("LOG_LEVEL", "INFO")
	var output bytes.Buffer
	log := New(false, &output)
	log.Event(Debug, "new_block_received", "websocket", "new block", map[string]any{"block": 100})
	log.Event(Info, "market_cycle", "market", "market cycle complete", map[string]any{"duration_ms": 184, "rpc_calls": 12})
	text := output.String()
	if strings.Contains(text, "new_block_received") {
		t.Fatalf("DEBUG event leaked at INFO level: %s", text)
	}
	if !strings.Contains(text, "duration_ms=184") || !strings.Contains(text, "rpc_calls=12") {
		t.Fatalf("compact fields missing: %s", text)
	}
	if strings.Count(strings.TrimSpace(text), "\n") != 0 {
		t.Fatalf("event was not one physical line: %q", text)
	}
}
