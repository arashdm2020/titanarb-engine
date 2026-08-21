package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestJSONIncludesRequiredFieldsAndRedacts(t *testing.T) {
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
