package observability

import (
	"os"
	"strings"
	"testing"
)

func TestWriterPersistsAndRedactsSecrets(t *testing.T) {
	dir := t.TempDir()
	writer, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(Trades, "trade_prepared", map[string]any{"tx_hash": "0xabc", "private_key": "do-not-store", "rpc_url": "https://credential@example"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dir + "/trades.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "do-not-store") || strings.Contains(text, "credential") || !strings.Contains(text, "0xabc") {
		t.Fatalf("unsafe or incomplete record: %s", text)
	}
}
