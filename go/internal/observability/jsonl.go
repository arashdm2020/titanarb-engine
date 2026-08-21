// Package observability persists credential-safe operational history as JSONL.
package observability

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	Opportunities = "opportunities"
	Trades        = "trades"
	Errors        = "errors"
	Performance   = "performance"
	Server        = "server"
)

var allowed = map[string]bool{Opportunities: true, Trades: true, Errors: true, Performance: true, Server: true}

type Record struct {
	Timestamp time.Time      `json:"timestamp"`
	Event     string         `json:"event"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type Writer struct {
	dir string
	mu  sync.Mutex
}

func New(dir string) (*Writer, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("observability directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Writer{dir: dir}, nil
}

func (w *Writer) Write(category, event string, fields map[string]any) error {
	if !allowed[category] {
		return fmt.Errorf("unsupported observability category %q", category)
	}
	record := Record{Timestamp: time.Now().UTC(), Event: event, Fields: sanitizeFields(fields)}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	file, err := os.OpenFile(filepath.Join(w.dir, category+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(append(encoded, '\n'))
	return err
}

func sanitizeFields(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		if sensitive(key) {
			out[key] = "[redacted]"
			continue
		}
		switch nested := value.(type) {
		case map[string]any:
			out[key] = sanitizeFields(nested)
		case string:
			if strings.Contains(nested, "://") {
				out[key] = "[redacted]"
			} else {
				out[key] = nested
			}
		default:
			out[key] = value
		}
	}
	return out
}
func sensitive(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	return k == "private_key" || k == "api_key" || k == "rpc_url" || k == "ws_rpc_url" || k == "wss_url" || k == "telegram_bot_token" || k == "secret"
}
