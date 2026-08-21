// Package logger provides credential-safe structured operational logging.
package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Level string

const (
	Debug Level = "DEBUG"
	Info  Level = "INFO"
	Warn  Level = "WARN"
	Error Level = "ERROR"
)

type Logger struct {
	json bool
	out  io.Writer
	mu   sync.Mutex
}

func New(jsonOutput bool, out io.Writer) *Logger { return &Logger{json: jsonOutput, out: out} }

func SafeEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "[redacted]"
	}
	return u.Host
}

func (l *Logger) Event(level Level, event, component, message string, fields map[string]any) {
	record := map[string]any{"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "level": level, "event": event, "component": component, "message": message}
	for k, v := range fields {
		if !sensitive(k) {
			record[k] = redact(v)
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.json {
		data, _ := json.Marshal(record)
		_, _ = fmt.Fprintln(l.out, string(data))
		return
	}
	color := map[Level]string{Debug: "36", Info: "32", Warn: "33", Error: "31"}[level]
	_, _ = fmt.Fprintf(l.out, "\x1b[%sm%s %-5s %-18s %-10s %s\x1b[0m\n", color, record["timestamp"], level, event, component, message)
}

func sensitive(key string) bool {
	k := strings.ToLower(key)
	return strings.Contains(k, "key") || strings.Contains(k, "secret") || strings.Contains(k, "password") || strings.Contains(k, "url") || strings.Contains(k, "rpc")
}
func redact(v any) any {
	if s, ok := v.(string); ok && (strings.Contains(s, "://") || strings.HasPrefix(s, "0x") && len(s) == 66) {
		return "[redacted]"
	}
	return v
}
