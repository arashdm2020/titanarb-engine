// Package telegram delivers asynchronous, credential-safe operational alerts.
// Delivery is intentionally fail-open: a Telegram outage can never delay quotes,
// safety checks, simulation, or (once authorized) execution.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type Severity string

const (
	Info     Severity = "info"
	Warning  Severity = "warning"
	Error    Severity = "error"
	Critical Severity = "critical"
)

type Message struct {
	Severity Severity
	Event    string
	Text     string
	Fields   map[string]string
}
type Config struct {
	Token, ChatID string
	QueueSize     int
}

func FromEnv() Config {
	return Config{Token: strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")), ChatID: strings.TrimSpace(os.Getenv("TELEGRAM_CHAT_ID")), QueueSize: 128}
}
func (c Config) Enabled() bool { return c.Token != "" && c.ChatID != "" }

type Client struct {
	cfg                   Config
	endpoint              string
	http                  *http.Client
	queue                 chan Message
	done                  chan struct{}
	sent, failed, dropped atomic.Uint64
}

func New(cfg Config) *Client {
	if cfg.QueueSize < 1 {
		cfg.QueueSize = 128
	}
	c := &Client{cfg: cfg, endpoint: "https://api.telegram.org", http: &http.Client{Timeout: 8 * time.Second}, queue: make(chan Message, cfg.QueueSize), done: make(chan struct{})}
	if cfg.Enabled() {
		go c.run()
	} else {
		close(c.done)
	}
	return c
}

// Notify is non-blocking and safe to call from any market or execution goroutine.
func (c *Client) Notify(message Message) {
	if c == nil || !c.cfg.Enabled() {
		return
	}
	message.Text = sanitize(message.Text)
	for key, value := range message.Fields {
		if sensitive(key) {
			message.Fields[key] = "[redacted]"
		} else {
			message.Fields[key] = sanitize(value)
		}
	}
	select {
	case c.queue <- message:
	default:
		c.dropped.Add(1)
	}
}

func (c *Client) run() {
	for message := range c.queue {
		c.send(message)
	}
	close(c.done)
}
func (c *Client) send(message Message) {
	body, _ := json.Marshal(map[string]string{"chat_id": c.cfg.ChatID, "text": format(message)})
	request, err := http.NewRequest(http.MethodPost, strings.TrimRight(c.endpoint, "/")+"/bot"+c.cfg.Token+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		c.failed.Add(1)
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		c.failed.Add(1)
		if response != nil {
			response.Body.Close()
		}
		return
	}
	response.Body.Close()
	c.sent.Add(1)
}
func (c *Client) Close(ctx context.Context) error {
	if c == nil || !c.cfg.Enabled() {
		return nil
	}
	close(c.queue)
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *Client) Snapshot() (sent, failed, dropped uint64) {
	return c.sent.Load(), c.failed.Load(), c.dropped.Load()
}

func format(m Message) string {
	prefix := map[Severity]string{Info: "INFO", Warning: "WARNING", Error: "ERROR", Critical: "CRITICAL"}[m.Severity]
	if prefix == "" {
		prefix = "INFO"
	}
	parts := []string{"[" + prefix + "] " + m.Event, m.Text}
	for key, value := range m.Fields {
		if !sensitive(key) {
			parts = append(parts, key+"="+value)
		}
	}
	return strings.Join(parts, "\n")
}
func sensitive(key string) bool {
	k := strings.ToLower(key)
	return strings.Contains(k, "secret") || strings.Contains(k, "private") || strings.Contains(k, "token") || strings.Contains(k, "api_key") || strings.Contains(k, "rpc") || strings.Contains(k, "url")
}
func sanitize(value string) string {
	if strings.Contains(value, "://") {
		return "[redacted]"
	}
	return value
}
