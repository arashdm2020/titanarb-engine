// Package telegram delivers asynchronous, credential-safe operational alerts.
// Delivery is intentionally fail-open: a Telegram outage can never delay quotes,
// safety checks, simulation, or (once authorized) execution.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
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

// Update contains only the untrusted fields needed for operator commands.
// Incoming credentials are never accepted through this transport.
type Update struct {
	ID       int64
	ChatID   string
	SenderID string
	Text     string
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
	message.Fields = cloneFields(message.Fields)
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

// SendTo queues an operator reply without blocking market or execution work.
func (c *Client) SendTo(chatID, text string) {
	if c == nil || !c.cfg.Enabled() || strings.TrimSpace(chatID) == "" {
		return
	}
	c.enqueue(chatID, Message{Severity: Info, Event: "operator_reply", Text: text})
}

func (c *Client) run() {
	for message := range c.queue {
		c.send(message)
	}
	close(c.done)
}
func (c *Client) send(message Message) {
	c.sendTo(c.cfg.ChatID, message)
}
func (c *Client) enqueue(chatID string, message Message) {
	message.Fields = cloneFields(message.Fields)
	message.Fields["_chat_id"] = chatID
	select {
	case c.queue <- message:
	default:
		c.dropped.Add(1)
	}
}
func (c *Client) sendTo(defaultChatID string, message Message) {
	chatID := defaultChatID
	if message.Fields != nil && message.Fields["_chat_id"] != "" {
		chatID = message.Fields["_chat_id"]
		delete(message.Fields, "_chat_id")
	}
	body, _ := json.Marshal(map[string]string{"chat_id": chatID, "text": format(message)})
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

// Updates long-polls Telegram without logging the credential-bearing URL.
// It is intended for exactly one operator-command goroutine.
func (c *Client) Updates(ctx context.Context, offset int64, timeoutSeconds int) ([]Update, error) {
	if c == nil || !c.cfg.Enabled() {
		return nil, nil
	}
	if timeoutSeconds < 1 || timeoutSeconds > 50 {
		timeoutSeconds = 25
	}
	body, _ := json.Marshal(map[string]any{"offset": offset, "timeout": timeoutSeconds, "allowed_updates": []string{"message"}})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.endpoint, "/")+"/bot"+c.cfg.Token+"/getUpdates", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("telegram getUpdates returned HTTP %d", response.StatusCode)
	}
	var raw struct {
		OK     bool `json:"ok"`
		Result []struct {
			UpdateID int64 `json:"update_id"`
			Message  *struct {
				Text string `json:"text"`
				Chat struct {
					ID json.Number `json:"id"`
				} `json:"chat"`
				From struct {
					ID json.Number `json:"id"`
				} `json:"from"`
			} `json:"message"`
		} `json:"result"`
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil || !raw.OK {
		return nil, fmt.Errorf("invalid Telegram getUpdates response")
	}
	updates := make([]Update, 0, len(raw.Result))
	for _, item := range raw.Result {
		if item.Message == nil {
			continue
		}
		updates = append(updates, Update{ID: item.UpdateID, ChatID: item.Message.Chat.ID.String(), SenderID: item.Message.From.ID.String(), Text: item.Message.Text})
	}
	return updates, nil
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
	// A dashboard is already deliberately formatted for a human operator. Keep
	// its detailed counters in JSONL, rather than appending an unordered raw
	// field dump to the Telegram message.
	if isPresentationMessage(m.Text) {
		return m.Text
	}
	prefix := map[Severity]string{Info: "INFO", Warning: "WARNING", Error: "ERROR", Critical: "CRITICAL"}[m.Severity]
	if prefix == "" {
		prefix = "INFO"
	}
	parts := []string{"[" + prefix + "] " + m.Event, m.Text}
	keys := make([]string, 0, len(m.Fields))
	for key := range m.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := m.Fields[key]
		if !sensitive(key) {
			parts = append(parts, key+"="+value)
		}
	}
	return strings.Join(parts, "\n")
}

func isPresentationMessage(text string) bool {
	trimmed := strings.TrimSpace(text)
	return strings.HasPrefix(trimmed, "📊 TITANARB —") ||
		strings.HasPrefix(trimmed, "🟢 TITANARB") ||
		strings.HasPrefix(trimmed, "🌊 MARKET MOVEMENT") ||
		strings.HasPrefix(trimmed, "🔥 OPPORTUNITY") ||
		strings.HasPrefix(trimmed, "🧪 SIMULATION") ||
		strings.HasPrefix(trimmed, "🚀 TX SENT") ||
		strings.HasPrefix(trimmed, "✅ TRADE") ||
		strings.HasPrefix(trimmed, "❌ TRADE")
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

func cloneFields(input map[string]string) map[string]string {
	if len(input) == 0 {
		return make(map[string]string)
	}
	output := make(map[string]string, len(input)+1)
	for key, value := range input {
		output[key] = value
	}
	return output
}
