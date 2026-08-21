// Package websocket exposes read-only new-head events with reconnect support.
package websocket

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/titanarb/titanarb-go/internal/logger"
	"github.com/titanarb/titanarb-go/internal/metrics"
)

type NewBlockEvent struct {
	Number    string
	Hash      string
	Timestamp string
}
type Client struct {
	url        string
	log        *logger.Logger
	metrics    *metrics.Metrics
	connected  atomic.Bool
	observer   func(string, map[string]any)
	observerMu sync.RWMutex
}

func New(url string, log *logger.Logger, m *metrics.Metrics) *Client {
	return &Client{url: url, log: log, metrics: m}
}
func (c *Client) Connected() bool { return c.connected.Load() }

// SetObserver installs an optional non-critical event observer. The websocket
// stream never depends on it; callers must make their observer fail-open.
func (c *Client) SetObserver(observer func(string, map[string]any)) {
	c.observerMu.Lock()
	c.observer = observer
	c.observerMu.Unlock()
}
func (c *Client) emit(event string, fields map[string]any) {
	c.observerMu.RLock()
	observer := c.observer
	c.observerMu.RUnlock()
	if observer != nil {
		observer(event, fields)
	}
}

// Start runs until ctx cancellation. It never emits trade signals and never sends transactions.
func (c *Client) Start(ctx context.Context) <-chan NewBlockEvent {
	events := make(chan NewBlockEvent, 16)
	go func() {
		defer close(events)
		delay := time.Second
		reconnecting := false
		for {
			if ctx.Err() != nil {
				return
			}
			if reconnecting {
				c.log.Event(logger.Info, "wss_reconnecting", "websocket", "reconnecting", nil)
				c.emit("wss_reconnecting", nil)
			}
			conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.url, nil)
			if err != nil {
				if !sleep(ctx, delay) {
					return
				}
				delay = next(delay)
				reconnecting = true
				continue
			}
			if err := subscribe(conn); err != nil {
				_ = conn.Close()
				if !sleep(ctx, delay) {
					return
				}
				delay = next(delay)
				reconnecting = true
				continue
			}
			if reconnecting {
				c.metrics.IncWSSReconnects()
				c.log.Event(logger.Info, "wss_connected", "websocket", "connection restored", nil)
				c.emit("wss_reconnected", nil)
			} else {
				c.log.Event(logger.Info, "wss_connected", "websocket", "connected", nil)
				c.emit("wss_connected", nil)
			}
			c.connected.Store(true)
			reconnecting = false
			delay = time.Second
			err = c.read(ctx, conn, events)
			c.connected.Store(false)
			_ = conn.Close()
			if ctx.Err() != nil {
				return
			}
			c.metrics.IncWSSDisconnects()
			c.log.Event(logger.Warn, "wss_disconnected", "websocket", "connection lost; polling fallback remains available", nil)
			c.emit("wss_disconnected", map[string]any{"http_fallback": "active"})
			reconnecting = true
		}
	}()
	return events
}
func subscribe(conn *websocket.Conn) error {
	return conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": []any{"newHeads"}})
}
func (c *Client) read(ctx context.Context, conn *websocket.Conn, out chan<- NewBlockEvent) error {
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	received := make(chan error, 1)
	go func() {
		for {
			var msg struct {
				Method string `json:"method"`
				Params struct {
					Result struct {
						Number    string `json:"number"`
						Hash      string `json:"hash"`
						Timestamp string `json:"timestamp"`
					} `json:"result"`
				} `json:"params"`
			}
			if err := conn.ReadJSON(&msg); err != nil {
				received <- err
				return
			}
			if msg.Method == "eth_subscription" {
				select {
				case out <- NewBlockEvent{msg.Params.Result.Number, msg.Params.Result.Hash, msg.Params.Result.Timestamp}:
					c.metrics.IncBlocks()
				case <-ctx.Done():
					received <- ctx.Err()
					return
				}
			}
		}
	}()
	for {
		select {
		case err := <-received:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return err
			}
		}
	}
}
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
func next(d time.Duration) time.Duration {
	if d >= 30*time.Second {
		return 30 * time.Second
	}
	return d * 2
}

var _ = json.RawMessage{}
