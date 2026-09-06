// Package websocket exposes read-only new-head events with reconnect support.
package websocket

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
	"strconv"

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
	endpoints  []Endpoint
	active     int
	log        *logger.Logger
	metrics    *metrics.Metrics
	connected  atomic.Bool
	observer   func(string, map[string]any)
	observerMu sync.RWMutex
	mu         sync.Mutex
	states     []endpointState
	lastBlock  uint64
}

type endpointState struct { failures int; cooldownUntil time.Time; attempts, successes, heads uint64 }

type Endpoint struct {
	Name string
	URL  string
}

func New(url string, log *logger.Logger, m *metrics.Metrics) *Client {
	return NewManaged([]Endpoint{{Name: "primary", URL: url}}, log, m)
}

func NewManaged(endpoints []Endpoint, log *logger.Logger, m *metrics.Metrics) *Client {
	client := &Client{log: log, metrics: m}
	for _, endpoint := range endpoints {
		if endpoint.URL == "" {
			continue
		}
		if endpoint.Name == "" {
			endpoint.Name = "provider"
		}
		client.endpoints = append(client.endpoints, endpoint)
		client.states = append(client.states, endpointState{})
	}
	if len(client.endpoints) > 0 {
		client.url = client.endpoints[0].URL
	}
	return client
}
func (c *Client) Connected() bool { return c.connected.Load() }
func (c *Client) ActiveProvider() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.endpoints) == 0 {
		return ""
	}
	return c.endpoints[c.active].Name
}

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
				c.log.Event(logger.Info, "wss_reconnecting", "websocket", "reconnecting", map[string]any{"provider": c.ActiveProvider()})
				c.emit("wss_reconnecting", map[string]any{"provider": c.ActiveProvider()})
			}
			endpoint := c.currentEndpoint()
			if endpoint.URL == "" {
				return
			}
			conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint.URL, nil)
			if err != nil {
				c.markFailed("connect")
				if !sleep(ctx, delay) {
					return
				}
				delay = next(delay)
				reconnecting = true
				continue
			}
			if err := subscribe(conn); err != nil {
				_ = conn.Close()
				c.markFailed("subscribe")
				if !sleep(ctx, delay) {
					return
				}
				delay = next(delay)
				reconnecting = true
				continue
			}
			if reconnecting {
				c.metrics.IncWSSReconnects()
				c.log.Event(logger.Info, "wss_connected", "websocket", "connection restored", map[string]any{"provider": c.ActiveProvider()})
				c.emit("wss_reconnected", map[string]any{"provider": c.ActiveProvider()})
			} else {
				c.log.Event(logger.Info, "wss_connected", "websocket", "connected", map[string]any{"provider": c.ActiveProvider()})
				c.emit("wss_connected", map[string]any{"provider": c.ActiveProvider()})
			}
			c.connected.Store(true)
			c.markSuccess()
			reconnecting = false
			delay = time.Second
			err = c.read(ctx, conn, events)
			c.connected.Store(false)
			_ = conn.Close()
			if ctx.Err() != nil {
				return
			}
			c.metrics.IncWSSDisconnects()
			c.log.Event(logger.Warn, "wss_disconnected", "websocket", "connection lost; polling fallback remains available", map[string]any{"provider": c.ActiveProvider()})
			c.emit("wss_disconnected", map[string]any{"http_fallback": "active", "provider": c.ActiveProvider()})
			c.markFailed("disconnect")
			reconnecting = true
		}
	}()
	return events
}

func (c *Client) currentEndpoint() Endpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.endpoints) == 0 {
		return Endpoint{}
	}
	now := time.Now()
	for i := range c.endpoints {
		idx := (c.active+i)%len(c.endpoints)
		if now.After(c.states[idx].cooldownUntil) { c.active=idx; c.url=c.endpoints[idx].URL; return c.endpoints[idx] }
	}
	return c.endpoints[c.active]
}

func (c *Client) markFailed(reason string) {
	c.mu.Lock()
	if len(c.endpoints)>0 { s:=&c.states[c.active]; s.failures++; d:=time.Second << min(s.failures-1,4); if d>30*time.Second {d=30*time.Second}; s.cooldownUntil=time.Now().Add(d) }
	if len(c.endpoints) < 2 {
		c.mu.Unlock()
		return
	}
	from := c.endpoints[c.active]
	fromIndex:=c.active; now:=time.Now(); nextIndex:=fromIndex
	for i:=1;i<=len(c.endpoints);i++ { idx:=(fromIndex+i)%len(c.endpoints); if now.After(c.states[idx].cooldownUntil) { nextIndex=idx; break } }
	if nextIndex==fromIndex { c.mu.Unlock(); return }
	c.active = nextIndex
	to := c.endpoints[c.active]
	c.url = to.URL
	c.mu.Unlock()
	c.log.Event(logger.Warn, "wss_failover", "websocket", "switching websocket provider", map[string]any{"from": from.Name, "to": to.Name, "reason": reason})
	c.emit("wss_failover", map[string]any{"from": from.Name, "to": to.Name, "reason": reason})
}
func (c *Client) markSuccess() { c.mu.Lock(); if len(c.states)>0 { s:=&c.states[c.active]; s.successes++; s.failures=0; s.cooldownUntil=time.Time{} }; c.mu.Unlock() }
func min(a,b int) int { if a<b{return a}; return b }
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
				n, err := strconv.ParseUint(msg.Params.Result.Number, 0, 64); if err != nil { continue }
				c.mu.Lock(); if n<=c.lastBlock { c.mu.Unlock(); continue }; c.lastBlock=n; if len(c.states)>0 { c.states[c.active].heads++ }; c.mu.Unlock()
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
