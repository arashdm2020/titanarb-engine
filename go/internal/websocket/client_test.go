package websocket

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/titanarb/titanarb-go/internal/logger"
	"github.com/titanarb/titanarb-go/internal/metrics"
)

func TestConnectDisconnectReconnect(t *testing.T) {
	var connections atomic.Int32
	up := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe map[string]any
		if conn.ReadJSON(&subscribe) != nil {
			return
		}
		n := connections.Add(1)
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "result": "sub"})
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "eth_subscription", "params": map[string]any{"result": map[string]string{"number": "0x1", "hash": "0xabc", "timestamp": "0x1"}}})
		if n == 1 {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}))
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	m := metrics.New()
	client := New(url, logger.New(false, io.Discard), m)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events := client.Start(ctx)
	received := 0
	for received < 2 {
		select {
		case <-events:
			received++
		case <-ctx.Done():
			t.Fatalf("events=%d metrics=%+v", received, m.Snapshot())
		}
	}
	if m.Snapshot().WSSDisconnects < 1 || m.Snapshot().WSSReconnects < 1 {
		t.Fatalf("missing reconnect metrics: %+v", m.Snapshot())
	}
}

func TestWSSFailoverUsesSecondaryWithoutDuplicateSubscriptions(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer primary.Close()
	var subscriptions atomic.Int32
	up := websocket.Upgrader{}
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe map[string]any
		if conn.ReadJSON(&subscribe) != nil {
			return
		}
		subscriptions.Add(1)
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "result": "sub"})
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "eth_subscription", "params": map[string]any{"result": map[string]string{"number": "0x2", "hash": "0xdef", "timestamp": "0x2"}}})
		time.Sleep(150 * time.Millisecond)
	}))
	defer secondary.Close()
	primaryURL := "ws" + strings.TrimPrefix(primary.URL, "http")
	secondaryURL := "ws" + strings.TrimPrefix(secondary.URL, "http")
	m := metrics.New()
	client := NewManaged([]Endpoint{{Name: "quicknode", URL: primaryURL}, {Name: "chainstack", URL: secondaryURL}}, logger.New(false, io.Discard), m)
	var failovers atomic.Int32
	client.SetObserver(func(event string, _ map[string]any) {
		if event == "wss_failover" {
			failovers.Add(1)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events := client.Start(ctx)
	select {
	case event := <-events:
		if event.Number != "0x2" {
			t.Fatalf("unexpected event: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("secondary WSS did not deliver newHeads")
	}
	if client.ActiveProvider() != "chainstack" || failovers.Load() == 0 {
		t.Fatalf("WSS failover mismatch provider=%s failovers=%d", client.ActiveProvider(), failovers.Load())
	}
	if subscriptions.Load() != 1 {
		t.Fatalf("duplicate WSS subscriptions: %d", subscriptions.Load())
	}
}
