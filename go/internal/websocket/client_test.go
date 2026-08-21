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
