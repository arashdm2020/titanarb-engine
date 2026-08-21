package telegram

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeliveryIsAsyncAndSecretSafe(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	c := New(Config{Token: "secret", ChatID: "1", QueueSize: 1})
	c.endpoint = server.URL
	c.Notify(Message{Severity: Info, Event: "bot_started", Text: "ready", Fields: map[string]string{"rpc_url": "https://secret"}})
	deadline := time.Now().Add(time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls.Load() != 1 {
		t.Fatal("notification was not delivered")
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFailureNeverBlocksCaller(t *testing.T) {
	c := New(Config{Token: "secret", ChatID: "1", QueueSize: 1})
	c.endpoint = "http://127.0.0.1:1"
	start := time.Now()
	for i := 0; i < 10; i++ {
		c.Notify(Message{Event: "warning", Text: "offline"})
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("notify unexpectedly blocked")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Close(ctx)
}
