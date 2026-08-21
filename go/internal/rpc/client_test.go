package rpc

import (
	"context"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMockSuccess(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xa4b1"}`))
	}))
	defer s.Close()
	id, err := New(s.URL, time.Second, 0, nil).ChainID(context.Background())
	if err != nil || id != 42161 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}
func TestRetryBehavior(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x123"}`))
	}))
	defer s.Close()
	m := metrics.New()
	n, err := New(s.URL, time.Second, 1, m).BlockNumber(context.Background())
	if err != nil || n != 0x123 || calls.Load() != 2 || m.Snapshot().RPCErrors != 1 {
		t.Fatalf("n=%d calls=%d metrics=%+v err=%v", n, calls.Load(), m.Snapshot(), err)
	}
}
func TestMockFailure(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "bad", http.StatusBadGateway) }))
	defer s.Close()
	if _, err := New(s.URL, time.Second, 0, nil).BlockNumber(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
}
