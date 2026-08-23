package rpc

import (
	"context"
	"errors"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestManagedClientSelectsPrimaryHTTP(t *testing.T) {
	s := rpcServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x123"}`)
	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: s.URL}, {Name: "chainstack", HTTP: s.URL}}, time.Second, 0, nil)
	if got := client.ActiveProvider(); got != "quicknode" {
		t.Fatalf("active provider=%s", got)
	}
	if block, err := client.BlockNumber(context.Background()); err != nil || block != 0x123 {
		t.Fatalf("block=%d err=%v", block, err)
	}
}

func TestManagedClientFailoverOn429(t *testing.T) {
	primary := rpcServer(t, http.StatusTooManyRequests, `rate limited`)
	secondary := rpcServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x456"}`)
	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: primary.URL}, {Name: "chainstack", HTTP: secondary.URL}}, time.Second, 1, nil)
	var event Event
	client.SetObserver(func(e Event) { event = e })
	block, err := client.BlockNumber(context.Background())
	if err != nil || block != 0x456 {
		t.Fatalf("block=%d err=%v", block, err)
	}
	if client.ActiveProvider() != "chainstack" || event.Name != "rpc_failover" || event.Reason != "rate_limit" {
		t.Fatalf("failover mismatch provider=%s event=%+v", client.ActiveProvider(), event)
	}
}

func TestManagedClientFailoverOn5xx(t *testing.T) {
	primary := rpcServer(t, http.StatusBadGateway, `bad gateway`)
	secondary := rpcServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x789"}`)
	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: primary.URL}, {Name: "chainstack", HTTP: secondary.URL}}, time.Second, 1, nil)
	block, err := client.BlockNumber(context.Background())
	if err != nil || block != 0x789 || client.ActiveProvider() != "chainstack" {
		t.Fatalf("failover failed block=%d provider=%s err=%v", block, client.ActiveProvider(), err)
	}
}

func TestManagedClientRPCRevertDoesNotMarkProviderUnhealthy(t *testing.T) {
	primary := rpcServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"message":"execution reverted"}}`)
	secondary := rpcServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: primary.URL}, {Name: "chainstack", HTTP: secondary.URL}}, time.Second, 1, nil)
	var raw string
	err := client.Call(context.Background(), "eth_call", []any{}, &raw)
	var rpcErr *Error
	if !errors.As(err, &rpcErr) || rpcErr.Kind != RPC {
		t.Fatalf("expected RPC error, got %T %v", err, err)
	}
	if client.ActiveProvider() != "quicknode" {
		t.Fatalf("valid RPC error caused failover to %s", client.ActiveProvider())
	}
}

func TestManagedClientCallerContextTimeoutDoesNotFailover(t *testing.T) {
	var secondaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer primary.Close()
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondaryCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x2"}`))
	}))
	defer secondary.Close()

	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: primary.URL}, {Name: "chainstack", HTTP: secondary.URL}}, time.Second, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.BlockNumber(ctx); err == nil {
		t.Fatal("expected caller context timeout")
	}
	if got := client.ActiveProvider(); got != "quicknode" {
		t.Fatalf("caller context timeout caused failover to %s", got)
	}
	if secondaryCalls.Load() != 0 {
		t.Fatalf("caller context timeout retried on secondary %d times", secondaryCalls.Load())
	}
}

func TestManagedClientRateLimiter(t *testing.T) {
	s := rpcServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: s.URL, MaxRPS: 2}}, time.Second, 0, nil)
	started := time.Now()
	for i := 0; i < 3; i++ {
		if _, err := client.BlockNumber(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond {
		t.Fatalf("rate limiter did not enforce provider RPS: %s", elapsed)
	}
}

func TestManagedClientCooldownAvoidsFlapping(t *testing.T) {
	primary := rpcServer(t, http.StatusTooManyRequests, `rate limited`)
	secondary := rpcServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x2"}`)
	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: primary.URL}, {Name: "chainstack", HTTP: secondary.URL}}, time.Second, 1, nil)
	if _, err := client.BlockNumber(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.BlockNumber(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.ActiveProvider() != "chainstack" {
		t.Fatalf("provider flapped back to %s during cooldown", client.ActiveProvider())
	}
}

func TestManagedClientPrimaryRecoveryHysteresis(t *testing.T) {
	var primaryStatus atomic.Int32
	primaryStatus.Store(http.StatusTooManyRequests)
	var primaryCalls atomic.Int32
	var secondaryCalls atomic.Int32

	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls.Add(1)
		if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
			t.Fatalf("missing JSON content type")
		}
		if primaryStatus.Load() != http.StatusOK {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x3"}`))
	}))
	defer primary.Close()

	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondaryCalls.Add(1)
		if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
			t.Fatalf("missing JSON content type")
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x2"}`))
	}))
	defer secondary.Close()

	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: primary.URL}, {Name: "chainstack", HTTP: secondary.URL}}, time.Second, 1, nil)
	if block, err := client.BlockNumber(context.Background()); err != nil || block != 0x2 {
		t.Fatalf("initial failover block=%d err=%v", block, err)
	}
	if got := client.ActiveProvider(); got != "chainstack" {
		t.Fatalf("expected fallback provider, got %s", got)
	}

	primaryStatus.Store(http.StatusOK)
	if block, err := client.BlockNumber(context.Background()); err != nil || block != 0x2 {
		t.Fatalf("hysteresis fallback block=%d err=%v", block, err)
	}
	if got := client.ActiveProvider(); got != "chainstack" {
		t.Fatalf("primary recovered too early, active=%s", got)
	}

	client.mu.Lock()
	client.lastSwitch = time.Now().Add(-3 * time.Minute)
	client.providers[0].cooldownUntil = time.Now().Add(-time.Second)
	client.mu.Unlock()

	if block, err := client.BlockNumber(context.Background()); err != nil || block != 0x3 {
		t.Fatalf("primary recovery block=%d err=%v", block, err)
	}
	if got := client.ActiveProvider(); got != "quicknode" {
		t.Fatalf("primary did not recover after hysteresis window, active=%s", got)
	}
	if primaryCalls.Load() != 2 || secondaryCalls.Load() != 2 {
		t.Fatalf("unexpected request distribution primary=%d secondary=%d", primaryCalls.Load(), secondaryCalls.Load())
	}
}

func TestManagedClientSnapshotsTrackProviderBlocks(t *testing.T) {
	s := rpcServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0xabc"}`)
	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: s.URL}}, time.Second, 0, nil)
	_, _ = client.BlockNumber(context.Background())
	snapshots := client.Snapshots()
	if len(snapshots) != 1 || snapshots[0].LatestBlock != 0xabc || !snapshots[0].Active {
		t.Fatalf("snapshot mismatch: %+v", snapshots)
	}
}

func TestManagedClientDoesNotDuplicateTransactionBroadcast(t *testing.T) {
	var primaryCalls atomic.Int32
	var secondaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls.Add(1)
		http.Error(w, "temporary", http.StatusBadGateway)
	}))
	defer primary.Close()
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondaryCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xhash"}`))
	}))
	defer secondary.Close()
	client := NewManaged([]ProviderConfig{{Name: "quicknode", HTTP: primary.URL}, {Name: "chainstack", HTTP: secondary.URL}}, time.Second, 1, nil)
	if _, err := client.SendRawTransaction(context.Background(), "0xdead"); err == nil {
		t.Fatal("expected primary broadcast failure")
	}
	if primaryCalls.Load() != 1 || secondaryCalls.Load() != 0 {
		t.Fatalf("broadcast was duplicated primary=%d secondary=%d", primaryCalls.Load(), secondaryCalls.Load())
	}
}

func rpcServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
			t.Fatalf("missing JSON content type")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}
