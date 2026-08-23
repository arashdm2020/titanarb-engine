// Package rpc implements the read-only Arbitrum JSON-RPC foundation client.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/titanarb/titanarb-go/internal/metrics"
)

type ErrorKind string

const (
	Network ErrorKind = "network"
	Timeout ErrorKind = "timeout"
	RPC     ErrorKind = "rpc"
	HTTP    ErrorKind = "http"
)

type Error struct {
	Kind ErrorKind
	Err  error
}

func (e *Error) Error() string { return e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

type Client struct {
	endpoint   string
	http       *http.Client
	retries    int
	metrics    *metrics.Metrics
	providers  []*providerState
	mu         sync.Mutex
	active     int
	lastSwitch time.Time
	observer   func(Event)
}

func New(endpoint string, timeout time.Duration, retries int, m *metrics.Metrics) *Client {
	return NewManaged([]ProviderConfig{{Name: "primary", HTTP: endpoint}}, timeout, retries, m)
}

type ProviderConfig struct {
	Name   string
	HTTP   string
	MaxRPS int
}

type ProviderSnapshot struct {
	Name              string
	Healthy           bool
	Active            bool
	Requests          uint64
	RateLimited       uint64
	Failures          uint64
	Latency           time.Duration
	LatestBlock       uint64
	CooldownUntil     time.Time
	ConsecutiveFailed uint64
}

type Event struct {
	Name      string
	From      string
	To        string
	Transport string
	Reason    string
}

type providerState struct {
	cfg                ProviderConfig
	limiter            *rateLimiter
	requests           uint64
	rateLimited        uint64
	failures           uint64
	consecutiveFailure uint64
	latencyEMA         time.Duration
	lastSuccessful     time.Time
	lastFailure        time.Time
	cooldownUntil      time.Time
	latestBlock        uint64
}

func NewManaged(configs []ProviderConfig, timeout time.Duration, retries int, m *metrics.Metrics) *Client {
	client := &Client{http: &http.Client{Timeout: timeout}, retries: retries, metrics: m}
	for _, cfg := range configs {
		cfg.Name = strings.TrimSpace(cfg.Name)
		cfg.HTTP = strings.TrimSpace(cfg.HTTP)
		if cfg.HTTP == "" {
			continue
		}
		if cfg.Name == "" {
			cfg.Name = "provider"
		}
		client.providers = append(client.providers, &providerState{cfg: cfg, limiter: newRateLimiter(cfg.MaxRPS)})
	}
	if len(client.providers) > 0 {
		client.endpoint = client.providers[0].cfg.HTTP
	}
	return client
}

func (c *Client) SetObserver(observer func(Event)) {
	c.mu.Lock()
	c.observer = observer
	c.mu.Unlock()
}

type Block struct {
	Number    string `json:"number"`
	Hash      string `json:"hash"`
	Timestamp string `json:"timestamp"`
}

// CallMessage is the JSON-RPC transaction shape used by eth_call and
// eth_estimateGas. Every field is hexadecimal when present.
type CallMessage struct {
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
	Data  string `json:"data,omitempty"`
	Value string `json:"value,omitempty"`
	Gas   string `json:"gas,omitempty"`
}

// Receipt deliberately keeps raw quantities as hex strings. Conversion is
// performed by the transaction layer so RPC formatting never leaks upward.
type Receipt struct {
	TransactionHash   string `json:"transactionHash"`
	BlockNumber       string `json:"blockNumber"`
	Status            string `json:"status"`
	GasUsed           string `json:"gasUsed"`
	EffectiveGasPrice string `json:"effectiveGasPrice"`
	Logs              []Log  `json:"logs"`
}

type Log struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if method == "eth_sendRawTransaction" {
		return c.callOnceNoFailover(ctx, method, body, out)
	}
	var last error
	for attempt := 0; attempt <= c.retries; attempt++ {
		provider, providerIndex := c.chooseProvider()
		if provider == nil {
			return &Error{Network, errors.New("no RPC provider configured")}
		}
		if err := provider.limiter.Wait(ctx); err != nil {
			return err
		}
		c.recordRequest(provider)
		err := c.callProvider(ctx, provider, body, out)
		if err == nil {
			c.recordSuccess(provider, method, out)
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if !providerFailure(err) {
			return err
		}
		last = err
		c.recordFailure(provider, err)
		c.failover(providerIndex, failureReason(err))
		if c.metrics != nil {
			c.metrics.IncRPCErrors()
		}
		if attempt < c.retries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}
		}
	}
	return last
}

func (c *Client) callOnceNoFailover(ctx context.Context, method string, body []byte, out any) error {
	provider, _ := c.chooseProvider()
	if provider == nil {
		return &Error{Network, errors.New("no RPC provider configured")}
	}
	if err := provider.limiter.Wait(ctx); err != nil {
		return err
	}
	c.recordRequest(provider)
	err := c.callProvider(ctx, provider, body, out)
	if err == nil {
		c.recordSuccess(provider, method, out)
		return nil
	}
	if ctx.Err() != nil {
		return err
	}
	if providerFailure(err) {
		c.recordFailure(provider, err)
		if c.metrics != nil {
			c.metrics.IncRPCErrors()
		}
	}
	return err
}

func (c *Client) callProvider(ctx context.Context, provider *providerState, body []byte, out any) error {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.cfg.HTTP, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return classify(err)
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return classify(readErr)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		provider.rateLimited++
		return &Error{HTTP, fmt.Errorf("rpc HTTP status %d", resp.StatusCode)}
	}
	if resp.StatusCode >= 500 {
		return &Error{HTTP, fmt.Errorf("rpc HTTP status %d", resp.StatusCode)}
	}
	if resp.StatusCode/100 != 2 {
		return &Error{HTTP, fmt.Errorf("rpc HTTP status %d", resp.StatusCode)}
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err = json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if envelope.Error != nil {
		return &Error{RPC, errors.New(envelope.Error.Message)}
	}
	if err = json.Unmarshal(envelope.Result, out); err != nil {
		return err
	}
	c.updateLatency(provider, time.Since(started))
	return nil
}

func (c *Client) chooseProvider() (*providerState, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.providers) == 0 {
		return nil, -1
	}
	now := time.Now()
	if c.active != 0 && len(c.providers) > 1 && now.Sub(c.lastSwitch) > 2*time.Minute && now.After(c.providers[0].cooldownUntil) {
		c.active = 0
	}
	for offset := 0; offset < len(c.providers); offset++ {
		index := (c.active + offset) % len(c.providers)
		if now.Before(c.providers[index].cooldownUntil) {
			continue
		}
		c.active = index
		return c.providers[index], index
	}
	return c.providers[c.active], c.active
}

func (c *Client) recordRequest(provider *providerState) {
	if c.metrics != nil {
		c.metrics.IncRPCCalls()
	}
	c.mu.Lock()
	provider.requests++
	c.mu.Unlock()
}

func (c *Client) recordSuccess(provider *providerState, method string, out any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	provider.consecutiveFailure = 0
	provider.lastSuccessful = time.Now()
	if method == "eth_blockNumber" {
		if raw, ok := out.(*string); ok {
			if block, err := strconv.ParseUint(strings.TrimPrefix(*raw, "0x"), 16, 64); err == nil {
				provider.latestBlock = block
			}
		}
	}
}

func (c *Client) recordFailure(provider *providerState, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	provider.failures++
	provider.consecutiveFailure++
	provider.lastFailure = time.Now()
	cooldown := 15 * time.Second
	if failureReason(err) == "rate_limit" {
		cooldown = 30 * time.Second
	}
	provider.cooldownUntil = time.Now().Add(cooldown)
	if provider.consecutiveFailure >= 3 {
		provider.cooldownUntil = time.Now().Add(60 * time.Second)
	}
}

func (c *Client) failover(from int, reason string) {
	c.mu.Lock()
	if len(c.providers) < 2 {
		c.mu.Unlock()
		return
	}
	old := c.providers[from]
	to := from
	now := time.Now()
	for offset := 1; offset < len(c.providers); offset++ {
		candidate := (from + offset) % len(c.providers)
		if now.Before(c.providers[candidate].cooldownUntil) {
			continue
		}
		to = candidate
		break
	}
	if to == from {
		c.mu.Unlock()
		return
	}
	c.active = to
	c.lastSwitch = time.Now()
	event := Event{Name: "rpc_failover", From: old.cfg.Name, To: c.providers[to].cfg.Name, Transport: "http", Reason: reason}
	observer := c.observer
	c.mu.Unlock()
	if observer != nil {
		observer(event)
	}
}

func (c *Client) updateLatency(provider *providerState, latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if provider.latencyEMA == 0 {
		provider.latencyEMA = latency
		return
	}
	provider.latencyEMA = time.Duration(float64(provider.latencyEMA)*0.8 + float64(latency)*0.2)
}

func providerFailure(err error) bool {
	var rpcErr *Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	if rpcErr.Kind == RPC {
		return false
	}
	if rpcErr.Kind == HTTP && !strings.Contains(rpcErr.Error(), " 429") && !strings.Contains(rpcErr.Error(), " 5") {
		return false
	}
	return true
}

func failureReason(err error) string {
	var rpcErr *Error
	if errors.As(err, &rpcErr) {
		if rpcErr.Kind == Timeout {
			return "timeout"
		}
		if rpcErr.Kind == Network {
			return "transport"
		}
		if rpcErr.Kind == HTTP {
			if strings.Contains(rpcErr.Error(), " 429") {
				return "rate_limit"
			}
			return "provider"
		}
	}
	return "unknown"
}

func (c *Client) ActiveProvider() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.providers) == 0 {
		return ""
	}
	return c.providers[c.active].cfg.Name
}

func (c *Client) Snapshots() []ProviderSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ProviderSnapshot, 0, len(c.providers))
	for i, provider := range c.providers {
		out = append(out, ProviderSnapshot{
			Name:              provider.cfg.Name,
			Healthy:           time.Now().After(provider.cooldownUntil),
			Active:            i == c.active,
			Requests:          provider.requests,
			RateLimited:       provider.rateLimited,
			Failures:          provider.failures,
			Latency:           provider.latencyEMA,
			LatestBlock:       provider.latestBlock,
			CooldownUntil:     provider.cooldownUntil,
			ConsecutiveFailed: provider.consecutiveFailure,
		})
	}
	return out
}

type rateLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func newRateLimiter(maxRPS int) *rateLimiter {
	if maxRPS <= 0 {
		return &rateLimiter{}
	}
	return &rateLimiter{interval: time.Second / time.Duration(maxRPS)}
}

func (l *rateLimiter) Wait(ctx context.Context) error {
	if l == nil || l.interval <= 0 {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	wait := time.Duration(0)
	if now.Before(l.next) {
		wait = l.next.Sub(now)
		l.next = l.next.Add(l.interval)
	} else {
		l.next = now.Add(l.interval)
	}
	l.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}
func (c *Client) ChainID(ctx context.Context) (int64, error) {
	var raw string
	err := c.Call(ctx, "eth_chainId", []any{}, &raw)
	if err != nil {
		return 0, err
	}
	return hexInt(raw)
}
func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	var raw string
	err := c.Call(ctx, "eth_blockNumber", []any{}, &raw)
	if err != nil {
		return 0, err
	}
	n, e := strconv.ParseUint(strings.TrimPrefix(raw, "0x"), 16, 64)
	return n, e
}
func (c *Client) GetBlockByNumber(ctx context.Context, number string) (Block, error) {
	var b Block
	err := c.Call(ctx, "eth_getBlockByNumber", []any{number, false}, &b)
	return b, err
}
func (c *Client) EthCall(ctx context.Context, call map[string]string) (string, error) {
	return c.EthCallAt(ctx, call, "latest")
}

func (c *Client) EthCallAt(ctx context.Context, call map[string]string, block string) (string, error) {
	if block == "" {
		block = "latest"
	}
	var raw string
	err := c.Call(ctx, "eth_call", []any{call, block}, &raw)
	return raw, err
}

func (c *Client) EstimateGas(ctx context.Context, call CallMessage) (uint64, error) {
	var raw string
	if err := c.Call(ctx, "eth_estimateGas", []any{call}, &raw); err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimPrefix(raw, "0x"), 16, 64)
	return v, err
}

func (c *Client) GasPrice(ctx context.Context) (*big.Int, error) {
	var raw string
	if err := c.Call(ctx, "eth_gasPrice", []any{}, &raw); err != nil {
		return nil, err
	}
	return hexBig(raw)
}

func (c *Client) MaxPriorityFeePerGas(ctx context.Context) (*big.Int, error) {
	var raw string
	if err := c.Call(ctx, "eth_maxPriorityFeePerGas", []any{}, &raw); err != nil {
		return nil, err
	}
	return hexBig(raw)
}

func (c *Client) TransactionCount(ctx context.Context, address, block string) (uint64, error) {
	if block == "" {
		block = "pending"
	}
	var raw string
	if err := c.Call(ctx, "eth_getTransactionCount", []any{address, block}, &raw); err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimPrefix(raw, "0x"), 16, 64)
	return v, err
}

func (c *Client) SendRawTransaction(ctx context.Context, rawTx string) (string, error) {
	var hash string
	err := c.Call(ctx, "eth_sendRawTransaction", []any{rawTx}, &hash)
	return hash, err
}

func (c *Client) TransactionReceipt(ctx context.Context, hash string) (*Receipt, error) {
	var receipt *Receipt
	if err := c.Call(ctx, "eth_getTransactionReceipt", []any{hash}, &receipt); err != nil {
		return nil, err
	}
	return receipt, nil
}
func (c *Client) GetCode(ctx context.Context, address string) (string, error) {
	var raw string
	err := c.Call(ctx, "eth_getCode", []any{address, "latest"}, &raw)
	return raw, err
}
func (c *Client) Healthy(ctx context.Context) (uint64, error) { return c.BlockNumber(ctx) }
func hexInt(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimPrefix(raw, "0x"), 16, 64)
}

func hexBig(raw string) (*big.Int, error) {
	v := new(big.Int)
	if _, ok := v.SetString(strings.TrimPrefix(raw, "0x"), 16); !ok {
		return nil, fmt.Errorf("invalid hex quantity")
	}
	return v, nil
}
func classify(err error) *Error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Timeout, err}
	}
	return &Error{Network, err}
}
