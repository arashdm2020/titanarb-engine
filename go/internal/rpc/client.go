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
	"sort"
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
	endpoint       string
	http           *http.Client
	retries        int
	metrics        *metrics.Metrics
	providers      []*providerState
	readLimiter    *rateLimiter
	premiumLimiter *rateLimiter
	ethBudget      *minuteBudget
	hotBudget      *minuteBudget
	mu             sync.Mutex
	active         int
	lastSwitch     time.Time
	observer       func(Event)
}

type RequestClass string

const (
	Standard   RequestClass = "standard"
	Background RequestClass = "background"
	HotPath    RequestClass = "hot_path"
	Critical   RequestClass = "critical"
)

type requestClassKey struct{}

func WithRequestClass(ctx context.Context, class RequestClass) context.Context {
	return context.WithValue(ctx, requestClassKey{}, class)
}
func requestClass(ctx context.Context) RequestClass {
	if v, ok := ctx.Value(requestClassKey{}).(RequestClass); ok {
		return v
	}
	return Standard
}

func New(endpoint string, timeout time.Duration, retries int, m *metrics.Metrics) *Client {
	return NewManaged([]ProviderConfig{{Name: "primary", HTTP: endpoint}}, timeout, retries, m)
}

func NewManagedWithReadBudget(configs []ProviderConfig, readTargetRPS int, timeout time.Duration, retries int, m *metrics.Metrics) *Client {
	return NewManagedWithBudgets(configs, readTargetRPS, 0, 0, 0, timeout, retries, m)
}

func NewManagedWithBudgets(configs []ProviderConfig, readTargetRPS, premiumRPS, maxEthCallsPerMinute, maxHotCallsPerMinute int, timeout time.Duration, retries int, m *metrics.Metrics) *Client {
	client := newManaged(configs, timeout, retries, m)
	client.readLimiter = newRateLimiter(readTargetRPS)
	client.premiumLimiter = newRateLimiterWithBurst(premiumRPS, 1)
	client.ethBudget = newMinuteBudget(maxEthCallsPerMinute)
	client.hotBudget = newMinuteBudget(maxHotCallsPerMinute)
	return client
}

type ProviderConfig struct {
	Name        string
	HTTP        string
	MaxRPS      int
	TargetRPS   int
	Burst       int
	MaxBlockLag int
	Tier        string
}

type ProviderSnapshot struct {
	Name               string
	Healthy            bool
	Active             bool
	Requests           uint64
	RateLimited        uint64
	Failures           uint64
	Latency            time.Duration
	LatestBlock        uint64
	CooldownUntil      time.Time
	ConsecutiveFailed  uint64
	MaxRPS             int
	TargetRPS          int
	SharePct           float64
	Burst              int
	InstantaneousRPS   float64
	CooldownRemaining  time.Duration
	Tier               string
	Inflight           int
	ProbationRemaining time.Duration
	EthCallRequests    uint64
	QuoteRequests      uint64
	Successes          uint64
	SuccessRatePct     float64
	Forbidden          uint64
	CooldownEvents     uint64
	LatencyP95         time.Duration
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
	latestBlockAt      time.Time
	requestTimes       []time.Time
	rateLimitStreak    uint64
	recent429Until     time.Time
	probationUntil     time.Time
	inflight           int
	ethCallRequests    uint64
	quoteRequests      uint64
	successes          uint64
	forbidden          uint64
	cooldownEvents     uint64
	latencySamples     []time.Duration
}

func NewManaged(configs []ProviderConfig, timeout time.Duration, retries int, m *metrics.Metrics) *Client {
	return newManaged(configs, timeout, retries, m)
}

func newManaged(configs []ProviderConfig, timeout time.Duration, retries int, m *metrics.Metrics) *Client {
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
		cfg.Tier = normalizeTier(cfg.Tier)
		client.providers = append(client.providers, &providerState{cfg: cfg, limiter: newRateLimiterWithBurst(effectiveRPS(cfg), effectiveBurst(cfg))})
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
	class := requestClass(ctx)
	for attempt := 0; attempt <= c.retries; attempt++ {
		if method == "eth_call" && class == HotPath && c.hotBudget != nil && !c.hotBudget.Allow() {
			return &Error{Network, errors.New("RPC hot-path minute budget exhausted")}
		}
		if method == "eth_call" && class != Critical && c.ethBudget != nil && !c.ethBudget.Allow() {
			return &Error{Network, errors.New("RPC eth_call minute budget exhausted")}
		}
		if class != Critical && c.readLimiter != nil {
			if err := c.readLimiter.Wait(ctx); err != nil {
				return err
			}
		}
		provider, providerIndex, providerDelay := c.chooseReadProvider(class)
		if provider == nil {
			return &Error{Network, errors.New("no eligible RPC provider configured")}
		}
		if provider.cfg.Tier == "premium" && class != Critical && c.premiumLimiter != nil {
			if err := c.premiumLimiter.Wait(ctx); err != nil {
				return err
			}
		}
		if err := waitDelay(ctx, providerDelay); err != nil {
			return err
		}
		c.recordRequest(provider, method, class)
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
	c.recordRequest(provider, method, Critical)
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
	c.mu.Lock()
	provider.inflight++
	c.mu.Unlock()
	defer func() { c.mu.Lock(); provider.inflight--; c.mu.Unlock() }()
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
		if quotaLimitMessage(string(data)) {
			return &Error{HTTP, errors.New("rpc HTTP status 429: quota exhausted")}
		}
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
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err = json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if envelope.Error != nil {
		if rpcRateLimited(envelope.Error.Code, envelope.Error.Message) {
			if quotaLimitMessage(envelope.Error.Message) {
				return &Error{HTTP, errors.New("rpc HTTP status 429: quota exhausted")}
			}
			return &Error{HTTP, errors.New("rpc HTTP status 429")}
		}
		return &Error{RPC, errors.New(envelope.Error.Message)}
	}
	if err = json.Unmarshal(envelope.Result, out); err != nil {
		return err
	}
	c.updateLatency(provider, time.Since(started))
	return nil
}

func rpcRateLimited(code int, message string) bool {
	m := strings.ToLower(message)
	if code == 429 || code == -32005 {
		return true
	}
	return strings.Contains(m, "rate limit") ||
		strings.Contains(m, "too many requests") ||
		strings.Contains(m, "daily request limit") ||
		strings.Contains(m, "usage limit")
}

func quotaLimitMessage(message string) bool {
	m := strings.ToLower(message)
	return strings.Contains(m, "daily request limit") ||
		strings.Contains(m, "usage limit") ||
		strings.Contains(m, "quota exhausted")
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

func (c *Client) chooseReadProvider(classes ...RequestClass) (*providerState, int, time.Duration) {
	class := Standard
	if len(classes) > 0 {
		class = classes[0]
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.providers) == 0 {
		return nil, -1, 0
	}
	now := time.Now()
	bestBlock := c.bestKnownBlockLocked()
	bestIndex := -1
	var bestScore time.Duration
	bestTier := 99
	flexibleHotPath := class == HotPath
	for pass := 0; pass < 2 && bestIndex == -1; pass++ {
		for index, provider := range c.providers {
			if now.Before(provider.cooldownUntil) {
				continue
			}
			rank := tierRank(class, provider.cfg.Tier)
			if rank >= 99 {
				continue
			}
			if pass == 0 && now.Before(provider.probationUntil) {
				continue
			}
			if providerLagged(provider, bestBlock, now) {
				continue
			}
			if !flexibleHotPath {
				if rank > bestTier {
					continue
				}
				if rank < bestTier {
					bestTier = rank
					bestIndex = -1
				}
			}
			score := provider.limiter.Delay()
			if flexibleHotPath {
				// Premium keeps a small deterministic preference, while a saturated
				// premium pool can spill bounded quote reads to healthy secondary
				// providers. Critical execution reads retain strict tier ordering.
				score += time.Duration(rank) * 75 * time.Millisecond
			}
			if now.Before(provider.recent429Until) {
				score += 250 * time.Millisecond
			}
			if provider.latencyEMA > 0 {
				score += provider.latencyEMA / 20
			}
			score += time.Duration(provider.inflight) * 25 * time.Millisecond
			score += time.Duration(provider.consecutiveFailure) * 100 * time.Millisecond
			if rps := effectiveRPS(provider.cfg); rps > 0 {
				// Weighted virtual finish time prevents a consistently faster or
				// earlier-listed endpoint from starving healthy peers while still
				// assigning proportionally more work to larger configured budgets.
				score += time.Duration(provider.requests) * time.Second / time.Duration(rps)
			}
			if bestIndex == -1 || score < bestScore || (score == bestScore && index < bestIndex) {
				bestIndex = index
				bestScore = score
			}
		}
	}
	if bestIndex == -1 {
		bestTier = 99
		for index, provider := range c.providers {
			rank := tierRank(class, provider.cfg.Tier)
			if rank >= 99 {
				continue
			}
			if !flexibleHotPath {
				if rank > bestTier {
					continue
				}
				if rank < bestTier {
					bestTier = rank
					bestIndex = -1
				}
			}
			delay := provider.limiter.Delay()
			if cooldown := time.Until(provider.cooldownUntil); cooldown > delay {
				delay = cooldown
			}
			score := delay
			if flexibleHotPath {
				score += time.Duration(rank) * 75 * time.Millisecond
			}
			if bestIndex == -1 || score < bestScore {
				bestIndex = index
				bestScore = score
			}
		}
	}
	if bestIndex == -1 {
		if len(c.providers) == 1 {
			bestIndex = 0
		} else {
			return nil, -1, 0
		}
	}
	c.active = bestIndex
	provider := c.providers[bestIndex]
	delay := provider.limiter.ReserveDelay()
	if cooldown := time.Until(provider.cooldownUntil); cooldown > delay {
		delay = cooldown
	}
	return provider, bestIndex, delay
}

func (c *Client) bestKnownBlockLocked() uint64 {
	var best uint64
	for _, provider := range c.providers {
		if provider.latestBlock > best {
			best = provider.latestBlock
		}
	}
	return best
}

func providerLagged(provider *providerState, best uint64, now time.Time) bool {
	if best == 0 || provider.latestBlock == 0 || provider.cfg.MaxBlockLag <= 0 {
		return false
	}
	if provider.latestBlockAt.IsZero() || now.Sub(provider.latestBlockAt) > 30*time.Second {
		return false
	}
	if provider.latestBlock >= best {
		return false
	}
	return best-provider.latestBlock > uint64(provider.cfg.MaxBlockLag)
}

func (c *Client) recordRequest(provider *providerState, method string, class RequestClass) {
	if c.metrics != nil {
		c.metrics.IncRPCCalls()
	}
	c.mu.Lock()
	provider.requests++
	if method == "eth_call" {
		provider.ethCallRequests++
		if class == HotPath {
			provider.quoteRequests++
		}
	}
	now := time.Now()
	provider.requestTimes = append(provider.requestTimes, now)
	provider.requestTimes = pruneRequestTimes(provider.requestTimes, now.Add(-time.Second))
	c.mu.Unlock()
}

func (c *Client) recordSuccess(provider *providerState, method string, out any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	provider.consecutiveFailure = 0
	provider.successes++
	provider.lastSuccessful = time.Now()
	provider.rateLimitStreak = 0
	if method == "eth_blockNumber" {
		if raw, ok := out.(*string); ok {
			if block, err := strconv.ParseUint(strings.TrimPrefix(*raw, "0x"), 16, 64); err == nil {
				provider.latestBlock = block
				provider.latestBlockAt = time.Now()
			}
		}
	}
}

func (c *Client) recordFailure(provider *providerState, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	provider.failures++
	if strings.Contains(err.Error(), " 403") {
		provider.forbidden++
	}
	provider.cooldownEvents++
	provider.consecutiveFailure++
	provider.lastFailure = time.Now()
	cooldown := 15 * time.Second
	if failureReason(err) == "rate_limit" {
		provider.rateLimited++
		provider.rateLimitStreak++
		cooldown = exponentialRateLimitCooldown(provider.rateLimitStreak)
		if quotaLimitMessage(err.Error()) {
			cooldown = 6 * time.Hour
		}
		provider.recent429Until = time.Now().Add(cooldown + 2*time.Minute)
	}
	provider.cooldownUntil = time.Now().Add(cooldown)
	provider.probationUntil = provider.cooldownUntil.Add(time.Minute)
	if provider.consecutiveFailure >= 3 {
		minimumCooldown := time.Now().Add(60 * time.Second)
		if provider.cooldownUntil.Before(minimumCooldown) {
			provider.cooldownUntil = minimumCooldown
		}
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
	} else {
		provider.latencyEMA = time.Duration(float64(provider.latencyEMA)*0.8 + float64(latency)*0.2)
	}
	provider.latencySamples = append(provider.latencySamples, latency)
	if len(provider.latencySamples) > 256 {
		provider.latencySamples = append(provider.latencySamples[:0], provider.latencySamples[len(provider.latencySamples)-256:]...)
	}
}

func providerFailure(err error) bool {
	var rpcErr *Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	if rpcErr.Kind == RPC {
		return false
	}
	if rpcErr.Kind == HTTP && !strings.Contains(rpcErr.Error(), " 403") && !strings.Contains(rpcErr.Error(), " 429") && !strings.Contains(rpcErr.Error(), " 5") {
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
	now := time.Now()
	var total uint64
	for _, provider := range c.providers {
		total += provider.requests
	}
	out := make([]ProviderSnapshot, 0, len(c.providers))
	for i, provider := range c.providers {
		provider.requestTimes = pruneRequestTimes(provider.requestTimes, now.Add(-time.Second))
		unavailableUntil := provider.cooldownUntil
		if provider.recent429Until.After(unavailableUntil) {
			unavailableUntil = provider.recent429Until
		}
		share := 0.0
		successRate := 0.0
		if total > 0 {
			share = float64(provider.requests) * 100 / float64(total)
		}
		if provider.successes+provider.failures > 0 {
			successRate = float64(provider.successes) * 100 / float64(provider.successes+provider.failures)
		}
		out = append(out, ProviderSnapshot{
			Name:              provider.cfg.Name,
			Healthy:           now.After(unavailableUntil),
			Active:            i == c.active,
			Requests:          provider.requests,
			RateLimited:       provider.rateLimited,
			Failures:          provider.failures,
			Latency:           provider.latencyEMA,
			LatestBlock:       provider.latestBlock,
			CooldownUntil:     provider.cooldownUntil,
			ConsecutiveFailed: provider.consecutiveFailure,
			MaxRPS:            provider.cfg.MaxRPS,
			TargetRPS:         effectiveRPS(provider.cfg),
			SharePct:          share,
			Burst:             effectiveBurst(provider.cfg),
			InstantaneousRPS:  float64(len(provider.requestTimes)),
			CooldownRemaining: positiveDuration(unavailableUntil.Sub(now)),
			Tier:              provider.cfg.Tier, Inflight: provider.inflight,
			ProbationRemaining: positiveDuration(provider.probationUntil.Sub(now)),
			EthCallRequests:    provider.ethCallRequests, QuoteRequests: provider.quoteRequests,
			Successes: provider.successes, SuccessRatePct: successRate, Forbidden: provider.forbidden,
			CooldownEvents: provider.cooldownEvents, LatencyP95: durationP95(provider.latencySamples),
		})
	}
	return out
}

func normalizeTier(tier string) string {
	tier = strings.ToLower(strings.TrimSpace(tier))
	if tier == "cheap" {
		return "secondary"
	}
	if tier != "premium" && tier != "secondary" && tier != "limited" && tier != "emergency" {
		return "secondary"
	}
	return tier
}
func tierRank(class RequestClass, tier string) int {
	switch class {
	case Background:
		if tier == "secondary" {
			return 0
		}
		if tier == "limited" {
			return 1
		}
		if tier == "emergency" {
			return 2
		}
		return 99
	case HotPath, Critical:
		if tier == "premium" {
			return 0
		}
		if tier == "secondary" {
			return 1
		}
		if tier == "limited" {
			return 2
		}
		return 3
	default:
		if tier == "secondary" {
			return 0
		}
		if tier == "limited" {
			return 1
		}
		if tier == "premium" {
			return 2
		}
		return 3
	}
}

func durationP95(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := (len(ordered)*95+99)/100 - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}

type minuteBudget struct {
	mu    sync.Mutex
	limit int
	calls []time.Time
}

func newMinuteBudget(limit int) *minuteBudget {
	if limit <= 0 {
		return nil
	}
	return &minuteBudget{limit: limit}
}
func (b *minuteBudget) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-time.Minute)
	i := 0
	for i < len(b.calls) && b.calls[i].Before(cutoff) {
		i++
	}
	b.calls = append(b.calls[:0], b.calls[i:]...)
	if len(b.calls) >= b.limit {
		return false
	}
	b.calls = append(b.calls, now)
	return true
}

func effectiveRPS(cfg ProviderConfig) int {
	if cfg.TargetRPS > 0 {
		return cfg.TargetRPS
	}
	return cfg.MaxRPS
}

func effectiveBurst(cfg ProviderConfig) int {
	if cfg.Burst < 1 {
		return 1
	}
	if cfg.Burst > 2 {
		return 2
	}
	return cfg.Burst
}

func exponentialRateLimitCooldown(streak uint64) time.Duration {
	if streak < 1 {
		streak = 1
	}
	shift := streak - 1
	if shift > 3 {
		shift = 3
	}
	return 30 * time.Second * time.Duration(uint64(1)<<shift)
}

func pruneRequestTimes(values []time.Time, cutoff time.Time) []time.Time {
	first := 0
	for first < len(values) && values[first].Before(cutoff) {
		first++
	}
	if first == 0 {
		return values
	}
	return append(values[:0], values[first:]...)
}

func positiveDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}

type rateLimiter struct {
	interval time.Duration
	burst    int
	mu       sync.Mutex
	next     time.Time
}

func newRateLimiter(maxRPS int) *rateLimiter {
	return newRateLimiterWithBurst(maxRPS, 1)
}

func newRateLimiterWithBurst(maxRPS, burst int) *rateLimiter {
	if maxRPS <= 0 {
		return &rateLimiter{}
	}
	if burst < 1 {
		burst = 1
	}
	if burst > 2 {
		burst = 2
	}
	return &rateLimiter{interval: time.Second / time.Duration(maxRPS), burst: burst}
}

func (l *rateLimiter) Wait(ctx context.Context) error {
	if l == nil || l.interval <= 0 {
		return nil
	}
	wait := l.ReserveDelay()
	return waitDelay(ctx, wait)
}

func (l *rateLimiter) ReserveDelay() time.Duration {
	if l == nil || l.interval <= 0 {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	burstWindow := time.Duration(l.burst-1) * l.interval
	if l.next.IsZero() || now.After(l.next.Add(burstWindow)) {
		l.next = now.Add(-time.Duration(l.burst-1) * l.interval)
	}
	wait := positiveDuration(l.next.Sub(now))
	l.next = l.next.Add(l.interval)
	return wait
}

func waitDelay(ctx context.Context, wait time.Duration) error {
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

func (l *rateLimiter) Delay() time.Duration {
	if l == nil || l.interval <= 0 {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if wait := time.Until(l.next); wait > 0 {
		return wait
	}
	return 0
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
