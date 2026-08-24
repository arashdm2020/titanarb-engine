package pairintel

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
)

type Caller interface {
	Call(context.Context, string, any, any) error
	EthCall(context.Context, map[string]string) (string, error)
	BlockNumber(context.Context) (uint64, error)
}

type Factory struct {
	Name, Address string
	DEX           pools.DEX
}
type QuoterFor func(pools.Pool) quotes.Quoter

type Service struct {
	Memory         *Memory
	Caller         Caller
	Discoverer     *pools.Discoverer
	Factories      []Factory
	Quoter         QuoterFor
	StatePath      string
	HotBusy        func() bool
	poolEvents     chan pools.Pool
	swapEvents     chan pools.Swap
	probeEvents    chan pools.Pool
	limiter        *pacedLimiter
	started        atomic.Bool
	factoryPools   atomic.Uint64
	duplicates     atomic.Uint64
	rejections     atomic.Uint64
	depthProbes    atomic.Uint64
	mu             sync.Mutex
	seenPools      map[string]struct{}
	lastProbe      map[string]time.Time
	representative map[string]*big.Int
	scanning       atomic.Bool
}

type ServiceSnapshot struct{ FactoryPools, DuplicateEvents, Rejections, DepthProbes uint64 }

func NewService(memory *Memory, caller Caller, discoverer *pools.Discoverer, factories []Factory, quoter QuoterFor, statePath string, hotBusy func() bool) *Service {
	if memory == nil {
		memory = NewMemory(DefaultConfig())
	}
	return &Service{Memory: memory, Caller: caller, Discoverer: discoverer, Factories: append([]Factory(nil), factories...), Quoter: quoter, StatePath: statePath, HotBusy: hotBusy, poolEvents: make(chan pools.Pool, 256), swapEvents: make(chan pools.Swap, 2048), probeEvents: make(chan pools.Pool, 64), limiter: newPacedLimiter(memory.cfg.MaxRPS, memory.cfg.Burst), seenPools: make(map[string]struct{}), lastProbe: make(map[string]time.Time), representative: make(map[string]*big.Int)}
}

func (s *Service) SetRepresentativeAmount(address string, amount *big.Int) {
	if amount == nil || amount.Sign() <= 0 {
		return
	}
	s.mu.Lock()
	s.representative[strings.ToLower(address)] = new(big.Int).Set(amount)
	s.mu.Unlock()
}

func (s *Service) ObservePool(pool pools.Pool) {
	select {
	case s.poolEvents <- pool:
	default:
	}
}
func (s *Service) ObserveSwap(swap pools.Swap) {
	select {
	case s.swapEvents <- swap:
	default:
	}
}

func (s *Service) Run(ctx context.Context) {
	if s == nil || s.Memory == nil || !s.Memory.cfg.Enabled || !s.started.CompareAndSwap(false, true) {
		return
	}
	go s.runProbes(ctx)
	factoryTick := time.NewTicker(time.Minute)
	saveTick := time.NewTicker(time.Minute)
	defer factoryTick.Stop()
	defer saveTick.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = s.Memory.Save(s.StatePath)
			return
		case p := <-s.poolEvents:
			s.Memory.ObservePool(p)
			s.markSeen(p.Address)
			s.queueProbe(p)
		case sw := <-s.swapEvents:
			s.Memory.ObserveSwap(sw)
		case <-factoryTick.C:
			if !s.busy() {
				go s.scanOnce(ctx)
			}
		case <-saveTick.C:
			s.Memory.SelectShadow()
			_ = s.Memory.Save(s.StatePath)
		}
	}
}

func (s *Service) Snapshot() ServiceSnapshot {
	return ServiceSnapshot{s.factoryPools.Load(), s.duplicates.Load(), s.rejections.Load(), s.depthProbes.Load()}
}
func (s *Service) busy() bool { return s.HotBusy != nil && s.HotBusy() }

func (s *Service) scanOnce(ctx context.Context) {
	if s.Caller == nil || s.Discoverer == nil || !s.scanning.CompareAndSwap(false, true) {
		return
	}
	defer s.scanning.Store(false)
	head, err := s.Caller.BlockNumber(ctx)
	if err != nil {
		return
	}
	for _, factory := range s.Factories {
		if s.busy() {
			return
		}
		from := s.Memory.Checkpoint(factory.Name) + 1
		if from == 1 || from > head {
			if head > 1200 {
				from = head - 1200
			} else {
				from = 1
			}
		}
		to := from + 1999
		if to > head {
			to = head
		}
		if from > to {
			continue
		}
		if err := s.limiter.Wait(ctx, s.HotBusy); err != nil {
			return
		}
		var logs []factoryLog
		filter := map[string]any{"address": factory.Address, "fromBlock": fmt.Sprintf("0x%x", from), "toBlock": fmt.Sprintf("0x%x", to)}
		if err := s.Caller.Call(ctx, "eth_getLogs", []any{filter}, &logs); err != nil {
			continue
		}
		for _, entry := range logs {
			candidate, ok := decodeFactoryLog(factory, entry)
			if !ok {
				continue
			}
			if s.duplicate(candidate.Pool) {
				s.duplicates.Add(1)
				continue
			}
			if !s.validateToken(ctx, candidate.Token0) || !s.validateToken(ctx, candidate.Token1) {
				s.rejections.Add(1)
				continue
			}
			if s.busy() {
				return
			}
			if err := s.limiter.Wait(ctx, s.HotBusy); err != nil {
				return
			}
			updated, err := s.Discoverer.RefreshPoolAt(ctx, pools.Pool{Address: candidate.Pool, Token0: candidate.Token0, Token1: candidate.Token1, DEX: factory.DEX, Fee: candidate.Fee}, candidate.Block)
			if err != nil || updated.Liquidity == nil || updated.Liquidity.Sign() <= 0 {
				s.rejections.Add(1)
				continue
			}
			s.Memory.ObservePool(updated)
			s.factoryPools.Add(1)
			s.queueProbe(updated)
		}
		s.Memory.SetCheckpoint(factory.Name, to)
	}
	_ = s.Memory.Save(s.StatePath)
}

func (s *Service) duplicate(pool string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(pool)
	if _, ok := s.seenPools[key]; ok {
		return true
	}
	s.seenPools[key] = struct{}{}
	return false
}
func (s *Service) markSeen(pool string) {
	s.mu.Lock()
	s.seenPools[strings.ToLower(pool)] = struct{}{}
	s.mu.Unlock()
}
func (s *Service) queueProbe(pool pools.Pool) {
	s.mu.Lock()
	last := s.lastProbe[strings.ToLower(pool.Address)]
	if time.Since(last) < time.Hour {
		s.mu.Unlock()
		return
	}
	s.lastProbe[strings.ToLower(pool.Address)] = time.Now()
	s.mu.Unlock()
	select {
	case s.probeEvents <- pool:
	default:
	}
}
func (s *Service) runProbes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case pool := <-s.probeEvents:
			for s.busy() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(250 * time.Millisecond):
				}
			}
			s.probeDepth(ctx, pool)
		}
	}
}

func (s *Service) validateToken(ctx context.Context, address string) bool {
	address = strings.ToLower(address)
	s.Memory.mu.RLock()
	_, known := s.Memory.tokens[address]
	s.Memory.mu.RUnlock()
	if known {
		return true
	}
	if err := s.limiter.Wait(ctx, s.HotBusy); err != nil {
		return false
	}
	var code string
	if err := s.Caller.Call(ctx, "eth_getCode", []any{address, "latest"}, &code); err != nil || code == "0x" || code == "0x0" {
		return false
	}
	if err := s.limiter.Wait(ctx, s.HotBusy); err != nil {
		return false
	}
	raw, err := s.Caller.EthCall(ctx, map[string]string{"to": address, "data": dex.StaticCall("decimals()")})
	if err != nil {
		return false
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) == 0 {
		return false
	}
	d := dex.WordUint(words[0])
	if !d.IsUint64() || d.Uint64() > 36 {
		return false
	}
	return s.Memory.RegisterToken(TokenMeta{Address: address, Decimals: uint8(d.Uint64()), HasCode: true}) == nil
}

func (s *Service) probeDepth(ctx context.Context, pool pools.Pool) {
	if s.Quoter == nil {
		return
	}
	q := s.Quoter(pool)
	if q == nil {
		return
	}
	s.Memory.mu.RLock()
	t0, ok0 := s.Memory.tokens[strings.ToLower(pool.Token0)]
	t1, ok1 := s.Memory.tokens[strings.ToLower(pool.Token1)]
	s.Memory.mu.RUnlock()
	if !ok0 || !ok1 {
		return
	}
	for _, direction := range []struct {
		in, out TokenMeta
		name    string
	}{{t0, t1, "0to1"}, {t1, t0, "1to0"}} {
		base := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(direction.in.Decimals)), nil)
		s.mu.Lock()
		if configured := s.representative[strings.ToLower(direction.in.Address)]; configured != nil {
			base = new(big.Int).Div(new(big.Int).Set(configured), big.NewInt(10))
			if base.Sign() <= 0 {
				base = big.NewInt(1)
			}
		}
		s.mu.Unlock()
		for _, bucket := range []struct {
			name string
			mul  int64
		}{{"small", 1}, {"medium", 10}, {"large", 100}} {
			if s.busy() {
				return
			}
			if err := s.limiter.Wait(ctx, s.HotBusy); err != nil {
				return
			}
			amount := new(big.Int).Mul(base, big.NewInt(bucket.mul))
			result, err := q.Quote(ctx, quotes.Request{TokenIn: direction.in.Address, TokenOut: direction.out.Address, AmountIn: amount, Pool: pool})
			depth := Depth{Direction: direction.name, Bucket: bucket.name, AmountIn: amount.String(), AmountInNormalized: normalizedAbs(amount, direction.in.Decimals), ObservedAt: time.Now().UTC(), Successful: err == nil && result.AmountOut != nil && result.AmountOut.Sign() > 0}
			if depth.Successful {
				depth.AmountOut = result.AmountOut.String()
				depth.PriceImpactBPS = depthImpact(pool, direction.in, direction.out, amount, result.AmountOut)
				depth.Score = clamp(1 - depth.PriceImpactBPS/500)
			}
			s.Memory.RecordDepth(pool.Address, depth)
			s.depthProbes.Add(1)
		}
	}
}

type factoryLog struct {
	Address, Data, BlockNumber string
	Topics                     []string
}
type factoryPool struct {
	Pool, Token0, Token1 string
	Fee                  uint32
	Block                uint64
}

var uniPoolCreated = strings.ToLower(dex.EventTopic("PoolCreated(address,address,uint24,int24,address)"))
var algebraPool = strings.ToLower(dex.EventTopic("Pool(address,address,address)"))

func decodeFactoryLog(factory Factory, l factoryLog) (factoryPool, bool) {
	if len(l.Topics) < 3 {
		return factoryPool{}, false
	}
	topic := strings.ToLower(l.Topics[0])
	if factory.DEX == pools.UniswapV3 && topic != uniPoolCreated {
		return factoryPool{}, false
	}
	if factory.DEX == pools.CamelotV3 && topic != algebraPool {
		return factoryPool{}, false
	}
	t0 := topicAddress(l.Topics[1])
	t1 := topicAddress(l.Topics[2])
	raw, err := hex.DecodeString(strings.TrimPrefix(l.Data, "0x"))
	if err != nil {
		return factoryPool{}, false
	}
	out := factoryPool{Token0: t0, Token1: t1, Block: hexUint(l.BlockNumber)}
	if factory.DEX == pools.UniswapV3 {
		if len(l.Topics) < 4 || len(raw) < 64 {
			return factoryPool{}, false
		}
		out.Fee = uint32(hexUint(l.Topics[3]))
		out.Pool = "0x" + hex.EncodeToString(raw[44:64])
	} else {
		if len(raw) < 32 {
			return factoryPool{}, false
		}
		out.Pool = "0x" + hex.EncodeToString(raw[12:32])
	}
	_, err = CanonicalPair(out.Token0, out.Token1)
	return out, err == nil && validAddress(out.Pool)
}
func topicAddress(s string) string {
	raw := strings.TrimPrefix(strings.ToLower(s), "0x")
	if len(raw) < 40 {
		return ""
	}
	return "0x" + raw[len(raw)-40:]
}
func hexUint(s string) uint64 {
	v, _ := strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
	return v
}

func depthImpact(pool pools.Pool, in, out TokenMeta, amountIn, amountOut *big.Int) float64 {
	price := canonicalPrice(pool.SqrtPriceX96, tokenDecimals(pool.Token0, in, out), tokenDecimals(pool.Token1, in, out))
	if price <= 0 {
		return 10000
	}
	inNorm := normalizedAbs(amountIn, in.Decimals)
	outNorm := normalizedAbs(amountOut, out.Decimals)
	expected := inNorm * price
	if !strings.EqualFold(in.Address, pool.Token0) {
		expected = inNorm / price
	}
	if expected <= 0 || outNorm >= expected {
		return 0
	}
	return math.Min(10000, (expected-outNorm)/expected*10000)
}
func tokenDecimals(address string, a, b TokenMeta) uint8 {
	if strings.EqualFold(address, a.Address) {
		return a.Decimals
	}
	return b.Decimals
}

type pacedLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
	burst    int
}

func newPacedLimiter(rps float64, burst int) *pacedLimiter {
	if rps <= 0 {
		rps = .5
	}
	if burst < 1 {
		burst = 1
	}
	return &pacedLimiter{interval: time.Duration(float64(time.Second) / rps), burst: burst}
}
func (l *pacedLimiter) Wait(ctx context.Context, busy func() bool) error {
	for {
		if busy != nil && busy() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		l.mu.Lock()
		now := time.Now()
		wait := l.next.Sub(now)
		if wait <= 0 {
			l.next = now.Add(l.interval)
			l.mu.Unlock()
			return nil
		}
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func ConfigFromLookup(get func(string) string) Config {
	cfg := DefaultConfig()
	if raw := strings.TrimSpace(get("PAIR_SCORING_ENABLED")); raw != "" {
		cfg.Enabled, _ = strconv.ParseBool(raw)
	}
	if mode := strings.ToLower(strings.TrimSpace(get("PAIR_ADMISSION_MODE"))); mode == "telemetry" || mode == "shadow" || mode == "live" {
		cfg.Mode = mode
	}
	cfg.MaxTrackedPairs = parseInt(get("PAIR_MAX_TRACKED_PAIRS"), 16)
	cfg.MaxShadowPairs = parseInt(get("PAIR_MAX_SHADOW_PAIRS"), 8)
	cfg.MaxDynamicAssets = parseInt(get("PAIR_MAX_DYNAMIC_ASSETS"), 4)
	cfg.MinObservation = time.Duration(parseInt(get("PAIR_MIN_OBSERVATION_MINUTES"), 60)) * time.Minute
	cfg.MaxRPS = parseFloat(get("PAIR_INTELLIGENCE_MAX_RPS"), .5)
	cfg.Burst = parseInt(get("PAIR_INTELLIGENCE_BURST"), 1)
	return cfg
}
func parseInt(raw string, fallback int) int {
	v, e := strconv.Atoi(strings.TrimSpace(raw))
	if e != nil || v < 1 {
		return fallback
	}
	return v
}
func parseFloat(raw string, fallback float64) float64 {
	v, e := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if e != nil || v <= 0 {
		return fallback
	}
	return v
}
func FromEnv() Config { return ConfigFromLookup(os.Getenv) }
