// Package runtimeconfig owns mutable operator risk settings. It is separate
// from .env: defaults are loaded at startup, while approved live adjustments
// are atomically persisted and survive a restart.
package runtimeconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Profile string

const (
	Conservative Profile = "CONSERVATIVE"
	Balanced     Profile = "BALANCED"
	Aggressive   Profile = "AGGRESSIVE"
	Custom       Profile = "CUSTOM"
)

type Settings struct {
	Profile              Profile   `json:"profile"`
	MinProfitUSD         float64   `json:"min_profit_usd"`
	MaxPriceImpactBPS    uint64    `json:"max_price_impact_bps"`
	SlippageBPS          uint64    `json:"slippage_bps"`
	LiquidityUtilization float64   `json:"liquidity_utilization"`
	OptimizerDepth       int       `json:"optimizer_depth"`
	RouteSearchDepth     int       `json:"route_search_depth"`
	VolatilityWeight     float64   `json:"volatility_weight"`
	GasSafetyMarginBPS   uint64    `json:"gas_safety_margin_bps"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func Defaults(profile Profile) Settings {
	var s Settings
	switch profile {
	case Conservative:
		s = Settings{Profile: Conservative, MinProfitUSD: 10, MaxPriceImpactBPS: 75, SlippageBPS: 50, LiquidityUtilization: .20, OptimizerDepth: 5, RouteSearchDepth: 3, VolatilityWeight: .75, GasSafetyMarginBPS: 1500}
	case Aggressive:
		s = Settings{Profile: Aggressive, MinProfitUSD: 2, MaxPriceImpactBPS: 200, SlippageBPS: 100, LiquidityUtilization: .40, OptimizerDepth: 12, RouteSearchDepth: 4, VolatilityWeight: 1.5, GasSafetyMarginBPS: 1000}
	default:
		s = Settings{Profile: Balanced, MinProfitUSD: 5, MaxPriceImpactBPS: 125, SlippageBPS: 75, LiquidityUtilization: .30, OptimizerDepth: 8, RouteSearchDepth: 4, VolatilityWeight: 1, GasSafetyMarginBPS: 1200}
	}
	s.UpdatedAt = time.Now().UTC()
	return s
}

func (s Settings) Validate() error {
	if s.Profile != Conservative && s.Profile != Balanced && s.Profile != Aggressive && s.Profile != Custom {
		return fmt.Errorf("unknown risk profile")
	}
	if s.MinProfitUSD <= 0 || s.MinProfitUSD > 1_000_000 || s.MaxPriceImpactBPS > 2_000 || s.SlippageBPS > 1_000 || s.LiquidityUtilization <= 0 || s.LiquidityUtilization > .50 || s.OptimizerDepth < 2 || s.OptimizerDepth > 32 || s.RouteSearchDepth < 2 || s.RouteSearchDepth > 4 || s.VolatilityWeight < 0 || s.VolatilityWeight > 5 || s.GasSafetyMarginBPS < 500 || s.GasSafetyMarginBPS > 5_000 {
		return fmt.Errorf("risk settings outside safe range")
	}
	return nil
}

type Manager struct {
	mu   sync.RWMutex
	path string
	data Settings
}

func Open(path string, fallback Settings) (*Manager, error) {
	if err := fallback.Validate(); err != nil {
		return nil, err
	}
	m := &Manager{path: path, data: fallback}
	if strings.TrimSpace(path) == "" {
		return m, nil
	}
	contents, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	var saved Settings
	if err := json.Unmarshal(contents, &saved); err != nil || saved.Validate() != nil {
		return nil, fmt.Errorf("invalid persisted runtime settings")
	}
	m.data = saved
	return m, nil
}

func (m *Manager) Snapshot() Settings {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data
}

func (m *Manager) SetProfile(profile Profile) (Settings, error) {
	if profile == Custom {
		m.mu.RLock()
		next := m.data
		m.mu.RUnlock()
		next.Profile = Custom
		return m.update(next)
	}
	if profile != Conservative && profile != Balanced && profile != Aggressive {
		return Settings{}, fmt.Errorf("unknown risk profile")
	}
	return m.update(Defaults(profile))
}

func (m *Manager) Set(key, value string) (Settings, error) {
	m.mu.RLock()
	next := m.data
	m.mu.RUnlock()
	next.Profile = Custom
	key = strings.ToLower(strings.TrimSpace(key))
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return Settings{}, fmt.Errorf("%s must be numeric", key)
	}
	switch key {
	case "min_profit_usd":
		next.MinProfitUSD = parsed
	case "max_price_impact_bps":
		next.MaxPriceImpactBPS = uint64(parsed)
	case "slippage_bps":
		next.SlippageBPS = uint64(parsed)
	case "liquidity_utilization":
		next.LiquidityUtilization = parsed
	case "optimizer_depth":
		next.OptimizerDepth = int(parsed)
	case "route_search_depth":
		next.RouteSearchDepth = int(parsed)
	case "volatility_weight":
		next.VolatilityWeight = parsed
	default:
		return Settings{}, fmt.Errorf("unsupported runtime setting %q", key)
	}
	if parsed < 0 {
		return Settings{}, fmt.Errorf("%s must be non-negative", key)
	}
	return m.update(next)
}

func (m *Manager) update(next Settings) (Settings, error) {
	next.UpdatedAt = time.Now().UTC()
	if err := next.Validate(); err != nil {
		return Settings{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.path != "" {
		if err := writeAtomic(m.path, next); err != nil {
			return Settings{}, err
		}
	}
	m.data = next
	return next, nil
}

func writeAtomic(path string, data Settings) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".runtime-config-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(encoded); err != nil || tmp.Chmod(0o600) != nil || tmp.Close() != nil {
		_ = tmp.Close()
		return fmt.Errorf("write runtime config")
	}
	return os.Rename(name, path)
}
