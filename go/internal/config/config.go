// Package config loads and validates the Go foundation runtime configuration.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

const ArbitrumOneChainID int64 = 42161

var addressPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// Config contains only the fields required by the Go foundation. PrivateKey is
// loaded for parity with the production environment but is never used in Phase 1.
type Config struct {
	ChainID                  int64
	HTTPRPCURL               string
	WSRPCURL                 string
	FlashExecutorAddress     string
	UniswapV3Adapter         string
	CamelotV3Adapter         string
	ProfitRecipient          string
	PrivateKey               string
	DryRun                   bool
	ExecutionMode            string
	BroadcastEnabled         bool
	SlippageBPS              uint64
	DeadlineSeconds          uint64
	GasLimitMultiplierBPS    uint64
	PriceMaxStalenessSeconds uint64
	SequencerGraceSeconds    uint64
}

// Token is a configured Arbitrum asset. Addresses are deliberately sourced
// from config/arbitrum.json rather than duplicated in Go.
type Token struct {
	Symbol   string `json:"symbol"`
	Address  string `json:"address"`
	Decimals uint8  `json:"decimals"`
	USDFeed  string `json:"usdFeed,omitempty"`
}

// MarketConfig contains the read-only protocol metadata used by Phase 2.
type MarketConfig struct {
	AavePool            string
	UniswapFactory      string
	UniswapSwapRouter   string
	UniswapQuoterV2     string
	CamelotFactory      string
	CamelotSwapRouter   string
	CamelotQuoter       string
	ETHUSDFeed          string
	SequencerUptimeFeed string
	ArbGasInfo          string
	BaseAsset           string
	IntermediateTokens  []string
	UniswapFeeTiers     []uint32
	Tokens              map[string]Token
}

// LoadMarketConfig reads the repository's canonical Arbitrum JSON file. It
// does not consult or emit secrets.
func LoadMarketConfig(path string) (MarketConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return MarketConfig{}, err
	}
	var raw struct {
		AaveV3 struct {
			Pool string `json:"pool"`
		} `json:"aaveV3"`
		UniswapV3 struct {
			Factory    string `json:"factory"`
			QuoterV2   string `json:"quoterV2"`
			SwapRouter string `json:"swapRouter"`
		} `json:"uniswapV3"`
		CamelotV3 struct {
			Factory    string `json:"algebraFactory"`
			Quoter     string `json:"quoter"`
			SwapRouter string `json:"swapRouter"`
		} `json:"camelotV3"`
		Chainlink struct {
			ETHUSDFeed          string `json:"ethUsdFeed"`
			SequencerUptimeFeed string `json:"sequencerUptimeFeed"`
		} `json:"chainlink"`
		ArbGasInfo struct {
			Precompile string `json:"precompile"`
		} `json:"arbGasInfo"`
		RouteDiscovery struct {
			BaseAsset          string   `json:"baseAsset"`
			IntermediateTokens []string `json:"intermediateTokens"`
			UniswapFeeTiers    []uint32 `json:"uniswapFeeTiers"`
		} `json:"routeDiscovery"`
		Tokens map[string]Token `json:"tokens"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return MarketConfig{}, err
	}
	cfg := MarketConfig{
		AavePool: raw.AaveV3.Pool, UniswapFactory: raw.UniswapV3.Factory, UniswapSwapRouter: raw.UniswapV3.SwapRouter,
		UniswapQuoterV2: raw.UniswapV3.QuoterV2, CamelotFactory: raw.CamelotV3.Factory, CamelotSwapRouter: raw.CamelotV3.SwapRouter,
		CamelotQuoter: raw.CamelotV3.Quoter, ETHUSDFeed: raw.Chainlink.ETHUSDFeed, SequencerUptimeFeed: raw.Chainlink.SequencerUptimeFeed, ArbGasInfo: raw.ArbGasInfo.Precompile,
		BaseAsset: raw.RouteDiscovery.BaseAsset, IntermediateTokens: raw.RouteDiscovery.IntermediateTokens,
		UniswapFeeTiers: raw.RouteDiscovery.UniswapFeeTiers, Tokens: raw.Tokens,
	}
	if cfg.BaseAsset == "" || len(cfg.Tokens) == 0 {
		return MarketConfig{}, fmt.Errorf("market configuration is incomplete")
	}
	for name, token := range cfg.Tokens {
		if !addressPattern.MatchString(token.Address) || token.Symbol == "" {
			return MarketConfig{}, fmt.Errorf("token %s is invalid", name)
		}
		if token.USDFeed != "" && !addressPattern.MatchString(token.USDFeed) {
			return MarketConfig{}, fmt.Errorf("token %s has an invalid USD feed", name)
		}
	}
	return cfg, nil
}

// USDFeeds exposes only explicitly configured token-price sources. It does not
// infer token economics from an asset symbol or assume a stablecoin peg.
func (m MarketConfig) USDFeeds() map[string]string {
	feeds := make(map[string]string)
	for _, token := range m.Tokens {
		if token.USDFeed != "" {
			feeds[token.Symbol] = token.USDFeed
		}
	}
	return feeds
}

// Load reads .env without overriding explicit environment variables.
func Load() (Config, error) {
	if path := os.Getenv("TITANARB_ENV_FILE"); path != "" {
		_ = godotenv.Load(path)
	} else {
		for _, path := range []string{".env", filepath.Join("..", ".env")} {
			if _, err := os.Stat(path); err == nil {
				_ = godotenv.Load(path)
				break
			}
		}
	}
	return FromLookup(os.Getenv)
}

// FromLookup enables deterministic tests without changing process environment.
func FromLookup(get func(string) string) (Config, error) {
	chainID, err := strconv.ParseInt(strings.TrimSpace(get("CHAIN_ID")), 10, 64)
	if err != nil {
		// Python keeps the immutable Arbitrum chain ID in config/arbitrum.json;
		// defaulting here preserves that source-of-truth behaviour.
		chainID = ArbitrumOneChainID
	}
	dryRun, err := strconv.ParseBool(strings.TrimSpace(get("DRY_RUN")))
	if err != nil {
		return Config{}, fmt.Errorf("DRY_RUN must be true or false")
	}
	cfg := Config{
		ChainID:              chainID,
		HTTPRPCURL:           firstNonEmpty(get("HTTP_RPC_URL"), get("ARBITRUM_RPC_URL")),
		WSRPCURL:             firstNonEmpty(get("WS_RPC_URL"), get("ARBITRUM_WSS_RPC_URL")),
		FlashExecutorAddress: strings.TrimSpace(get("FLASH_EXECUTOR_ADDRESS")),
		UniswapV3Adapter:     strings.TrimSpace(get("UNISWAP_V3_ADAPTER")),
		CamelotV3Adapter:     strings.TrimSpace(get("CAMELOT_V3_ADAPTER")),
		ProfitRecipient:      strings.TrimSpace(get("PROFIT_RECIPIENT")),
		PrivateKey:           strings.TrimSpace(get("PRIVATE_KEY")), DryRun: dryRun,
		ExecutionMode: strings.TrimSpace(strings.ToLower(get("EXECUTION_MODE"))),
		// Mainnet broadcasting is opt-in independently from the shared runtime
		// flags. Local-fork validation sets this only in its child process.
		BroadcastEnabled:         strings.EqualFold(strings.TrimSpace(get("GO_LIVE_EXECUTION")), "true"),
		SlippageBPS:              parseUintDefault(get("SLIPPAGE_BPS"), 50),
		DeadlineSeconds:          parseUintDefault(get("TX_DEADLINE_SECONDS"), 120),
		GasLimitMultiplierBPS:    parseMultiplierBPS(get("GAS_LIMIT_MULTIPLIER"), 12000),
		PriceMaxStalenessSeconds: parseUintDefault(get("PRICE_MAX_STALENESS_SECONDS"), 3600),
		SequencerGraceSeconds:    parseUintDefault(get("SEQUENCER_GRACE_PERIOD_SECONDS"), 3600),
	}
	return cfg, cfg.Validate()
}

func parseUintDefault(raw string, fallback uint64) uint64 {
	v, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

// parseMultiplierBPS accepts the existing Python-style decimal multiplier.
func parseMultiplierBPS(raw string, fallback uint64) uint64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	parts := strings.SplitN(raw, ".", 2)
	whole, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return fallback
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > 4 {
		fraction = fraction[:4]
	}
	for len(fraction) < 4 {
		fraction += "0"
	}
	f, err := strconv.ParseUint(fraction, 10, 64)
	if err != nil {
		return fallback
	}
	return whole*10_000 + f
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (c Config) Validate() error {
	if c.ChainID != ArbitrumOneChainID {
		return fmt.Errorf("CHAIN_ID must equal %d", ArbitrumOneChainID)
	}
	if c.HTTPRPCURL == "" {
		return fmt.Errorf("ARBITRUM_RPC_URL is required")
	}
	if c.WSRPCURL == "" {
		return fmt.Errorf("ARBITRUM_WSS_RPC_URL is required")
	}
	for name, value := range map[string]string{
		"FLASH_EXECUTOR_ADDRESS": c.FlashExecutorAddress, "UNISWAP_V3_ADAPTER": c.UniswapV3Adapter,
		"CAMELOT_V3_ADAPTER": c.CamelotV3Adapter,
	} {
		if !addressPattern.MatchString(value) {
			return fmt.Errorf("%s must be a valid address", name)
		}
	}
	if c.ProfitRecipient != "" && !addressPattern.MatchString(c.ProfitRecipient) {
		return fmt.Errorf("PROFIT_RECIPIENT must be a valid address")
	}
	if c.PrivateKey == "" {
		return fmt.Errorf("PRIVATE_KEY is required")
	}
	if c.ExecutionMode != "live" && c.ExecutionMode != "dry_run" {
		return fmt.Errorf("EXECUTION_MODE must be live or dry_run")
	}
	if c.SlippageBPS > 10_000 || c.DeadlineSeconds == 0 || c.GasLimitMultiplierBPS < 10_000 {
		return fmt.Errorf("invalid execution safety configuration")
	}
	return nil
}
