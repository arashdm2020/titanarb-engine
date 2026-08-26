// Package config loads and validates the Go foundation runtime configuration.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

const ArbitrumOneChainID int64 = 42161

var addressPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// Config contains only the fields required by the Go foundation. PrivateKey is
// loaded for parity with the production environment but is never used in Phase 1.
type Config struct {
	ChainID                     int64
	HTTPRPCURL                  string
	WSRPCURL                    string
	RPCProviders                []RPCProviderConfig
	RPCReadTargetRPS            int
	RPCPremiumAggregateRPS      int
	RPCMaxEthCallsPerMinute     int
	RPCMaxHotCallsPerMinute     int
	MaxCandidateStalenessBlocks uint64
	FlashExecutorAddress        string
	UniswapV3Adapter            string
	CamelotV3Adapter            string
	ProfitRecipient             string
	PrivateKey                  string
	DryRun                      bool
	ExecutionMode               string
	BroadcastEnabled            bool
	SlippageBPS                 uint64
	DeadlineSeconds             uint64
	GasLimitMultiplierBPS       uint64
	PriceMaxStalenessSeconds    uint64
	SequencerGraceSeconds       uint64
}

type RPCProviderConfig struct {
	Name        string
	HTTP        string
	WSS         string
	MaxRPS      int
	TargetRPS   int
	Burst       int
	MaxBlockLag int
	Tier        string
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
	ExecutionAssetNames []string
	MarketAssetNames    []string
	BaseAsset           string
	IntermediateTokens  []string
	UniswapFeeTiers     []uint32
	Tokens              map[string]Token
}

// ExecutionAssets returns the deployed executor's allow-listed universe.
// executionAssets is neutral membership metadata; its order is normalized and
// never used for ranking or as a preferred route start. BaseAsset is retained
// only as a compatibility fallback for older configuration files.
func (m MarketConfig) ExecutionAssets() []string {
	seen := make(map[string]struct{})
	configured := m.ExecutionAssetNames
	if len(configured) == 0 {
		configured = append([]string{m.BaseAsset}, m.IntermediateTokens...)
	}
	for _, symbol := range configured {
		if _, ok := m.Tokens[symbol]; ok && symbol != "" {
			seen[symbol] = struct{}{}
		}
	}
	assets := make([]string, 0, len(seen))
	for symbol := range seen {
		assets = append(assets, symbol)
	}
	sort.Strings(assets)
	return assets
}

// MarketAssets is the read-only graph membership. It may include observed or
// operator-approved intermediate assets, but never grants flash-loan/start or
// execution eligibility. When unset it is exactly ExecutionAssets.
func (m MarketConfig) MarketAssets() []string {
	configured := m.MarketAssetNames
	if len(configured) == 0 {
		return m.ExecutionAssets()
	}
	seen := make(map[string]struct{})
	for _, symbol := range configured {
		if _, ok := m.Tokens[symbol]; ok && symbol != "" {
			seen[symbol] = struct{}{}
		}
	}
	for _, symbol := range m.ExecutionAssets() {
		seen[symbol] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for symbol := range seen {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// DiscoveryAssets is the broader, read-only token registry. Tokens outside
// ExecutionAssets may be inspected but cannot be sent to the deployed
// executor until its on-chain allow-list is explicitly updated.
func (m MarketConfig) DiscoveryAssets() []string {
	assets := make([]string, 0, len(m.Tokens))
	for symbol := range m.Tokens {
		assets = append(assets, symbol)
	}
	sort.Strings(assets)
	return assets
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
			ExecutionAssets    []string `json:"executionAssets"`
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
		ExecutionAssetNames: raw.RouteDiscovery.ExecutionAssets, BaseAsset: raw.RouteDiscovery.BaseAsset, IntermediateTokens: raw.RouteDiscovery.IntermediateTokens,
		UniswapFeeTiers: raw.RouteDiscovery.UniswapFeeTiers, Tokens: raw.Tokens,
	}
	if len(cfg.ExecutionAssets()) == 0 || len(cfg.Tokens) == 0 {
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
	for _, symbol := range cfg.ExecutionAssetNames {
		if _, ok := cfg.Tokens[symbol]; !ok {
			return MarketConfig{}, fmt.Errorf("execution asset %s is not configured", symbol)
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
		ChainID:                     chainID,
		HTTPRPCURL:                  firstNonEmpty(get("RPC_PRIMARY_HTTP"), get("HTTP_RPC_URL"), get("ARBITRUM_RPC_URL")),
		WSRPCURL:                    firstNonEmpty(get("RPC_PRIMARY_WSS"), get("WS_RPC_URL"), get("ARBITRUM_WSS_RPC_URL")),
		RPCProviders:                rpcProvidersFromLookup(get),
		RPCReadTargetRPS:            int(parseUintDefault(get("RPC_READ_TARGET_RPS"), 8)),
		RPCPremiumAggregateRPS:      int(parseUintDefault(get("RPC_ALCHEMY_AGGREGATE_RPS"), 6)),
		RPCMaxEthCallsPerMinute:     int(parseUintDefault(get("RPC_MAX_ETH_CALLS_PER_MINUTE"), 480)),
		RPCMaxHotCallsPerMinute:     int(parseUintDefault(firstNonEmpty(get("RPC_MAX_HOT_CALLS_PER_MINUTE"), get("MAX_QUOTES_PER_MINUTE")), 360)),
		MaxCandidateStalenessBlocks: parseUintDefault(get("MAX_CANDIDATE_STALENESS_BLOCKS"), 2),
		FlashExecutorAddress:        strings.TrimSpace(get("FLASH_EXECUTOR_ADDRESS")),
		UniswapV3Adapter:            strings.TrimSpace(get("UNISWAP_V3_ADAPTER")),
		CamelotV3Adapter:            strings.TrimSpace(get("CAMELOT_V3_ADAPTER")),
		ProfitRecipient:             strings.TrimSpace(get("PROFIT_RECIPIENT")),
		PrivateKey:                  strings.TrimSpace(get("PRIVATE_KEY")), DryRun: dryRun,
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

func rpcProvidersFromLookup(get func(string) string) []RPCProviderConfig {
	primary := RPCProviderConfig{
		Name:        firstNonEmpty(get("RPC_PRIMARY_NAME"), "primary"),
		HTTP:        strings.TrimSpace(get("RPC_PRIMARY_HTTP")),
		WSS:         strings.TrimSpace(get("RPC_PRIMARY_WSS")),
		MaxRPS:      int(parseUintDefault(get("RPC_PRIMARY_MAX_RPS"), 0)),
		TargetRPS:   int(parseUintDefault(firstNonEmpty(get("RPC_PRIMARY_TARGET_RPS"), get("RPC_QUICKNODE_TARGET_RPS")), 0)),
		Burst:       int(parseUintDefault(get("RPC_PRIMARY_BURST"), 1)),
		MaxBlockLag: int(parseUintDefault(get("RPC_PRIMARY_MAX_BLOCK_LAG"), parseUintDefault(get("RPC_PROVIDER_MAX_BLOCK_LAG"), 5))),
		Tier:        firstNonEmpty(get("RPC_PRIMARY_TIER"), inferredProviderTier(firstNonEmpty(get("RPC_PRIMARY_NAME"), "primary"))),
	}
	secondary := RPCProviderConfig{
		Name:        firstNonEmpty(get("RPC_SECONDARY_NAME"), "secondary"),
		HTTP:        strings.TrimSpace(get("RPC_SECONDARY_HTTP")),
		WSS:         strings.TrimSpace(get("RPC_SECONDARY_WSS")),
		MaxRPS:      int(parseUintDefault(get("RPC_SECONDARY_MAX_RPS"), 0)),
		TargetRPS:   int(parseUintDefault(firstNonEmpty(get("RPC_SECONDARY_TARGET_RPS"), get("RPC_CHAINSTACK_TARGET_RPS")), 0)),
		Burst:       int(parseUintDefault(get("RPC_SECONDARY_BURST"), 1)),
		MaxBlockLag: int(parseUintDefault(get("RPC_SECONDARY_MAX_BLOCK_LAG"), parseUintDefault(get("RPC_PROVIDER_MAX_BLOCK_LAG"), 5))),
		Tier:        firstNonEmpty(get("RPC_SECONDARY_TIER"), inferredProviderTier(firstNonEmpty(get("RPC_SECONDARY_NAME"), "secondary"))),
	}
	if primary.HTTP == "" {
		primary.HTTP = firstNonEmpty(get("HTTP_RPC_URL"), get("ARBITRUM_RPC_URL"))
	}
	if primary.WSS == "" {
		primary.WSS = firstNonEmpty(get("WS_RPC_URL"), get("ARBITRUM_WSS_RPC_URL"))
	}
	var providers []RPCProviderConfig
	if primary.HTTP != "" || primary.WSS != "" {
		providers = append(providers, primary)
	}
	if secondary.HTTP != "" || secondary.WSS != "" {
		providers = append(providers, secondary)
	}
	alchemyRPS := int(parseUintDefault(get("RPC_ALCHEMY_ENDPOINT_RPS"), 3))
	alchemyBurst := int(parseUintDefault(get("RPC_ALCHEMY_ENDPOINT_BURST"), 1))
	for i := 1; i <= 3; i++ {
		url := strings.TrimSpace(get(fmt.Sprintf("RPC_ALCHEMY_HTTP_%d", i)))
		if url == "" {
			continue
		}
		providers = append(providers, RPCProviderConfig{Name: fmt.Sprintf("alchemy_%d", i), HTTP: url, MaxRPS: alchemyRPS, TargetRPS: alchemyRPS, Burst: alchemyBurst, MaxBlockLag: primary.MaxBlockLag, Tier: "premium"})
	}
	ankrRPS := int(parseUintDefault(get("RPC_ANKR_ENDPOINT_RPS"), 4))
	ankrBurst := int(parseUintDefault(get("RPC_ANKR_ENDPOINT_BURST"), 1))
	for i := 1; i <= 2; i++ {
		url := strings.TrimSpace(get(fmt.Sprintf("RPC_ANKR_HTTP_%d", i)))
		if url == "" {
			continue
		}
		providers = append(providers, RPCProviderConfig{Name: fmt.Sprintf("ankr_%d", i), HTTP: url, MaxRPS: ankrRPS, TargetRPS: ankrRPS, Burst: ankrBurst, MaxBlockLag: primary.MaxBlockLag, Tier: "secondary"})
	}
	if url := strings.TrimSpace(get("RPC_QUICKNODE_HTTP")); url != "" {
		quicknodeRPS := int(parseUintDefault(get("RPC_QUICKNODE_TARGET_RPS"), 1))
		providers = append(providers, RPCProviderConfig{Name: "quicknode", HTTP: url, MaxRPS: quicknodeRPS, TargetRPS: quicknodeRPS, Burst: int(parseUintDefault(get("RPC_QUICKNODE_BURST"), 1)), MaxBlockLag: primary.MaxBlockLag, Tier: "limited"})
	}
	return providers
}

func inferredProviderTier(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	switch {
	case strings.Contains(name, "alchemy"):
		return "premium"
	case strings.Contains(name, "ankr"), strings.Contains(name, "chainstack"):
		return "secondary"
	case strings.Contains(name, "quicknode"):
		return "limited"
	case strings.Contains(name, "official"), strings.Contains(name, "public"), strings.Contains(name, "arbitrum"):
		return "emergency"
	default:
		return "secondary"
	}
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
