package alpha

import (
	"context"
	"fmt"
	"math/big"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/fees"
	"github.com/titanarb/titanarb-go/internal/graph"
)

type Caller interface {
	EthCall(context.Context, map[string]string) (string, error)
}
type PriceSource interface {
	USD(context.Context, config.Token) (*big.Rat, error)
}

// AavePremium reads the live premium basis-points value. It does not cache or
// assume a fixed premium, so the optimizer always uses the current protocol value.
type AavePremium struct {
	Caller Caller
	Pool   string
}

func (p AavePremium) Premium(ctx context.Context, amount *big.Int) (*big.Int, error) {
	if p.Caller == nil || amount == nil {
		return nil, fmt.Errorf("Aave premium caller and amount are required")
	}
	raw, err := p.Caller.EthCall(ctx, map[string]string{"to": p.Pool, "data": dex.StaticCall("FLASHLOAN_PREMIUM_TOTAL()")})
	if err != nil {
		return nil, err
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) == 0 {
		return nil, fmt.Errorf("Aave premium response: %w", err)
	}
	return new(big.Int).Div(new(big.Int).Mul(amount, dex.WordUint(words[0])), big.NewInt(10_000)), nil
}

// ChainlinkPrices uses a token-to-feed mapping supplied by configuration. There
// is intentionally no symbol switch or stablecoin assumption.
type ChainlinkPrices struct {
	Caller Caller
	Feeds  map[string]string
}

func (p ChainlinkPrices) USD(ctx context.Context, asset config.Token) (*big.Rat, error) {
	feed := p.Feeds[asset.Symbol]
	if feed == "" {
		return nil, fmt.Errorf("USD feed missing for asset %s", asset.Symbol)
	}
	decRaw, err := p.Caller.EthCall(ctx, map[string]string{"to": feed, "data": dex.StaticCall("decimals()")})
	if err != nil {
		return nil, err
	}
	decWords, err := dex.DecodeWords(decRaw)
	if err != nil || len(decWords) == 0 {
		return nil, fmt.Errorf("feed decimals: %w", err)
	}
	raw, err := p.Caller.EthCall(ctx, map[string]string{"to": feed, "data": dex.StaticCall("latestRoundData()")})
	if err != nil {
		return nil, err
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) < 5 {
		return nil, fmt.Errorf("feed latestRoundData: %w", err)
	}
	answer := dex.WordInt(words[1])
	if answer.Sign() <= 0 {
		return nil, fmt.Errorf("USD feed answer is non-positive")
	}
	return new(big.Rat).SetFrac(answer, new(big.Int).Exp(big.NewInt(10), dex.WordUint(decWords[0]), nil)), nil
}

// AssetFeeModel converts authoritative Arbitrum L1/L2 components into raw units
// of whichever loan asset the graph chooses. No ETH/token comparison is allowed.
type AssetFeeModel struct {
	Service  *fees.Service
	Prices   PriceSource
	GasLimit uint64
}

func (m AssetFeeModel) Estimate(ctx context.Context, asset config.Token, route graph.Route, hops []Hop) (*big.Int, *big.Int, error) {
	if m.Service == nil || m.Prices == nil {
		return nil, nil, fmt.Errorf("authoritative fee service and asset price source are required")
	}
	gas := m.GasLimit
	if gas == 0 {
		gas = 5_000_000
	}
	payload := make([]byte, 4+160+256*len(route.Hops))
	estimate, err := m.Service.Estimate(ctx, gas, payload)
	if err != nil {
		return nil, nil, err
	}
	price, err := m.Prices.USD(ctx, asset)
	if err != nil {
		return nil, nil, err
	}
	if estimate.TotalETH == nil || estimate.TotalETH.Sign() <= 0 || estimate.TotalUSD == nil {
		return nil, nil, fmt.Errorf("incomplete ETH fee estimate")
	}
	ethUSD := new(big.Rat).Quo(estimate.TotalUSD, estimate.TotalETH)
	l2 := ethWeiToAssetRaw(estimate.L2Cost, ethUSD, price, asset.Decimals)
	l1 := ethWeiToAssetRaw(estimate.L1Cost, ethUSD, price, asset.Decimals)
	// Preserve the configured fee safety margin by charging it to the L2 side;
	// Alpha.Evaluate subtracts both returned components before ranking.
	l2.Add(l2, ethWeiToAssetRaw(estimate.SafetyMarginWei, ethUSD, price, asset.Decimals))
	return l2, l1, nil
}
func ethWeiToAssetRaw(wei *big.Int, ethUSD, assetUSD *big.Rat, decimals uint8) *big.Int {
	if wei == nil || wei.Sign() <= 0 || ethUSD == nil || assetUSD == nil || assetUSD.Sign() <= 0 {
		return big.NewInt(0)
	}
	v := new(big.Rat).SetFrac(wei, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	v.Mul(v, ethUSD)
	v.Quo(v, assetUSD)
	v.Mul(v, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)))
	return new(big.Int).Quo(v.Num(), v.Denom())
}
