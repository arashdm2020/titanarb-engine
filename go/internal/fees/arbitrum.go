// Package fees provides Python-parity, read-only Arbitrum fee estimation.
package fees

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/titanarb/titanarb-go/internal/dex"
)

const NodeInterface = "0x00000000000000000000000000000000000000C8"
const feeDestination = "0x000000000000000000000000000000000000dEaD"

type Caller interface {
	EthCall(context.Context, map[string]string) (string, error)
}
type FeeEstimate struct {
	L2Cost          *big.Int
	L1Cost          *big.Int
	TotalWei        *big.Int
	TotalETH        *big.Rat
	TotalUSD        *big.Rat
	SafetyMarginWei *big.Int
	Timestamp       time.Time
}
type Service struct {
	caller                 Caller
	arbGasInfo, ethUSDFeed string
	safetyBPS              uint64
}

func New(c Caller, arbGasInfo, ethUSDFeed string, safetyBPS uint64) *Service {
	return &Service{c, arbGasInfo, ethUSDFeed, safetyBPS}
}

func (s *Service) Estimate(ctx context.Context, gasLimit uint64, calldata []byte) (FeeEstimate, error) {
	pricesRaw, err := s.caller.EthCall(ctx, map[string]string{"to": s.arbGasInfo, "data": dex.StaticCall("getPricesInWei()")})
	if err != nil {
		return FeeEstimate{}, err
	}
	prices, err := dex.DecodeWords(pricesRaw)
	if err != nil || len(prices) < 6 {
		return FeeEstimate{}, fmt.Errorf("ArbGasInfo.getPricesInWei response: %w", err)
	}
	perL2Tx := dex.WordUint(prices[0])
	perArbGas := dex.WordUint(prices[5])
	l2 := new(big.Int).Add(perL2Tx, new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), perArbGas))
	dead, err := dex.AddressWord(feeDestination)
	if err != nil {
		return FeeEstimate{}, err
	}
	nodeData := dex.DynamicBytesCall("gasEstimateComponents(address,bool,bytes)", [][]byte{dead, dex.BoolWord(false)}, calldata)
	nodeRaw, err := s.caller.EthCall(ctx, map[string]string{"to": NodeInterface, "data": nodeData})
	if err != nil {
		return FeeEstimate{}, err
	}
	node, err := dex.DecodeWords(nodeRaw)
	if err != nil || len(node) < 4 {
		return FeeEstimate{}, fmt.Errorf("NodeInterface response: %w", err)
	}
	l1 := new(big.Int).Mul(dex.WordUint(node[1]), dex.WordUint(node[2]))
	raw := new(big.Int).Add(l2, l1)
	margin := new(big.Int).Div(new(big.Int).Mul(raw, new(big.Int).SetUint64(s.safetyBPS)), big.NewInt(10_000))
	total := new(big.Int).Add(raw, margin)
	price, err := s.ethUSD(ctx)
	if err != nil {
		return FeeEstimate{}, err
	}
	eth := new(big.Rat).SetFrac(total, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	usd := new(big.Rat).Mul(eth, price)
	return FeeEstimate{L2Cost: l2, L1Cost: l1, TotalWei: total, TotalETH: eth, TotalUSD: usd, SafetyMarginWei: margin, Timestamp: time.Now().UTC()}, nil
}
func (s *Service) ethUSD(ctx context.Context) (*big.Rat, error) {
	decRaw, err := s.caller.EthCall(ctx, map[string]string{"to": s.ethUSDFeed, "data": dex.StaticCall("decimals()")})
	if err != nil {
		return nil, err
	}
	dec, err := dex.DecodeWords(decRaw)
	if err != nil || len(dec) < 1 {
		return nil, fmt.Errorf("feed decimals: %w", err)
	}
	raw, err := s.caller.EthCall(ctx, map[string]string{"to": s.ethUSDFeed, "data": dex.StaticCall("latestRoundData()")})
	if err != nil {
		return nil, err
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) < 5 {
		return nil, fmt.Errorf("latestRoundData: %w", err)
	}
	answer := dex.WordInt(words[1])
	if answer.Sign() <= 0 {
		return nil, fmt.Errorf("ETH/USD answer is non-positive")
	}
	denom := new(big.Int).Exp(big.NewInt(10), dex.WordUint(dec[0]), nil)
	return new(big.Rat).SetFrac(answer, denom), nil
}
