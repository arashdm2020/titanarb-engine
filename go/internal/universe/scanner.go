package universe

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
)

type PairDiscoverer interface {
	DiscoverPair(context.Context, string, string) ([]pools.Pool, error)
}

type ChainCaller interface {
	EthCall(context.Context, map[string]string) (string, error)
}

type QuoteRouter func(pools.Pool) quotes.Quoter

type Scanner struct {
	Market     config.MarketConfig
	Discoverer PairDiscoverer
	Caller     ChainCaller
	Quoter     QuoteRouter
	AmountRaw  map[string]*big.Int
}

type Candidate struct {
	Symbol              string         `json:"symbol"`
	Address             string         `json:"address"`
	Decimals            uint8          `json:"decimals"`
	Score               float64        `json:"score"`
	UniswapPools        int            `json:"uniswap_pools"`
	CamelotPools        int            `json:"camelot_pools"`
	AaveReserve         bool           `json:"aave_reserve"`
	LiquidityScore      string         `json:"liquidity_score"`
	QuoteSuccesses      int            `json:"quote_successes"`
	QuoteAttempts       int            `json:"quote_attempts"`
	ConnectedGraphDepth int            `json:"connected_graph_depth"`
	ConnectedAssets     []string       `json:"connected_assets"`
	DEXPresence         map[string]int `json:"dex_presence"`
	Reasons             []string       `json:"reasons,omitempty"`
}

type Report struct {
	ExecutionAssets []string    `json:"execution_assets"`
	ScannedAssets   int         `json:"scanned_assets"`
	Candidates      []Candidate `json:"candidates"`
}

func (s Scanner) Scan(ctx context.Context) (Report, error) {
	if s.Discoverer == nil {
		return Report{}, fmt.Errorf("discoverer is required")
	}
	executionAssets := s.Market.ExecutionAssets()
	executionSet := make(map[string]struct{}, len(executionAssets))
	for _, symbol := range executionAssets {
		executionSet[symbol] = struct{}{}
	}
	var candidates []Candidate
	for _, symbol := range s.Market.DiscoveryAssets() {
		if _, execution := executionSet[symbol]; execution {
			continue
		}
		token := s.Market.Tokens[symbol]
		candidate := Candidate{
			Symbol:      token.Symbol,
			Address:     token.Address,
			Decimals:    token.Decimals,
			DEXPresence: make(map[string]int),
		}
		connected := make(map[string]struct{})
		var liquidity = new(big.Int)
		for _, base := range executionAssets {
			baseToken := s.Market.Tokens[base]
			found, err := s.Discoverer.DiscoverPair(ctx, token.Address, baseToken.Address)
			if err != nil {
				candidate.Reasons = append(candidate.Reasons, "discovery_error:"+base)
				continue
			}
			if len(found) > 0 {
				connected[base] = struct{}{}
			}
			for _, pool := range found {
				if pool.Liquidity != nil {
					liquidity.Add(liquidity, pool.Liquidity)
				}
				switch pool.DEX {
				case pools.UniswapV3:
					candidate.UniswapPools++
					candidate.DEXPresence[string(pools.UniswapV3)]++
				case pools.CamelotV3:
					candidate.CamelotPools++
					candidate.DEXPresence[string(pools.CamelotV3)]++
				}
				if s.Quoter != nil {
					candidate.QuoteAttempts++
					quoter := s.Quoter(pool)
					if quoter != nil {
						amount := s.amountFor(token)
						if amount.Sign() > 0 {
							if _, err := quoter.Quote(ctx, quotes.Request{TokenIn: token.Address, TokenOut: baseToken.Address, AmountIn: amount, Pool: pool}); err == nil {
								candidate.QuoteSuccesses++
							}
						}
					}
				}
			}
		}
		candidate.AaveReserve = s.aaveReserveAvailable(ctx, token.Address)
		candidate.ConnectedAssets = sortedKeys(connected)
		candidate.ConnectedGraphDepth = len(candidate.ConnectedAssets)
		candidate.LiquidityScore = liquidity.String()
		candidate.Score = scoreCandidate(candidate, liquidity)
		candidates = append(candidates, candidate)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		return candidates[i].Symbol < candidates[j].Symbol
	})
	return Report{ExecutionAssets: executionAssets, ScannedAssets: len(candidates), Candidates: candidates}, nil
}

func (s Scanner) amountFor(token config.Token) *big.Int {
	if s.AmountRaw != nil {
		if amount := s.AmountRaw[token.Symbol]; amount != nil && amount.Sign() > 0 {
			return new(big.Int).Set(amount)
		}
	}
	decimals := int(token.Decimals)
	if decimals < 0 {
		decimals = 0
	}
	if decimals > 18 {
		decimals = 18
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
}

func (s Scanner) aaveReserveAvailable(ctx context.Context, token string) bool {
	if s.Caller == nil || strings.TrimSpace(s.Market.AavePool) == "" {
		return false
	}
	word, err := dex.AddressWord(token)
	if err != nil {
		return false
	}
	raw, err := s.Caller.EthCall(ctx, map[string]string{"to": s.Market.AavePool, "data": dex.StaticCall("getReserveData(address)", word)})
	if err != nil {
		return false
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) < 9 {
		return false
	}
	return !isZeroAddress(dex.WordAddress(words[8]))
}

func scoreCandidate(c Candidate, liquidity *big.Int) float64 {
	score := 0.0
	if c.UniswapPools > 0 {
		score += 25
	}
	if c.CamelotPools > 0 {
		score += 25
	}
	if c.AaveReserve {
		score += 25
	}
	score += float64(c.ConnectedGraphDepth) * 5
	if c.QuoteAttempts > 0 {
		score += 15 * float64(c.QuoteSuccesses) / float64(c.QuoteAttempts)
	}
	if liquidity != nil && liquidity.Sign() > 0 {
		score += math.Min(10, math.Log10(float64(len(liquidity.String()))+1)*3)
	}
	return score
}

func sortedKeys(input map[string]struct{}) []string {
	output := make([]string, 0, len(input))
	for value := range input {
		output = append(output, value)
	}
	sort.Strings(output)
	return output
}

func isZeroAddress(address string) bool {
	trimmed := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(address)), "0x")
	return trimmed == "" || trimmed == strings.Repeat("0", 40)
}
