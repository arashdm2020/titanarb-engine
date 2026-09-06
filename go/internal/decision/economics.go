// Package decision is an analysis-only consumer of market data. It has no
// transaction, signer, execution pipeline or opportunity-event dependency.
package decision

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type Asset struct {
	Address  string `json:"address"`
	Decimals uint8  `json:"decimals"`
}
type Balance struct {
	Asset Asset
	Raw   *big.Int
}
type FundingState struct {
	Borrowed    []Balance
	OwnedBefore []Balance
}
type Liability struct {
	Asset              Asset
	Principal, Premium *big.Int
}
type ResidualInventory []Balance

// Price is an independent, executable liquidation bound in USD per whole token,
// not a spot price from the opportunity's own pools. MaxRaw bounds liquidity.
type Price struct {
	USD                                    *big.Rat
	AsOf                                   time.Time
	MaxRaw                                 *big.Int
	Independent, Executable, ReviewedToken bool
	HaircutBPS                             uint64
}
type ValuationState struct {
	Numeraire      string
	Prices         map[string]Price
	ExposureLimits map[string]*big.Int // Explicit residual caps in raw asset units.
	Now            time.Time
	MaxAge         time.Duration
}
type PortfolioInput struct {
	Funding     FundingState
	Liabilities []Liability
	After       []Balance // Before repayment, AFTER all modeled swaps.
	Costs       []Balance // Unpaid gas/L1/other costs; never include fees already in swap outputs.
	Valuation   ValuationState
}
type PortfolioResult struct {
	LiabilitiesSatisfied bool              `json:"liabilities_satisfied"`
	ValuationComplete    bool              `json:"valuation_complete"`
	Residual             map[string]string `json:"residual_raw"`
	NetUSD               string            `json:"net_usd"`
	Positive             bool              `json:"positive"`
	Reason               string            `json:"reason"`
	ExecutionAuthority   bool              `json:"execution_authority"` // Always false.
}

func address(a Asset) (string, error) {
	if !common.IsHexAddress(a.Address) || common.HexToAddress(a.Address) == (common.Address{}) || a.Decimals > 36 {
		return "", fmt.Errorf("invalid canonical asset")
	}
	return strings.ToLower(a.Address), nil
}
func add(m map[string]*big.Int, meta map[string]Asset, b Balance) error {
	k, err := address(b.Asset)
	if err != nil || b.Raw == nil || b.Raw.Sign() < 0 {
		return fmt.Errorf("invalid balance")
	}
	if old, ok := meta[k]; ok && old.Decimals != b.Asset.Decimals {
		return fmt.Errorf("conflicting decimals")
	}
	meta[k] = b.Asset
	if m[k] == nil {
		m[k] = new(big.Int)
	}
	m[k].Add(m[k], b.Raw)
	return nil
}

// EvaluatePortfolio settles each liability in its actual asset BEFORE valuation.
// Debt proceeds are not equity; principal/premium are deducted exactly once.
// Costs need not be paid in the funding asset. No negative balance is financed
// by merely marking another asset to market.
func EvaluatePortfolio(in PortfolioInput) PortfolioResult {
	r := PortfolioResult{Residual: map[string]string{}, NetUSD: "UNKNOWN"}
	fail := func(reason string) PortfolioResult { r.Reason = reason; return r }
	meta := map[string]Asset{}
	after, owned, borrowed, principal := map[string]*big.Int{}, map[string]*big.Int{}, map[string]*big.Int{}, map[string]*big.Int{}
	for _, b := range in.After {
		if err := add(after, meta, b); err != nil {
			return fail(err.Error())
		}
	}
	for _, b := range in.Funding.OwnedBefore {
		if err := add(owned, meta, b); err != nil {
			return fail(err.Error())
		}
	}
	for _, b := range in.Funding.Borrowed {
		if err := add(borrowed, meta, b); err != nil {
			return fail(err.Error())
		}
	}
	debts := map[string]*big.Int{}
	for _, l := range in.Liabilities {
		if l.Principal == nil || l.Principal.Sign() <= 0 || l.Premium == nil || l.Premium.Sign() < 0 {
			return fail("invalid liability")
		}
		if err := add(principal, meta, Balance{l.Asset, l.Principal}); err != nil {
			return fail(err.Error())
		}
		if err := add(debts, meta, Balance{l.Asset, new(big.Int).Add(l.Principal, l.Premium)}); err != nil {
			return fail(err.Error())
		}
	}
	if len(principal) != len(borrowed) {
		return fail("funding/liability mismatch")
	}
	for k, p := range principal {
		if borrowed[k] == nil || p.Cmp(borrowed[k]) != 0 {
			return fail("funding/liability mismatch")
		}
	}
	for k, debt := range debts {
		if after[k] == nil || after[k].Cmp(debt) < 0 {
			return fail("unpaid liability: " + k)
		}
		after[k].Sub(after[k], debt)
	}
	r.LiabilitiesSatisfied = true
	for _, cost := range in.Costs {
		k, err := address(cost.Asset)
		if err != nil || cost.Raw == nil || cost.Raw.Sign() < 0 {
			return fail("invalid cost")
		}
		if old, ok := meta[k]; ok && old.Decimals != cost.Asset.Decimals {
			return fail("conflicting cost decimals")
		}
		if cost.Raw.Sign() == 0 {
			continue
		}
		if after[k] == nil || after[k].Cmp(cost.Raw) < 0 {
			return fail("unfunded cost: " + k)
		}
		after[k].Sub(after[k], cost.Raw)
	}
	for k, v := range after {
		if v.Sign() > 0 {
			r.Residual[k] = v.String()
		}
	}
	for k, v := range after {
		if v.Sign() == 0 {
			continue
		}
		cap := in.Valuation.ExposureLimits[k]
		if cap == nil || cap.Sign() < 0 || v.Cmp(cap) > 0 {
			return fail("missing/exceeded residual exposure limit: " + k)
		}
	}
	if in.Valuation.Numeraire != "USD" || in.Valuation.MaxAge <= 0 || in.Valuation.Now.IsZero() {
		return fail("missing valuation policy")
	}
	value := func(balances map[string]*big.Int) (*big.Rat, error) {
		total := new(big.Rat)
		for k, raw := range balances {
			if raw.Sign() == 0 {
				continue
			}
			p, ok := in.Valuation.Prices[k]
			age := in.Valuation.Now.Sub(p.AsOf)
			if !ok || p.USD == nil || p.USD.Sign() <= 0 || p.MaxRaw == nil || p.MaxRaw.Cmp(raw) < 0 || !p.Independent || !p.Executable || !p.ReviewedToken || age < 0 || age > in.Valuation.MaxAge || p.HaircutBPS > 10000 {
				return nil, fmt.Errorf("missing/weak/stale liquidation valuation: %s", k)
			}
			v := new(big.Rat).SetFrac(raw, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(meta[k].Decimals)), nil))
			v.Mul(v, p.USD)
			v.Mul(v, new(big.Rat).SetFrac64(int64(10000-p.HaircutBPS), 10000))
			total.Add(total, v)
		}
		return total, nil
	}
	a, err := value(after)
	if err != nil {
		return fail(err.Error())
	}
	b, err := value(owned)
	if err != nil {
		return fail(err.Error())
	}
	net := new(big.Rat).Sub(a, b)
	r.ValuationComplete = true
	r.NetUSD = net.RatString()
	r.Positive = net.Sign() > 0
	r.Reason = "analysis_only"
	return r
}

// AmountLadder never increases the production amount. Fractions are in raw
// asset units, bounded to five unique positive sizes, and are quote requests,
// not extrapolated profitability claims.
func AmountLadder(base, depthCap *big.Int) []*big.Int {
	if base == nil || base.Sign() <= 0 {
		return nil
	}
	ceiling := new(big.Int).Set(base)
	if depthCap != nil && depthCap.Sign() > 0 && depthCap.Cmp(ceiling) < 0 {
		ceiling.Set(depthCap)
	}
	out := []*big.Int{}
	seen := map[string]bool{}
	for _, d := range []int64{100, 20, 10, 4, 1} {
		x := new(big.Int).Quo(ceiling, big.NewInt(d))
		if x.Sign() > 0 && !seen[x.String()] {
			seen[x.String()] = true
			out = append(out, x)
		}
	}
	return out
}
