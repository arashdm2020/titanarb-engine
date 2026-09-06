package decision

import (
	"math/big"
	"testing"
	"time"
)

var testUSD = Asset{"0x0000000000000000000000000000000000000001", 6}
var testETH = Asset{"0x0000000000000000000000000000000000000002", 18}

func bi(s string) *big.Int { x, _ := new(big.Int).SetString(s, 10); return x }
func portfolioFixture() PortfolioInput {
	now := time.Unix(10000, 0)
	return PortfolioInput{Funding: FundingState{Borrowed: []Balance{{testUSD, bi("1000000000")}}}, Liabilities: []Liability{{testUSD, bi("1000000000"), bi("500000")}}, After: []Balance{{testUSD, bi("1000500000")}, {testETH, bi("1000000000000000")}}, Valuation: ValuationState{Numeraire: "USD", Now: now, MaxAge: time.Minute, ExposureLimits: map[string]*big.Int{testETH.Address: bi("1000000000000000000")}, Prices: map[string]Price{testETH.Address: {USD: big.NewRat(2000, 1), AsOf: now, MaxRaw: bi("1000000000000000000"), Independent: true, Executable: true, ReviewedToken: true}}}}
}

func TestResidualExposureIsExplicit(t *testing.T) {
	in := portfolioFixture()
	in.Valuation.ExposureLimits = nil
	if EvaluatePortfolio(in).Positive {
		t.Fatal("missing exposure limit")
	}
	in = portfolioFixture()
	in.Valuation.ExposureLimits[testETH.Address] = bi("1")
	if EvaluatePortfolio(in).Positive {
		t.Fatal("exceeded exposure limit")
	}
}
func TestResidualProfitIsIndependentOfFunding(t *testing.T) {
	in := portfolioFixture()
	r := EvaluatePortfolio(in)
	if !r.LiabilitiesSatisfied || !r.ValuationComplete || r.NetUSD != "2" || !r.Positive || r.ExecutionAuthority {
		t.Fatalf("%+v", r)
	}
	if in.After[0].Raw.String() != "1000500000" {
		t.Fatal("input mutated")
	}
}
func TestValuableResidualCannotRepayMissingLiability(t *testing.T) {
	in := portfolioFixture()
	in.After[0].Raw = bi("1000000000")
	r := EvaluatePortfolio(in)
	if r.LiabilitiesSatisfied || r.Positive {
		t.Fatalf("unpaid liability accepted: %+v", r)
	}
}
func TestWeakResidualValuationRejected(t *testing.T) {
	for _, kind := range []string{"stale", "future", "self", "illiquid", "unreviewed", "negative", "haircut"} {
		t.Run(kind, func(t *testing.T) {
			in := portfolioFixture()
			p := in.Valuation.Prices[testETH.Address]
			switch kind {
			case "stale":
				p.AsOf = p.AsOf.Add(-2 * time.Minute)
			case "future":
				p.AsOf = p.AsOf.Add(time.Second)
			case "self":
				p.Independent = false
			case "illiquid":
				p.MaxRaw = bi("1")
			case "unreviewed":
				p.ReviewedToken = false
			case "negative":
				p.USD = big.NewRat(-1, 1)
			case "haircut":
				p.HaircutBPS = 10001
			}
			in.Valuation.Prices[testETH.Address] = p
			r := EvaluatePortfolio(in)
			if r.ValuationComplete || r.Positive {
				t.Fatalf("%+v", r)
			}
		})
	}
}
func TestCostsPrincipalAndOwnedEquityCountedOnce(t *testing.T) {
	in := portfolioFixture()
	in.Costs = []Balance{{testETH, bi("100000000000000")}}
	in.Funding.OwnedBefore = []Balance{{testETH, bi("200000000000000")}}
	r := EvaluatePortfolio(in)
	if r.NetUSD != "7/5" {
		t.Fatalf("want $1.4, got %+v", r)
	}
}
func TestFundingMismatchAndDuplicateDebt(t *testing.T) {
	in := portfolioFixture()
	in.Liabilities = append(in.Liabilities, in.Liabilities[0])
	if EvaluatePortfolio(in).LiabilitiesSatisfied {
		t.Fatal("duplicate debt ignored")
	}
	in = portfolioFixture()
	in.Funding.Borrowed[0].Raw = bi("1")
	if EvaluatePortfolio(in).LiabilitiesSatisfied {
		t.Fatal("mismatched funding ignored")
	}
}
func TestConflictingDecimalsAndUnfundedCost(t *testing.T) {
	in := portfolioFixture()
	a := testUSD
	a.Decimals = 18
	in.After = append(in.After, Balance{a, bi("1")})
	if EvaluatePortfolio(in).ValuationComplete {
		t.Fatal("decimals")
	}
	in = portfolioFixture()
	in.Costs = []Balance{{testETH, bi("999999999999999999")}}
	if EvaluatePortfolio(in).Positive {
		t.Fatal("unfunded gas counted as profit")
	}
}
func TestLadderBoundedUniqueAndNonMutating(t *testing.T) {
	for _, b := range []string{"1", "6", "1000000000"} {
		base := bi(b)
		ladder := AmountLadder(base, bi("400"))
		seen := map[string]bool{}
		for _, a := range ladder {
			if a.Sign() <= 0 || a.Cmp(base) > 0 || a.Cmp(bi("400")) > 0 || seen[a.String()] {
				t.Fatal(ladder)
			}
			seen[a.String()] = true
		}
		if len(ladder) > 5 || base.String() != b {
			t.Fatal("bounds or mutation")
		}
	}
}
