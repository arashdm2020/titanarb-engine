package universe

import "testing"

func TestManagerApprovesOnlyScoredCrossVenueCandidates(t *testing.T) {
	manager := NewManager([]string{"USDC", "WETH"}, DefaultPolicy())
	decision := manager.Select(Report{Candidates: []Candidate{
		{Name: "TOKEN_A", Symbol: "TOKENA", Score: 95, UniswapPools: 2, CamelotPools: 1, QuoteAttempts: 10, QuoteSuccesses: 10, ConnectedGraphDepth: 3},
		{Name: "TOKEN_B", Symbol: "TOKENB", Score: 99, UniswapPools: 3, CamelotPools: 0, QuoteAttempts: 10, QuoteSuccesses: 10, ConnectedGraphDepth: 3},
	}})
	if len(decision.AddedAssets) != 1 || decision.AddedAssets[0] != "TOKEN_A" {
		t.Fatalf("unexpected approvals: %#v", decision)
	}
	for _, item := range decision.Candidates {
		if item.Symbol == "TOKENB" && item.Action != "rejected" {
			t.Fatalf("single-venue token should not be approved: %#v", item)
		}
	}
}

func TestManagerEjectsLowValueDynamicCandidateAfterFeedback(t *testing.T) {
	policy := DefaultPolicy()
	policy.FailureEjectAfter = 2
	manager := NewManager([]string{"USDC"}, policy)
	report := Report{Candidates: []Candidate{
		{Name: "USDC_E_BRIDGED_ALTERNATIVE", Symbol: "USDC.e", Score: 90, UniswapPools: 1, CamelotPools: 1, QuoteAttempts: 4, QuoteSuccesses: 4, ConnectedGraphDepth: 2},
	}}
	first := manager.Select(report)
	if len(first.AddedAssets) != 1 {
		t.Fatalf("candidate not added: %#v", first)
	}
	manager.ApplyFeedback(Feedback{Asset: "USDC_E_BRIDGED_ALTERNATIVE", Evaluations: 2, Useful: 0})
	second := manager.Select(Report{})
	if len(second.RemovedAssets) != 1 || second.RemovedAssets[0] != "USDC_E_BRIDGED_ALTERNATIVE" {
		t.Fatalf("candidate not removed after bounded low-value feedback: %#v", second)
	}
}
