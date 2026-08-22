package universe

import (
	"sort"
)

// Policy bounds dynamic market-universe expansion. It controls read-only graph
// membership only; execution allow-lists and contracts remain separate.
type Policy struct {
	MaxDynamicAssets     int
	MinScore             float64
	MinQuoteSuccessRatio float64
	MinGraphDepth        int
	RequireCrossVenue    bool
	FailureEjectAfter    uint64
}

func DefaultPolicy() Policy {
	return Policy{
		MaxDynamicAssets:     4,
		MinScore:             80,
		MinQuoteSuccessRatio: 0.50,
		MinGraphDepth:        2,
		RequireCrossVenue:    true,
		FailureEjectAfter:    12,
	}
}

type Feedback struct {
	Asset       string
	Evaluations uint64
	Useful      uint64
}

type CandidateDecision struct {
	Symbol string  `json:"symbol"`
	Score  float64 `json:"score"`
	Action string  `json:"action"`
	Reason string  `json:"reason"`
}

type Decision struct {
	ActiveAssets  []string            `json:"active_assets"`
	AddedAssets   []string            `json:"added_assets"`
	RemovedAssets []string            `json:"removed_assets"`
	Candidates    []CandidateDecision `json:"candidates"`
}

type Manager struct {
	core     map[string]struct{}
	active   map[string]struct{}
	failures map[string]uint64
	policy   Policy
}

func NewManager(coreAssets []string, policy Policy) *Manager {
	if policy.MaxDynamicAssets < 1 {
		policy = DefaultPolicy()
	}
	m := &Manager{
		core:     make(map[string]struct{}, len(coreAssets)),
		active:   make(map[string]struct{}, len(coreAssets)),
		failures: make(map[string]uint64),
		policy:   policy,
	}
	for _, asset := range coreAssets {
		if asset == "" {
			continue
		}
		m.core[asset] = struct{}{}
		m.active[asset] = struct{}{}
	}
	return m
}

func (m *Manager) ApplyFeedback(feedback Feedback) {
	if m == nil || feedback.Asset == "" {
		return
	}
	if feedback.Evaluations > 0 && feedback.Useful == 0 {
		m.failures[feedback.Asset] += feedback.Evaluations
		return
	}
	if feedback.Useful > 0 {
		m.failures[feedback.Asset] = 0
	}
}

func (m *Manager) Select(report Report) Decision {
	if m == nil {
		return Decision{}
	}
	decision := Decision{}
	for asset := range m.active {
		if _, core := m.core[asset]; core {
			continue
		}
		if m.policy.FailureEjectAfter > 0 && m.failures[asset] >= m.policy.FailureEjectAfter {
			delete(m.active, asset)
			decision.RemovedAssets = append(decision.RemovedAssets, asset)
		}
	}

	candidates := append([]Candidate(nil), report.Candidates...)
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		return candidates[i].Symbol < candidates[j].Symbol
	})

	dynamicCount := 0
	for asset := range m.active {
		if _, core := m.core[asset]; !core {
			dynamicCount++
		}
	}

	for _, candidate := range candidates {
		reason := m.rejectionReason(candidate)
		if reason != "" {
			decision.Candidates = append(decision.Candidates, CandidateDecision{Symbol: candidate.Symbol, Score: candidate.Score, Action: "rejected", Reason: reason})
			continue
		}
		if dynamicCount >= m.policy.MaxDynamicAssets {
			decision.Candidates = append(decision.Candidates, CandidateDecision{Symbol: candidate.Symbol, Score: candidate.Score, Action: "deferred", Reason: "dynamic universe cap reached"})
			continue
		}
		name := candidate.Name
		if name == "" {
			name = candidate.Symbol
		}
		if _, ok := m.active[name]; !ok {
			m.active[name] = struct{}{}
			decision.AddedAssets = append(decision.AddedAssets, name)
			dynamicCount++
		}
		decision.Candidates = append(decision.Candidates, CandidateDecision{Symbol: candidate.Symbol, Score: candidate.Score, Action: "approved", Reason: "candidate passed objective market filters"})
	}

	for _, asset := range sortedSet(m.active) {
		decision.ActiveAssets = append(decision.ActiveAssets, asset)
	}
	sort.Strings(decision.AddedAssets)
	sort.Strings(decision.RemovedAssets)
	return decision
}

func (m *Manager) rejectionReason(candidate Candidate) string {
	name := candidate.Name
	if name == "" {
		name = candidate.Symbol
	}
	if _, core := m.core[name]; core {
		return "core execution asset"
	}
	if candidate.Score < m.policy.MinScore {
		return "score below minimum"
	}
	if candidate.ConnectedGraphDepth < m.policy.MinGraphDepth {
		return "insufficient graph connectivity"
	}
	if m.policy.RequireCrossVenue && (candidate.UniswapPools == 0 || candidate.CamelotPools == 0) {
		return "insufficient cross-venue presence"
	}
	if candidate.QuoteAttempts > 0 {
		ratio := float64(candidate.QuoteSuccesses) / float64(candidate.QuoteAttempts)
		if ratio < m.policy.MinQuoteSuccessRatio {
			return "quote success ratio below minimum"
		}
	}
	return ""
}

func sortedSet(input map[string]struct{}) []string {
	out := make([]string, 0, len(input))
	for value := range input {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
