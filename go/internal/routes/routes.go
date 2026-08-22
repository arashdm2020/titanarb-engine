// Package routes performs bounded 2--4-hop cycle enumeration, not graph search.
package routes

import (
	"sort"

	"github.com/titanarb/titanarb-go/internal/pools"
)

type Pair struct{ From, To string }
type Route struct {
	Symbols []string
	Hops    []pools.Pool
}

// Build cycles from a legacy configured base asset. New runtime code should
// use BuildAll, which gives every asset equal start/end treatment.
func Build(base string, intermediates []string, byPair map[Pair][]pools.Pool, maxRoutes int) []Route {
	return buildFrom(base, uniqueSorted(intermediates), byPair, maxRoutes)
}

// BuildAll enumerates 2--4-hop asset-continuous cycles for every supplied
// asset. A rotated route is intentionally retained: its first asset is the
// flash-loan asset and therefore creates a materially different execution.
// Token ordering is normalized only for deterministic output, never ranking.
func BuildAll(assets []string, byPair map[Pair][]pools.Pool, maxRoutes int) []Route {
	if maxRoutes < 1 {
		return nil
	}
	assets = uniqueSorted(assets)
	if len(assets) < 2 {
		return nil
	}
	// A global first-come cap would give alphabetically earlier symbols more
	// routes. Reserve an equal bounded share for every possible loan asset;
	// economic ranking happens after quotes, never during enumeration.
	perAsset := (maxRoutes + len(assets) - 1) / len(assets)
	var output []Route
	for _, start := range assets {
		intermediates := make([]string, 0, len(assets)-1)
		for _, asset := range assets {
			if asset != start {
				intermediates = append(intermediates, asset)
			}
		}
		remaining := maxRoutes - len(output)
		limit := perAsset
		if limit > remaining {
			limit = remaining
		}
		output = append(output, buildFrom(start, intermediates, byPair, limit)...)
		if len(output) >= maxRoutes {
			return output
		}
	}
	return output
}

func buildFrom(start string, intermediates []string, byPair map[Pair][]pools.Pool, maximum int) []Route {
	if maximum < 1 {
		return nil
	}

	// Reserve bounded capacity across hop depths so short cycles do not
	// consume the entire route budget before longer cycles are considered.
	perHopLimit := maximum / 3
	if perHopLimit < 1 {
		perHopLimit = 1
	}

	var output []Route
	for hops := 2; hops <= 4; hops++ {
		for _, path := range permutations(intermediates, hops-1) {
			symbols := append([]string{start}, path...)
			symbols = append(symbols, start)
			choices := make([][]pools.Pool, 0, hops)
			valid := true
			for i := 0; i < len(symbols)-1; i++ {
				available := byPair[Pair{symbols[i], symbols[i+1]}]
				if len(available) == 0 {
					valid = false
					break
				}
				choices = append(choices, available)
			}
			if !valid {
				continue
			}
			before := len(output)
			appendCombinations(symbols, choices, 0, nil, &output, before+perHopLimit)
			if len(output) >= maximum {
				return output
			}
		}
	}
	return output
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{})
	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	output := make([]string, 0, len(seen))
	for value := range seen {
		output = append(output, value)
	}
	sort.Strings(output)
	return output
}

func appendCombinations(symbols []string, choices [][]pools.Pool, index int, current []pools.Pool, out *[]Route, maximum int) {
	if len(*out) >= maximum {
		return
	}
	if index == len(choices) {
		copyHops := append([]pools.Pool(nil), current...)
		*out = append(*out, Route{Symbols: append([]string(nil), symbols...), Hops: copyHops})
		return
	}
	for _, p := range choices[index] {
		appendCombinations(symbols, choices, index+1, append(current, p), out, maximum)
		if len(*out) >= maximum {
			return
		}
	}
}

func permutations(values []string, n int) [][]string {
	values = append([]string(nil), values...)
	sort.Strings(values)
	var result [][]string
	var walk func([]string, []string)
	walk = func(path, remaining []string) {
		if len(path) == n {
			result = append(result, append([]string(nil), path...))
			return
		}
		for i, value := range remaining {
			next := append([]string(nil), remaining[:i]...)
			next = append(next, remaining[i+1:]...)
			walk(append(path, value), next)
		}
	}
	walk(nil, values)
	return result
}

func (r Route) String() string {
	if len(r.Symbols) == 0 {
		return ""
	}
	result := r.Symbols[0]
	for _, symbol := range r.Symbols[1:] {
		result += " -> " + symbol
	}
	return result
}
