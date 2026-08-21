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

// Build cycles from the configured base asset through unique configured
// intermediates. It is intentionally bounded to 2--4 hops and maxRoutes.
func Build(base string, intermediates []string, byPair map[Pair][]pools.Pool, maxRoutes int) []Route {
	if maxRoutes < 1 {
		return nil
	}
	var output []Route
	for hops := 2; hops <= 4; hops++ {
		for _, path := range permutations(intermediates, hops-1) {
			symbols := append([]string{base}, path...)
			symbols = append(symbols, base)
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
			appendCombinations(symbols, choices, 0, nil, &output, maxRoutes)
			if len(output) >= maxRoutes {
				return output
			}
		}
	}
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
