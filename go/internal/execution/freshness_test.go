package execution

import (
	"math"
	"testing"
)

func TestCandidateFreshness(t *testing.T) {
	for _, test := range []struct {
		name                    string
		source, latest, maximum uint64
		want                    bool
	}{
		{name: "exact block with zero tolerance", source: 100, latest: 100, maximum: 0, want: true},
		{name: "one block rejected with zero tolerance", source: 100, latest: 101, maximum: 0, want: false},
		{name: "one block admitted", source: 100, latest: 101, maximum: 1, want: true},
		{name: "two blocks rejected by one block tolerance", source: 100, latest: 102, maximum: 1, want: false},
		{name: "two blocks admitted by default", source: 100, latest: 102, maximum: 2, want: true},
		{name: "three blocks rejected by default", source: 100, latest: 103, maximum: 2, want: false},
		{name: "older observed head is safe", source: 101, latest: 100, maximum: 0, want: true},
		{name: "fixture without source block", source: 0, latest: math.MaxUint64, maximum: 0, want: true},
		{name: "near uint64 maximum is overflow safe", source: math.MaxUint64 - 1, latest: math.MaxUint64, maximum: 2, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := candidateIsFresh(test.source, test.latest, test.maximum); got != test.want {
				t.Fatalf("candidateIsFresh(%d,%d,%d)=%t want %t", test.source, test.latest, test.maximum, got, test.want)
			}
		})
	}
}

func TestCandidateBlockLagIsUnderflowSafe(t *testing.T) {
	for _, test := range []struct{ source, latest, want uint64 }{
		{source: 100, latest: 99, want: 0},
		{source: 100, latest: 100, want: 0},
		{source: 100, latest: 103, want: 3},
		{source: math.MaxUint64 - 1, latest: math.MaxUint64, want: 1},
	} {
		if got := candidateBlockLag(test.source, test.latest); got != test.want {
			t.Fatalf("candidateBlockLag(%d,%d)=%d want %d", test.source, test.latest, got, test.want)
		}
	}
}
