package execution

import "testing"

func TestCandidateFreshness(t *testing.T) {
	for _, test := range []struct {
		name           string
		source, latest uint64
		want           bool
	}{
		{name: "same pinned state", source: 100, latest: 100, want: true},
		{name: "newer state invalidates", source: 100, latest: 101, want: false},
		{name: "fixture without source block", source: 0, latest: 101, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := candidateIsFresh(test.source, test.latest); got != test.want {
				t.Fatalf("candidateIsFresh(%d,%d)=%t want %t", test.source, test.latest, got, test.want)
			}
		})
	}
}
