package monitor

import (
	"context"
	"testing"
)

func TestSystemSample(t *testing.T) {
	sample, err := New(t.TempDir()).Sample(context.Background())
	if err != nil {
		t.Skipf("host metrics unavailable: %v", err)
	}
	if sample.ProcessRSS == 0 {
		t.Fatal("process RSS not captured")
	}
}
