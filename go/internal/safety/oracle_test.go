package safety

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"
	"time"
)

type fake struct {
	responses []string
	n         int
}

func (f *fake) EthCall(context.Context, map[string]string) (string, error) {
	v := f.responses[f.n]
	f.n++
	return v, nil
}
func word(v int64) string {
	b := make([]byte, 32)
	big.NewInt(v).FillBytes(b)
	return hex.EncodeToString(b)
}
func response(answer, started, updated int64) string {
	return "0x" + word(1) + word(answer) + word(started) + word(updated) + word(1)
}
func TestGateRejectsDownSequencer(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f := &fake{responses: []string{response(3000_00000000, now.Add(-time.Minute).Unix(), now.Unix()), response(1, now.Add(-2*time.Hour).Unix(), now.Unix())}}
	err := New(f, "0x1", "0x2", time.Hour, time.Hour).Check(context.Background(), now)
	if err == nil {
		t.Fatal("expected sequencer rejection")
	}
}
