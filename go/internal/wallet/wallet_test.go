package wallet

import (
	"context"
	"testing"
)

type nonceSource struct{ pending uint64 }

func (n nonceSource) TransactionCount(context.Context, string, string) (uint64, error) {
	return n.pending, nil
}
func TestPendingNonceReservationsAreSequential(t *testing.T) {
	w, err := New("0123456789012345678901234567890123456789012345678901234567890123", nonceSource{pending: 7})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := w.NextNonce(context.Background())
	b, _ := w.NextNonce(context.Background())
	if a != 7 || b != 8 {
		t.Fatalf("nonces %d,%d", a, b)
	}
	w.Release(8)
	c, _ := w.NextNonce(context.Background())
	if c != 8 {
		t.Fatalf("released nonce %d", c)
	}
}
