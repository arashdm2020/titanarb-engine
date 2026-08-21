// Package wallet provides isolated key handling, deterministic address
// derivation, and thread-safe pending-nonce reservation.
package wallet

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type PendingNonceSource interface {
	TransactionCount(context.Context, string, string) (uint64, error)
}

type Wallet struct {
	key     *ecdsa.PrivateKey
	address common.Address
	nonces  *NonceManager
}

func New(privateKey string, source PendingNonceSource) (*Wallet, error) {
	raw := strings.TrimPrefix(strings.TrimSpace(privateKey), "0x")
	if raw == "" {
		return nil, fmt.Errorf("private key is required")
	}
	key, err := crypto.HexToECDSA(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}
	return &Wallet{key: key, address: crypto.PubkeyToAddress(key.PublicKey), nonces: &NonceManager{source: source}}, nil
}

func (w *Wallet) Address() common.Address { return w.address }
func (w *Wallet) NextNonce(ctx context.Context) (uint64, error) {
	return w.nonces.Next(ctx, w.address.Hex())
}
func (w *Wallet) Release(nonce uint64) { w.nonces.Release(nonce) }

func (w *Wallet) Sign(chainID *big.Int, tx *types.Transaction) (*types.Transaction, error) {
	return types.SignTx(tx, types.LatestSignerForChainID(chainID), w.key)
}

type NonceManager struct {
	source PendingNonceSource
	mu     sync.Mutex
	next   *uint64
}

func (m *NonceManager) Next(ctx context.Context, address string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending, err := m.source.TransactionCount(ctx, address, "pending")
	if err != nil {
		return 0, err
	}
	if m.next == nil || pending > *m.next {
		m.next = &pending
	}
	n := *m.next
	next := n + 1
	m.next = &next
	return n, nil
}
func (m *NonceManager) Release(nonce uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.next != nil && *m.next == nonce+1 {
		m.next = &nonce
	}
}
