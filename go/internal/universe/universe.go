// Package universe manages the configured, execution-safe token and pool universe.
//
// It intentionally has no preferred asset: every registered token is an equal graph
// node.  A production executor still needs its own on-chain allow-list; this package
// never turns an arbitrary discovered token into an executable asset by itself.
package universe

import (
	"sort"
	"strings"
	"sync"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/pools"
)

type Registry struct {
	mu        sync.RWMutex
	tokens    map[string]config.Token
	byAddress map[string]string
	pools     map[string]pools.Pool
}

func New(tokens map[string]config.Token) *Registry {
	r := &Registry{tokens: make(map[string]config.Token), byAddress: make(map[string]string), pools: make(map[string]pools.Pool)}
	for name, token := range tokens {
		r.AddToken(name, token)
	}
	return r
}

func (r *Registry) AddToken(name string, token config.Token) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tokens[name] = token
	r.byAddress[strings.ToLower(token.Address)] = name
}

// AddPool accepts a discovered pool only when both endpoints are registered. This
// keeps discovery dynamic while preserving the deployed executor's token boundary.
func (r *Registry) AddPool(pool pools.Pool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byAddress[strings.ToLower(pool.Token0)]; !ok {
		return false
	}
	if _, ok := r.byAddress[strings.ToLower(pool.Token1)]; !ok {
		return false
	}
	r.pools[strings.ToLower(pool.Address)] = pool
	return true
}

func (r *Registry) Token(name string) (config.Token, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tokens[name]
	return t, ok
}

func (r *Registry) SymbolForAddress(address string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	name, ok := r.byAddress[strings.ToLower(address)]
	return name, ok
}

func (r *Registry) Tokens() map[string]config.Token {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copy := make(map[string]config.Token, len(r.tokens))
	for name, token := range r.tokens {
		copy[name] = token
	}
	return copy
}

func (r *Registry) TokenNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tokens))
	for name := range r.tokens {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) Pools() []pools.Pool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]pools.Pool, 0, len(r.pools))
	for _, pool := range r.pools {
		result = append(result, pool)
	}
	sort.Slice(result, func(i, j int) bool { return strings.ToLower(result[i].Address) < strings.ToLower(result[j].Address) })
	return result
}
