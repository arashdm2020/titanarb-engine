// Package safety implements the same pre-execution oracle and sequencer gates
// that protect the legacy runtime. It is read-only and fails closed.
package safety

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/titanarb/titanarb-go/internal/dex"
)

type Caller interface {
	EthCall(context.Context, map[string]string) (string, error)
}
type Gate struct {
	caller                      Caller
	ethUSD, sequencer           string
	maxStaleness, recoveryGrace time.Duration
}

func New(c Caller, ethUSD, sequencer string, maxStaleness, recoveryGrace time.Duration) *Gate {
	return &Gate{caller: c, ethUSD: ethUSD, sequencer: sequencer, maxStaleness: maxStaleness, recoveryGrace: recoveryGrace}
}

// Check validates a positive, fresh ETH/USD answer and a healthy sequencer
// whose recovery grace period has elapsed. Any malformed response rejects.
func (g *Gate) Check(ctx context.Context, now time.Time) error {
	if g == nil {
		return fmt.Errorf("oracle gate missing")
	}
	feed, err := g.latest(ctx, g.ethUSD)
	if err != nil {
		return fmt.Errorf("ETH/USD: %w", err)
	}
	if feed.answer.Sign() <= 0 {
		return fmt.Errorf("ETH/USD answer is non-positive")
	}
	if g.maxStaleness > 0 && now.Sub(time.Unix(feed.updatedAt, 0)) > g.maxStaleness {
		return fmt.Errorf("ETH/USD price is stale")
	}
	seq, err := g.latest(ctx, g.sequencer)
	if err != nil {
		return fmt.Errorf("sequencer: %w", err)
	}
	if seq.answer.Sign() != 0 {
		return fmt.Errorf("sequencer is down")
	}
	if g.recoveryGrace > 0 && now.Sub(time.Unix(seq.startedAt, 0)) < g.recoveryGrace {
		return fmt.Errorf("sequencer recovery grace period active")
	}
	return nil
}

type round struct {
	answer               *big.Int
	startedAt, updatedAt int64
}

func (g *Gate) latest(ctx context.Context, to string) (round, error) {
	if to == "" {
		return round{}, fmt.Errorf("feed address missing")
	}
	raw, err := g.caller.EthCall(ctx, map[string]string{"to": to, "data": dex.StaticCall("latestRoundData()")})
	if err != nil {
		return round{}, err
	}
	w, err := dex.DecodeWords(raw)
	if err != nil || len(w) < 5 {
		return round{}, fmt.Errorf("invalid latestRoundData response")
	}
	answer := dex.WordInt(w[1])
	started := dex.WordUint(w[2])
	updated := dex.WordUint(w[3])
	if updated.Sign() <= 0 {
		return round{}, fmt.Errorf("updatedAt missing")
	}
	return round{answer: answer, startedAt: started.Int64(), updatedAt: updated.Int64()}, nil
}
