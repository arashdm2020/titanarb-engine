// Package health reports read-only runtime health.
package health

import (
	"context"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/rpc"
)

type Status string

const (
	Healthy  Status = "HEALTHY"
	Degraded Status = "DEGRADED"
	Failed   Status = "FAILED"
)

type Report struct {
	Status       Status
	ChainID      int64
	LatestBlock  uint64
	WSSConnected bool
	Message      string
}
type Monitor struct {
	rpc          *rpc.Client
	wssConnected func() bool
	lastBlock    uint64
}

func New(client *rpc.Client, wssConnected func() bool) *Monitor {
	return &Monitor{rpc: client, wssConnected: wssConnected}
}
func (m *Monitor) Check(ctx context.Context) Report {
	chain, err := m.rpc.ChainID(ctx)
	if err != nil {
		return Report{Status: Failed, Message: "RPC unreachable"}
	}
	if chain != config.ArbitrumOneChainID {
		return Report{Status: Failed, ChainID: chain, Message: "unexpected chain ID"}
	}
	block, err := m.rpc.BlockNumber(ctx)
	if err != nil {
		return Report{Status: Failed, ChainID: chain, Message: "block read failed"}
	}
	status := Healthy
	message := "healthy"
	if !m.wssConnected() {
		status = Degraded
		message = "WSS unavailable; HTTP polling fallback"
	}
	if m.lastBlock > 0 && block <= m.lastBlock {
		status = Degraded
		message = "latest block not moving"
	}
	m.lastBlock = block
	return Report{status, chain, block, m.wssConnected(), message}
}
