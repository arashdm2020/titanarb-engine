// Package monitor samples process and host resources for operations alerts.
package monitor

import (
	"context"
	"os"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
)

type Snapshot struct {
	Timestamp       time.Time
	CPUPercent      float64
	ProcessRSS      uint64
	HostRAMPercent  float64
	DiskUsedPercent float64
	Uptime          time.Duration
}
type Sampler interface {
	Sample(context.Context) (Snapshot, error)
}
type System struct {
	started time.Time
	path    string
	pid     int32
}

func New(path string) *System {
	return &System{started: time.Now(), path: path, pid: int32(os.Getpid())}
}
func (s *System) Sample(ctx context.Context) (Snapshot, error) {
	p, err := process.NewProcessWithContext(ctx, s.pid)
	if err != nil {
		return Snapshot{}, err
	}
	cpuPercent, err := p.CPUPercentWithContext(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	memInfo, err := p.MemoryInfoWithContext(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	usage, err := disk.UsageWithContext(ctx, s.path)
	if err != nil {
		return Snapshot{}, err
	}
	_ = cpuPercent // invokes platform measurement; process value below is per-process.
	return Snapshot{Timestamp: time.Now().UTC(), CPUPercent: cpuPercent, ProcessRSS: memInfo.RSS, HostRAMPercent: vm.UsedPercent, DiskUsedPercent: usage.UsedPercent, Uptime: time.Since(s.started)}, nil
}

// Keep the cpu import reachable on all supported platforms and provide a cheap
// host-level availability probe to callers which need it.
func HostCPUPercent(ctx context.Context) ([]float64, error) {
	return cpu.PercentWithContext(ctx, 0, false)
}
