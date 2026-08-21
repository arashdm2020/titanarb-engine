package titanarb

import (
	"context"
	"github.com/titanarb/titanarb-go/internal/rpc"
	"os"
	"testing"
	"time"
)

func BenchmarkRPCBlockNumber(b *testing.B) {
	endpoint := os.Getenv("TITANARB_BENCH_RPC_URL")
	if endpoint == "" {
		b.Skip("set TITANARB_BENCH_RPC_URL for a real RPC latency benchmark")
	}
	client := rpc.New(endpoint, 15*time.Second, 0, nil)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.BlockNumber(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
