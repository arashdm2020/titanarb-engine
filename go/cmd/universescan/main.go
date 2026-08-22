package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/rpc"
	"github.com/titanarb/titanarb-go/internal/universe"
)

func main() {
	if _, err := config.Load(); err != nil {
		exit("configuration error: %v", err)
	}
	cfg, err := config.LoadMarketConfig(marketConfigPath())
	if err != nil {
		exit("market config error: %v", err)
	}
	runtimeCfg, err := config.Load()
	if err != nil {
		exit("runtime config error: %v", err)
	}
	m := metrics.New()
	client := rpc.New(runtimeCfg.HTTPRPCURL, 20*time.Second, 1, m)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	discoverer := pools.NewDiscoverer(client, cfg.UniswapFactory, cfg.CamelotFactory, cfg.UniswapFeeTiers)
	uniswap := quotes.NewUniswapV3(client, cfg.UniswapQuoterV2, m)
	camelot := quotes.NewCamelot(client, cfg.CamelotQuoter, m)
	scanner := universe.Scanner{
		Market:     cfg,
		Discoverer: discoverer,
		Caller:     client,
		Quoter: func(pool pools.Pool) quotes.Quoter {
			switch pool.DEX {
			case pools.UniswapV3:
				return uniswap
			case pools.CamelotV3:
				return camelot
			default:
				return nil
			}
		},
	}
	report, err := scanner.Scan(ctx)
	if err != nil {
		exit("universe scan failed: %v", err)
	}
	out := struct {
		universe.Report
		Metrics metrics.Snapshot `json:"metrics"`
	}{
		Report:  report,
		Metrics: m.Snapshot(),
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		exit("encode report: %v", err)
	}
}

func marketConfigPath() string {
	if path := os.Getenv("TITANARB_MARKET_CONFIG"); path != "" {
		return path
	}
	for _, path := range []string{filepath.Join("config", "arbitrum.json"), filepath.Join("..", "config", "arbitrum.json")} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return filepath.Join("config", "arbitrum.json")
}

func exit(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
