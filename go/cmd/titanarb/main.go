// titanarb is Phase 1 of the Go migration: read-only runtime infrastructure.
package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/titanarb/titanarb-go/internal/alpha"
	"github.com/titanarb/titanarb-go/internal/cache"
	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/execution"
	"github.com/titanarb/titanarb-go/internal/fees"
	"github.com/titanarb/titanarb-go/internal/health"
	"github.com/titanarb/titanarb-go/internal/logger"
	"github.com/titanarb/titanarb-go/internal/market"
	"github.com/titanarb/titanarb-go/internal/metrics"
	monitor2 "github.com/titanarb/titanarb-go/internal/monitor"
	"github.com/titanarb/titanarb-go/internal/observability"
	"github.com/titanarb/titanarb-go/internal/operations"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/profit"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/rpc"
	"github.com/titanarb/titanarb-go/internal/safety"
	"github.com/titanarb/titanarb-go/internal/telegram"
	ws "github.com/titanarb/titanarb-go/internal/websocket"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}
	log := logger.New(os.Getenv("TITANARB_LOG_FORMAT") == "json", os.Stdout)
	m := metrics.New()
	operationsDir := os.Getenv("TITANARB_OBSERVABILITY_DIR")
	if operationsDir == "" {
		operationsDir = filepath.Join("runtime", "go")
	}
	operationSink, operationErr := operations.New(operationsDir, telegram.New(telegram.FromEnv()))
	if operationErr != nil {
		log.Event(logger.Warn, "health_check_failed", "operations", "observability disabled: "+operationErr.Error(), nil)
	} else {
		defer operationSink.Close(context.Background())
		defer alert(operationSink, observability.Server, "bot_stopped", telegram.Info, "Go runtime stopped", nil)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	rpcClient := rpc.New(cfg.HTTPRPCURL, 15*time.Second, 2, m)
	chain, err := rpcClient.ChainID(ctx)
	if err != nil || chain != config.ArbitrumOneChainID {
		log.Event(logger.Error, "health_check_failed", "rpc", "unable to validate Arbitrum One", nil)
		os.Exit(1)
	}
	block, err := rpcClient.BlockNumber(ctx)
	if err != nil {
		log.Event(logger.Error, "health_check_failed", "rpc", "unable to read latest block", nil)
		os.Exit(1)
	}
	log.Event(logger.Info, "bot_started", "runtime", "TitanArb Go runtime started", map[string]any{"chain_id": chain, "execution_runtime": os.Getenv("TITANARB_EXECUTION_RUNTIME") == "true"})
	log.Event(logger.Info, "rpc_connected", "rpc", "connected", map[string]any{"latest_block": block})
	alert(operationSink, observability.Server, "bot_started", telegram.Info, "Go runtime started", map[string]any{"chain_id": chain, "latest_block": block, "broadcast_enabled": cfg.BroadcastEnabled})
	w := ws.New(cfg.WSRPCURL, log, m)
	w.SetObserver(func(event string, fields map[string]any) {
		severity := telegram.Info
		if event == "wss_disconnected" {
			severity = telegram.Warning
		}
		alert(operationSink, observability.Server, event, severity, "websocket operational event", fields)
	})
	monitor := health.New(rpcClient, w.Connected)
	systemMonitor := monitor2.New(operationsDir)
	events := w.Start(ctx)
	marketEngine, marketCfg, marketErr := buildMarketEngine(rpcClient, m)
	if marketErr != nil && os.Getenv("TITANARB_MARKET_ENGINE") == "true" {
		log.Event(logger.Warn, "health_check_failed", "market", "market engine disabled: "+marketErr.Error(), nil)
	}
	marketRunning := make(chan struct{}, 1)
	phase4Engine, phase4Err := buildPhase4Engine(rpcClient, m)
	if phase4Err != nil && os.Getenv("TITANARB_PHASE4_READ_ONLY") == "true" {
		log.Event(logger.Warn, "health_check_failed", "phase4", "Phase 4 alpha engine disabled: "+phase4Err.Error(), nil)
	}
	phase4Running := make(chan struct{}, 1)
	var executionPipeline *execution.Pipeline
	if os.Getenv("TITANARB_EXECUTION_RUNTIME") == "true" && marketErr == nil {
		var executionErr error
		executionPipeline, executionErr = buildExecutionPipeline(cfg, marketCfg, rpcClient, m)
		if executionErr != nil {
			log.Event(logger.Error, "health_check_failed", "execution", "Go execution runtime disabled: "+executionErr.Error(), nil)
		} else if executionErr = execution.VerifyDeployment(ctx, rpcClient, cfg, marketCfg, executionPipeline.WalletAddress().Hex()); executionErr != nil {
			log.Event(logger.Error, "health_check_failed", "execution", "on-chain deployment verification failed: "+executionErr.Error(), nil)
			executionPipeline = nil
		} else {
			log.Event(logger.Info, "execution_preflight_verified", "execution", "executor, adapters, routers, allow-lists, and signer verified", nil)
		}
	}
	healthTicker := time.NewTicker(30 * time.Second)
	defer healthTicker.Stop()
	summaryTicker := time.NewTicker(summaryInterval())
	defer summaryTicker.Stop()
	metricsPath := os.Getenv("TITANARB_METRICS_JSON")
	if metricsPath != "" {
		if err := os.MkdirAll(filepath.Dir(metricsPath), 0o700); err != nil {
			log.Event(logger.Warn, "health_check_failed", "metrics", "metrics directory unavailable", nil)
			metricsPath = ""
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			log.Event(logger.Info, "new_block_received", "websocket", "new Arbitrum block", map[string]any{"block": event.Number, "hash": event.Hash})
			publish(operationSink, observability.Performance, "new_block_received", telegram.Info, "new Arbitrum block", map[string]any{"block": event.Number, "hash": event.Hash})
			if marketEngine != nil {
				select {
				case marketRunning <- struct{}{}:
					go func() {
						defer func() { <-marketRunning }()
						cycleCtx, stop := context.WithTimeout(ctx, 45*time.Second)
						defer stop()
						routes, err := marketEngine.Cycle(cycleCtx)
						if err != nil {
							log.Event(logger.Warn, "health_check_failed", "market", "market cycle failed", nil)
							return
						}
						log.Event(logger.Info, "pool_updated", "market", "read-only market cycle complete", map[string]any{"routes": len(routes)})
						for {
							select {
							case outcome := <-marketEngine.Events:
								log.Event(logger.Info, string(outcome.Type), "market", outcome.Reason, nil)
								fields := map[string]any{"reason": outcome.Reason}
								if outcome.Opportunity != nil {
									fields["route"] = outcome.Opportunity.Route.String()
									fields["expected_profit"] = outcome.Opportunity.ExpectedProfit.String()
								}
								severity := telegram.Info
								if outcome.Type == opportunity.QuoteFailed {
									severity = telegram.Warning
								}
								if outcome.Type == opportunity.Found || outcome.Type == opportunity.QuoteFailed {
									alert(operationSink, observability.Opportunities, string(outcome.Type), severity, "market opportunity event", fields)
								} else {
									publish(operationSink, observability.Opportunities, string(outcome.Type), severity, "market opportunity event", fields)
								}
								if outcome.Type == opportunity.Found && executionPipeline != nil {
									result := executionPipeline.Process(cycleCtx, outcome.Opportunity)
									log.Event(logger.Info, "execution_"+result.Decision, "execution", result.Reason, map[string]any{"tx_hash": result.TxHash})
									tradeFields := map[string]any{"decision": result.Decision, "reason": result.Reason, "tx_hash": result.TxHash}
									if result.FinalProfit != nil {
										tradeFields["expected_profit"] = result.FinalProfit.String()
									}
									if result.Receipt != nil {
										tradeFields["receipt_status"] = result.Receipt.Status
										tradeFields["gas_used"] = result.Receipt.GasUsed
									}
									category := observability.Trades
									severity := telegram.Info
									if result.Decision == "reject" {
										category = observability.Errors
										severity = telegram.Warning
									}
									alert(operationSink, category, "execution_"+result.Decision, severity, result.Reason, tradeFields)
								}
							default:
								return
							}
						}
					}()
				default:
					log.Event(logger.Debug, "pool_updated", "market", "previous market cycle still running", nil)
				}
			}
			if phase4Engine != nil {
				select {
				case phase4Running <- struct{}{}:
					go func() {
						defer func() { <-phase4Running }()
						cycleCtx, stop := context.WithTimeout(ctx, 45*time.Second)
						defer stop()
						stats, err := phase4Engine.Refresh(cycleCtx, phase4MaxHops(), 256)
						if err != nil {
							log.Event(logger.Warn, "health_check_failed", "phase4", "read-only graph refresh failed", nil)
							publish(operationSink, observability.Errors, "phase4_refresh_failed", telegram.Warning, "read-only graph refresh failed", nil)
							return
						}
						fields := map[string]any{"pairs": stats.Pairs, "pools": stats.Pools, "routes": stats.Routes, "failures": stats.Failures}
						log.Event(logger.Info, "pool_updated", "phase4", "asset-agnostic graph refresh complete", fields)
						publish(operationSink, observability.Performance, "phase4_graph_refreshed", telegram.Info, "read-only asset-agnostic graph refresh complete", fields)
					}()
				default:
					log.Event(logger.Debug, "pool_updated", "phase4", "previous graph refresh still running", nil)
				}
			}
		case <-healthTicker.C:
			report := monitor.Check(ctx)
			if report.Status != health.Healthy {
				log.Event(logger.Warn, "health_check_failed", "health", report.Message, map[string]any{"latest_block": report.LatestBlock})
				alert(operationSink, observability.Errors, "health_check_failed", telegram.Warning, report.Message, map[string]any{"latest_block": report.LatestBlock})
			}
			if sample, err := systemMonitor.Sample(ctx); err == nil {
				publish(operationSink, observability.Server, "server_metrics", telegram.Info, "server metrics sampled", map[string]any{"process_rss": sample.ProcessRSS, "cpu_percent": sample.CPUPercent, "ram_percent": sample.HostRAMPercent, "disk_percent": sample.DiskUsedPercent, "uptime_seconds": uint64(sample.Uptime.Seconds())})
			}
			if metricsPath != "" {
				file, err := os.OpenFile(metricsPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
				if err != nil {
					log.Event(logger.Warn, "health_check_failed", "metrics", "metrics file unavailable", nil)
					metricsPath = ""
				} else {
					err = m.WriteJSON(file)
					_ = file.Close()
					if err != nil {
						log.Event(logger.Warn, "health_check_failed", "metrics", "metrics write failed", nil)
					}
				}
			}
			publish(operationSink, observability.Performance, "metrics_snapshot", telegram.Info, "runtime metrics snapshot", map[string]any{"blocks": m.Snapshot().BlocksReceived, "routes": m.Snapshot().RoutesEvaluated, "quotes": m.Snapshot().Quotes, "quote_failures": m.Snapshot().QuoteFailures, "opportunities": m.Snapshot().Opportunities})
		case <-summaryTicker.C:
			snapshot := m.Snapshot()
			alert(operationSink, observability.Performance, "operational_summary", telegram.Info, "periodic operational summary", map[string]any{"blocks": snapshot.BlocksReceived, "routes": snapshot.RoutesEvaluated, "quotes": snapshot.Quotes, "opportunities": snapshot.Opportunities, "rpc_errors": snapshot.RPCErrors, "wss_disconnects": snapshot.WSSDisconnects, "transactions": snapshot.TransactionsBroadcast})
		}
	}
}

func buildMarketEngine(client *rpc.Client, metrics *metrics.Metrics) (*market.Engine, config.MarketConfig, error) {
	if os.Getenv("TITANARB_MARKET_ENGINE") != "true" {
		return nil, config.MarketConfig{}, nil
	}
	marketConfig, err := loadMarketConfig()
	if err != nil {
		return nil, config.MarketConfig{}, err
	}
	amount, err := rawEnv("TITANARB_MARKET_AMOUNT_RAW")
	if err != nil {
		return nil, config.MarketConfig{}, err
	}
	minimum, err := rawEnv("TITANARB_MARKET_MIN_PROFIT_RAW")
	if err != nil {
		return nil, config.MarketConfig{}, err
	}
	discoverer := pools.NewDiscoverer(client, marketConfig.UniswapFactory, marketConfig.CamelotFactory, marketConfig.UniswapFeeTiers)
	uni := quotes.NewUniswapV3(client, marketConfig.UniswapQuoterV2, metrics)
	camelot := quotes.NewCamelot(client, marketConfig.CamelotQuoter, metrics)
	safetyBPS := uint64(1000)
	if raw := os.Getenv("GAS_SAFETY_MARGIN_BPS"); raw != "" {
		if parsed, parseErr := strconv.ParseUint(raw, 10, 64); parseErr == nil {
			safetyBPS = parsed
		}
	}
	base, ok := marketConfig.Tokens[marketConfig.BaseAsset]
	if !ok {
		return nil, config.MarketConfig{}, fmt.Errorf("base token missing from market config")
	}
	feeService := fees.New(client, marketConfig.ArbGasInfo, marketConfig.ETHUSDFeed, safetyBPS)
	evaluator := opportunity.New(client, marketConfig, uni, camelot, opportunity.ArbitrumCostModel{Service: feeService, BaseDecimals: base.Decimals}, minimum, 4, metrics)
	return market.New(marketConfig, discoverer, cache.NewPoolCache(metrics), evaluator, amount, 4, metrics), marketConfig, nil
}

// buildPhase4Engine is read-only by construction: it has no signer,
// transaction builder, simulation client, or execution-pipeline dependency.
func buildPhase4Engine(client *rpc.Client, m *metrics.Metrics) (*alpha.Engine, error) {
	if os.Getenv("TITANARB_PHASE4_READ_ONLY") != "true" {
		return nil, nil
	}
	marketConfig, err := loadMarketConfig()
	if err != nil {
		return nil, err
	}
	discoverer := pools.NewDiscoverer(client, marketConfig.UniswapFactory, marketConfig.CamelotFactory, marketConfig.UniswapFeeTiers)
	return alpha.New(marketConfig.Tokens, discoverer, quotes.NewUniswapV3(client, marketConfig.UniswapQuoterV2, m), quotes.NewCamelot(client, marketConfig.CamelotQuoter, m), 4), nil
}

func phase4MaxHops() int {
	const safeExecutionMaxHops = 4
	value, err := strconv.Atoi(os.Getenv("TITANARB_GRAPH_MAX_HOPS"))
	if err != nil || value < 2 {
		return safeExecutionMaxHops
	}
	// The deployed safety path is proven for at most four hops. Keep graph
	// discovery within that active compatibility boundary until it is extended
	// by a separately validated contract change.
	if value > safeExecutionMaxHops {
		return safeExecutionMaxHops
	}
	return value
}

func publish(sink *operations.Sink, category, event string, severity telegram.Severity, message string, fields map[string]any) {
	if sink != nil {
		sink.Publish(operations.Event{Category: category, Name: event, Severity: severity, Message: message, Fields: fields})
	}
}

func alert(sink *operations.Sink, category, event string, severity telegram.Severity, message string, fields map[string]any) {
	if sink != nil {
		sink.Publish(operations.Event{Category: category, Name: event, Severity: severity, Message: message, Fields: fields, Notify: true})
	}
}

func summaryInterval() time.Duration {
	if raw := os.Getenv("TITANARB_TELEGRAM_SUMMARY_INTERVAL"); raw != "" {
		if interval, err := time.ParseDuration(raw); err == nil && interval >= time.Minute {
			return interval
		}
	}
	return time.Hour
}

func loadMarketConfig() (config.MarketConfig, error) {
	configPath := os.Getenv("TITANARB_MARKET_CONFIG")
	if configPath == "" {
		for _, candidate := range []string{"config/arbitrum.json", filepath.Join("..", "config", "arbitrum.json")} {
			if _, err := os.Stat(candidate); err == nil {
				configPath = candidate
				break
			}
		}
	}
	return config.LoadMarketConfig(configPath)
}

func buildExecutionPipeline(cfg config.Config, marketCfg config.MarketConfig, client *rpc.Client, m *metrics.Metrics) (*execution.Pipeline, error) {
	minimum, err := rawEnv("TITANARB_EXECUTION_MIN_PROFIT_RAW")
	if err != nil {
		minimum, err = rawEnv("TITANARB_MARKET_MIN_PROFIT_RAW")
	}
	if err != nil {
		return nil, fmt.Errorf("execution min profit: %w", err)
	}
	safetyBPS := uint64(1000)
	if raw := os.Getenv("GAS_SAFETY_MARGIN_BPS"); raw != "" {
		if parsed, parseErr := strconv.ParseUint(raw, 10, 64); parseErr == nil {
			safetyBPS = parsed
		}
	}
	feeService := fees.New(client, marketCfg.ArbGasInfo, marketCfg.ETHUSDFeed, safetyBPS)
	gate := safety.New(client, marketCfg.ETHUSDFeed, marketCfg.SequencerUptimeFeed, time.Duration(cfg.PriceMaxStalenessSeconds)*time.Second, time.Duration(cfg.SequencerGraceSeconds)*time.Second)
	return execution.NewPipeline(cfg, marketCfg, client, feeService, gate, minimum, profit.New(os.Getenv("TITANARB_TRADE_METRICS_JSON")), m)
}

func rawEnv(name string) (*big.Int, error) {
	v, ok := new(big.Int).SetString(os.Getenv(name), 10)
	if !ok || v.Sign() < 0 {
		return nil, fmt.Errorf("%s must be a non-negative raw token amount", name)
	}
	return v, nil
}
