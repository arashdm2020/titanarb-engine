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
	"strings"
	"syscall"
	"time"

	"github.com/titanarb/titanarb-go/internal/alpha"
	"github.com/titanarb/titanarb-go/internal/cache"
	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/control"
	"github.com/titanarb/titanarb-go/internal/dashboard"
	"github.com/titanarb/titanarb-go/internal/dex"
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
	"github.com/titanarb/titanarb-go/internal/runtimeconfig"
	"github.com/titanarb/titanarb-go/internal/safety"
	"github.com/titanarb/titanarb-go/internal/scheduler"
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
	telegramConfig := telegram.FromEnv()
	notifier := telegram.New(telegramConfig)
	operationSink, operationErr := operations.New(operationsDir, notifier)
	if operationErr != nil {
		log.Event(logger.Warn, "health_check_failed", "operations", "observability disabled: "+operationErr.Error(), nil)
	} else {
		defer operationSink.Close(context.Background())
		defer alert(operationSink, observability.Server, "bot_stopped", telegram.Info, "Go runtime stopped", nil)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	runtimeConfigPath := os.Getenv("TITANARB_RUNTIME_CONFIG_PATH")
	if runtimeConfigPath == "" {
		runtimeConfigPath = filepath.Join(operationsDir, "runtime_config.json")
	}
	runtimeRisk, riskErr := runtimeconfig.Open(runtimeConfigPath, runtimeconfig.Defaults(runtimeconfig.Balanced))
	if riskErr != nil {
		log.Event(logger.Warn, "health_check_failed", "risk", "runtime risk controls unavailable: "+riskErr.Error(), nil)
		runtimeRisk, _ = runtimeconfig.Open("", runtimeconfig.Defaults(runtimeconfig.Balanced))
	}
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
	if telegramConfig.Enabled() && runtimeRisk != nil {
		go control.Run(ctx, notifier, control.Handler{
			Auth: control.Authorizer{ChatID: telegramConfig.ChatID, AdminID: os.Getenv("TELEGRAM_ADMIN_ID")},
			Risk: runtimeRisk,
			Status: func() string {
				return fmt.Sprintf("🟢 TITANARB — STATUS\n🌐 Arbitrum One\n🧠 Risk: %s\n🔌 WSS: %t\n📡 RPC: Healthy", runtimeRisk.Snapshot().Profile, w.Connected())
			},
			Market: func() string {
				marketSnapshot := market.Snapshot{}
				if marketEngine != nil {
					marketSnapshot = marketEngine.Snapshot()
				}
				wssStatus := "Disconnected"
				if w.Connected() {
					wssStatus = "Healthy"
				}
				return dashboard.FormatMarket(dashboard.MarketSnapshot{Status: "ONLINE", RiskProfile: string(runtimeRisk.Snapshot().Profile), WSS: wssStatus, ActivePools: marketSnapshot.ActivePools, Cycles: marketSnapshot.Cycles, Metrics: m.Snapshot()})
			},
			Top: func() string { return "🏆 No globally ranked candidate is available yet." },
		})
	}
	phase4Engine, phase4Err := buildPhase4Engine(rpcClient, m)
	if phase4Err != nil && os.Getenv("TITANARB_PHASE4_READ_ONLY") == "true" {
		log.Event(logger.Warn, "health_check_failed", "phase4", "Phase 4 alpha engine disabled: "+phase4Err.Error(), nil)
	}
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
	var marketScheduler *scheduler.Latest
	if marketEngine != nil {
		var recordedCoalesced uint64
		marketScheduler = scheduler.New(func(runCtx context.Context, trigger scheduler.Trigger) {
			cycleStarted := time.Now()
			before := m.Snapshot()
			cycleCtx, stop := context.WithTimeout(runCtx, 45*time.Second)
			defer stop()
			settings := runtimeconfig.Defaults(runtimeconfig.Balanced)
			if runtimeRisk != nil {
				settings = runtimeRisk.Snapshot()
			}
			report, cycleErr := marketEngine.CycleAt(cycleCtx, trigger.Block, settings.RouteSearchDepth, routeBudget(settings), settings.VolatilityWeight)
			if cycleErr != nil {
				log.Event(logger.Warn, "health_check_failed", "market", "market cycle failed", map[string]any{"block": trigger.Block, "error_type": fmt.Sprintf("%T", cycleErr)})
				publish(operationSink, observability.Errors, "market_cycle_failed", telegram.Warning, "market cycle failed", map[string]any{"block": trigger.Block, "error_type": fmt.Sprintf("%T", cycleErr)})
				return
			}

			var rejected, quoteFailed, found, stale uint64
			for {
				select {
				case outcome := <-marketEngine.Events:
					fields := map[string]any{"reason": outcome.Reason, "source_block": trigger.Block}
					if outcome.Opportunity != nil {
						fields["route"] = outcome.Opportunity.Route.String()
						fields["expected_profit"] = outcome.Opportunity.ExpectedProfit.String()
						fields["source_block"] = outcome.Opportunity.SourceBlock
					}
					switch outcome.Type {
					case opportunity.Rejected:
						rejected++
					case opportunity.QuoteFailed:
						quoteFailed++
						publish(operationSink, observability.Opportunities, string(outcome.Type), telegram.Warning, "quote failed", fields)
					case opportunity.Found:
						found++
						log.Event(logger.Info, string(outcome.Type), "market", "profitable candidate detected", fields)
						alert(operationSink, observability.Opportunities, string(outcome.Type), telegram.Info, "profitable candidate detected", fields)
						if executionPipeline != nil {
							result := executionPipeline.Process(cycleCtx, outcome.Opportunity)
							if strings.HasPrefix(result.Reason, "stale candidate:") {
								stale++
							}
							log.Event(logger.Info, "execution_"+result.Decision, "execution", result.Reason, map[string]any{"tx_hash": result.TxHash})
							tradeFields := map[string]any{"decision": result.Decision, "reason": result.Reason, "tx_hash": result.TxHash}
							if result.FinalProfit != nil {
								tradeFields["expected_profit"] = result.FinalProfit.String()
							}
							if result.Receipt != nil {
								tradeFields["receipt_status"] = result.Receipt.Status
								tradeFields["gas_used"] = result.Receipt.GasUsed
							}
							category, severity := observability.Trades, telegram.Info
							if result.Decision == "reject" {
								category, severity = observability.Errors, telegram.Warning
							}
							alert(operationSink, category, "execution_"+result.Decision, severity, result.Reason, tradeFields)
						}
					}
				default:
					goto eventsDrained
				}
			}
		eventsDrained:
			if phase4Engine != nil && report.FullReconcile {
				stats, refreshErr := phase4Engine.Refresh(cycleCtx, boundedHopDepth(settings.RouteSearchDepth), routeBudget(settings))
				if refreshErr != nil {
					log.Event(logger.Warn, "health_check_failed", "phase4", "periodic graph reconciliation failed", nil)
				} else {
					publish(operationSink, observability.Performance, "phase4_graph_reconciled", telegram.Info, "periodic asset-agnostic graph reconciliation complete", map[string]any{"pairs": stats.Pairs, "pools": stats.Pools, "routes": stats.Routes, "failures": stats.Failures})
				}
			}

			latest := marketScheduler.LatestBlock()
			lag := uint64(0)
			if latest > report.StateBlock {
				lag = latest - report.StateBlock
			}
			coalesced := marketScheduler.BlocksCoalesced()
			if coalesced > recordedCoalesced {
				m.AddBlocksCoalesced(coalesced - recordedCoalesced)
				recordedCoalesced = coalesced
			}
			after := m.Snapshot()
			rpcCalls := after.RPCCalls - before.RPCCalls
			duration := report.Duration
			blockToStart := cycleStarted.Sub(trigger.ObservedAt)
			if blockToStart < 0 {
				blockToStart = 0
			}
			m.ObserveMarketCycle(metrics.CycleSample{
				BlockToStartMS: uint64(blockToStart.Milliseconds()),
				DurationMS:     uint64(duration.Milliseconds()), QuoteDurationMS: uint64(report.QuoteDuration.Milliseconds()),
				OptimizerMS: uint64(report.OptimizerDuration.Milliseconds()), OptimizerRuns: report.OptimizerRuns, OptimizerSamples: report.OptimizerSamples,
				LagBlocks: lag, DirtyPools: report.DirtyPools, RoutesRecomputed: report.RoutesRecomputed, RoutesReused: report.RoutesReused,
				RoutesEvaluated: report.RoutesEvaluated, RPCCalls: rpcCalls,
			})
			fields := map[string]any{
				"block": report.StateBlock, "full_reconcile": report.FullReconcile, "cycles": len(report.Routes),
				"routes_recomputed": report.RoutesRecomputed, "routes_reused": report.RoutesReused, "routes_evaluated_cycle": report.RoutesEvaluated,
				"quotes": after.Quotes - before.Quotes, "quote_failures": quoteFailed,
				"rejected_min_profit": rejected, "profitable_candidates": found, "stale_candidates": stale,
				"dirty_pools": report.DirtyPools, "block_to_cycle_start_ms": blockToStart.Milliseconds(), "duration_ms": duration.Milliseconds(),
				"quote_duration_ms": report.QuoteDuration.Milliseconds(), "optimizer_duration_ms": report.OptimizerDuration.Milliseconds(),
				"optimizer_runs": report.OptimizerRuns, "optimizer_samples": report.OptimizerSamples, "cycle_lag_blocks": lag,
				"blocks_coalesced": coalesced, "rpc_calls": rpcCalls,
			}
			log.Event(logger.Info, "market_cycle", "market", "market cycle complete", fields)
			publish(operationSink, observability.Performance, "market_cycle", telegram.Info, "market cycle complete", fields)
		})
		if executionPipeline != nil {
			executionPipeline.SetLatestBlockSource(marketScheduler.LatestBlock)
		}
		go marketScheduler.Run(ctx)
	}
	healthTicker := time.NewTicker(30 * time.Second)
	defer healthTicker.Stop()
	pollTicker := time.NewTicker(pollInterval())
	defer pollTicker.Stop()
	summaryTicker := time.NewTicker(summaryInterval())
	defer summaryTicker.Stop()
	metricsPath := os.Getenv("TITANARB_METRICS_JSON")
	if metricsPath != "" {
		if err := os.MkdirAll(filepath.Dir(metricsPath), 0o700); err != nil {
			log.Event(logger.Warn, "health_check_failed", "metrics", "metrics directory unavailable", nil)
			metricsPath = ""
		}
	}
	fallbackActive := false
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			log.Event(logger.Debug, "new_block_received", "websocket", "new Arbitrum block", map[string]any{"block": event.Number, "hash": event.Hash})
			publish(operationSink, observability.Performance, "new_block_received", telegram.Info, "new Arbitrum block", map[string]any{"block": event.Number, "hash": event.Hash})
			if marketScheduler != nil {
				fallbackActive = false
				blockNumber, parseErr := parseHexBlock(event.Number)
				if parseErr != nil {
					log.Event(logger.Warn, "health_check_failed", "market", "invalid newHeads block number", nil)
					continue
				}
				marketScheduler.Submit(scheduler.Trigger{Block: blockNumber, Hash: event.Hash, ObservedAt: time.Now().UTC()})
			}
		case <-pollTicker.C:
			if marketScheduler == nil || w.Connected() {
				continue
			}
			if !fallbackActive {
				fallbackActive = true
				log.Event(logger.Warn, "http_fallback_active", "market", "WSS unavailable; HTTP polling fallback active", nil)
				alert(operationSink, observability.Server, "http_fallback_active", telegram.Warning, "WSS unavailable; HTTP polling fallback active", nil)
			}
			latestBlock, pollErr := rpcClient.BlockNumber(ctx)
			if pollErr != nil {
				log.Event(logger.Warn, "rpc_degraded", "rpc", "HTTP fallback block poll failed", map[string]any{"error_type": fmt.Sprintf("%T", pollErr)})
				publish(operationSink, observability.Errors, "rpc_degraded", telegram.Warning, "HTTP fallback block poll failed", map[string]any{"error_type": fmt.Sprintf("%T", pollErr)})
				continue
			}
			marketScheduler.Submit(scheduler.Trigger{Block: latestBlock, ObservedAt: time.Now().UTC()})
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
			marketSnapshot := market.Snapshot{}
			if marketEngine != nil {
				marketSnapshot = marketEngine.Snapshot()
			}
			wssStatus := "Disconnected"
			if w.Connected() {
				wssStatus = "Healthy"
			}
			profile := "BALANCED"
			if runtimeRisk != nil {
				profile = string(runtimeRisk.Snapshot().Profile)
			}
			message := dashboard.FormatMarket(dashboard.MarketSnapshot{Status: "ONLINE", RiskProfile: profile, WSS: wssStatus, ActivePools: marketSnapshot.ActivePools, Cycles: marketSnapshot.Cycles, Metrics: snapshot})
			alert(operationSink, observability.Performance, "operational_summary", telegram.Info, message, map[string]any{"blocks": snapshot.BlocksReceived, "routes": snapshot.RoutesEvaluated, "quotes": snapshot.Quotes, "opportunities": snapshot.Opportunities, "rpc_errors": snapshot.RPCErrors, "wss_disconnects": snapshot.WSSDisconnects, "transactions": snapshot.TransactionsBroadcast})
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
	feeService := fees.New(client, marketConfig.ArbGasInfo, marketConfig.ETHUSDFeed, safetyBPS)
	quoteWorkers := boundedWorkerCount("TITANARB_QUOTE_WORKERS", 8)
	evaluator := opportunity.New(client, marketConfig, uni, camelot, opportunity.ArbitrumCostModel{Service: feeService}, minimum, quoteWorkers, metrics)
	amounts := marketAmounts(marketConfig, amount)
	liquidityProvider := func(ctx context.Context, asset string) (*big.Int, error) {
		token, ok := marketConfig.Tokens[asset]
		if !ok {
			return nil, fmt.Errorf("unknown liquidity asset %s", asset)
		}

		word, err := dex.AddressWord(token.Address)
		if err != nil {
			return nil, err
		}

		raw, err := client.EthCall(ctx, map[string]string{
			"to":   marketConfig.AavePool,
			"data": dex.StaticCall("getVirtualUnderlyingBalance(address)", word),
		})
		if err != nil {
			return nil, err
		}

		words, err := dex.DecodeWords(raw)
		if err != nil || len(words) == 0 {
			return nil, fmt.Errorf("invalid aave liquidity response")
		}

		available := dex.WordUint(words[0])
		available.Mul(available, big.NewInt(30))
		available.Div(available, big.NewInt(100))

		return available, nil
	}

	return market.NewWithAmounts(marketConfig, discoverer, cache.NewPoolCache(metrics), evaluator, amounts, 4, metrics, liquidityProvider), marketConfig, nil
}

func boundedWorkerCount(name string, fallback int) int {
	workers := fallback
	if parsed, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name))); err == nil {
		workers = parsed
	}
	if workers < 1 {
		return 1
	}
	if workers > 32 {
		return 32
	}
	return workers
}

func marketAmounts(marketConfig config.MarketConfig, legacyAmount *big.Int) map[string]*big.Int {
	amounts := make(map[string]*big.Int)
	for _, symbol := range marketConfig.ExecutionAssets() {
		name := "TITANARB_MARKET_AMOUNT_" + strings.ToUpper(strings.ReplaceAll(symbol, ".", "_")) + "_RAW"
		if raw := os.Getenv(name); raw != "" {
			if amount, ok := new(big.Int).SetString(raw, 10); ok && amount.Sign() > 0 {
				amounts[symbol] = amount
			}
		}
	}
	// Compatibility for the pre-existing one-amount configuration. This is
	// intentionally restricted to its legacy asset; it is never copied to
	// WETH, ARB, USDT, or another asset.
	if len(amounts) == 0 && marketConfig.BaseAsset != "" && legacyAmount != nil && legacyAmount.Sign() > 0 {
		amounts[marketConfig.BaseAsset] = new(big.Int).Set(legacyAmount)
	}
	return amounts
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

func boundedHopDepth(value int) int {
	if value < 2 {
		return 2
	}
	if value > 4 {
		return 4
	}
	return value
}

func routeBudget(settings runtimeconfig.Settings) int {
	// Optimizer depth governs bounded read-only candidate breadth. The cap
	// protects the RPC budget and is applied evenly across all start assets by
	// routes.BuildAll.
	depth := settings.OptimizerDepth
	if depth < 2 {
		depth = 2
	}
	if depth > 32 {
		depth = 32
	}
	return depth * 32
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

func pollInterval() time.Duration {
	if raw := strings.TrimSpace(os.Getenv("POLL_INTERVAL_SECONDS")); raw != "" {
		if seconds, err := strconv.ParseUint(raw, 10, 64); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return 15 * time.Second
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

func parseHexBlock(raw string) (uint64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, fmt.Errorf("empty block number")
	}
	base := 10
	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		base = 16
		value = value[2:]
	}
	return strconv.ParseUint(value, base, 64)
}
