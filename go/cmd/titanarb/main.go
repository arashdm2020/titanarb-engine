// titanarb is Phase 1 of the Go migration: read-only runtime infrastructure.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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
	"github.com/titanarb/titanarb-go/internal/nearmiss"
	"github.com/titanarb/titanarb-go/internal/observability"
	"github.com/titanarb/titanarb-go/internal/operations"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pairintel"
	"github.com/titanarb/titanarb-go/internal/pools"
	"github.com/titanarb/titanarb-go/internal/profit"
	"github.com/titanarb/titanarb-go/internal/quotes"
	"github.com/titanarb/titanarb-go/internal/rpc"
	"github.com/titanarb/titanarb-go/internal/runtimeconfig"
	"github.com/titanarb/titanarb-go/internal/safety"
	"github.com/titanarb/titanarb-go/internal/scheduler"
	"github.com/titanarb/titanarb-go/internal/telegram"
	"github.com/titanarb/titanarb-go/internal/universe"
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
	rpcClient := rpc.NewManagedWithBudgets(rpcProviderConfigs(cfg), cfg.RPCReadTargetRPS, cfg.RPCPremiumAggregateRPS, cfg.RPCMaxEthCallsPerMinute, cfg.RPCMaxHotCallsPerMinute, 15*time.Second, 2, m)
	rpcClient.SetObserver(func(event rpc.Event) {
		log.Event(logger.Warn, event.Name, "rpc", "RPC provider changed", map[string]any{"from": event.From, "to": event.To, "transport": event.Transport, "reason": event.Reason})
		alert(operationSink, observability.Server, event.Name, telegram.Warning, "RPC provider changed", map[string]any{"from": event.From, "to": event.To, "transport": event.Transport, "reason": event.Reason})
	})
	rpcClient.SetFailureObserver(func(event rpc.FailureEvent) {
		fields := map[string]any{
			"provider": event.Provider, "endpoint": event.Endpoint, "tier": event.Tier,
			"method": event.Method, "http_status": event.HTTPStatus, "json_rpc_code": event.RPCCode,
			"json_rpc_message": safeRPCMessage(event.RPCMessage), "retryable": event.Retryable,
			"rate_limited": event.RateLimited, "timeout": event.Timeout,
			"cooldown_ms": event.Cooldown.Milliseconds(), "stage": event.Stage,
			"request_latency_ms": event.Latency.Milliseconds(), "block": event.Block,
		}
		for key, value := range event.Fields {
			fields[key] = safeRPCField(value)
		}
		level := logger.Debug
		if event.Retryable {
			level = logger.Warn
		}
		log.Event(level, "rpc_request_failed", "rpc", "RPC request failed", fields)
		if event.Retryable {
			publish(operationSink, observability.Errors, "rpc_request_failed", telegram.Warning, "RPC request failed", fields)
		}
	})
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
	w := ws.NewManaged(wssEndpoints(cfg), log, m)
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
	var marketBusy atomic.Bool
	marketEngine, marketCfg, pairService, marketErr := buildMarketEngine(rpcClient, m, operationsDir, &marketBusy)
	if marketErr != nil && os.Getenv("TITANARB_MARKET_ENGINE") == "true" {
		log.Event(logger.Warn, "health_check_failed", "market", "market engine disabled: "+marketErr.Error(), nil)
	}
	if pairService != nil {
		go pairService.Run(ctx)
	}
	if telegramConfig.Enabled() && runtimeRisk != nil {
		go control.Run(ctx, notifier, control.Handler{
			Auth: control.Authorizer{ChatID: telegramConfig.ChatID, AdminID: os.Getenv("TELEGRAM_ADMIN_ID")},
			Risk: runtimeRisk,
			Status: func() string {
				return fmt.Sprintf("🟢 TITANARB — STATUS\n🌐 Arbitrum One\n🧠 Risk: %s\n🔌 WSS: %s %t\n📡 RPC: %s", runtimeRisk.Snapshot().Profile, w.ActiveProvider(), w.Connected(), rpcClient.ActiveProvider())
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
		} else if executionErr = execution.VerifyDeployment(rpc.WithRequestMetadata(ctx, "execution", block), rpcClient, cfg, marketCfg, executionPipeline.WalletAddress().Hex()); executionErr != nil {
			log.Event(logger.Error, "health_check_failed", "execution", "on-chain deployment verification failed: "+executionErr.Error(), nil)
			executionPipeline = nil
		} else {
			log.Event(logger.Info, "execution_preflight_verified", "execution", "executor, adapters, routers, allow-lists, and signer verified", nil)
		}
	}
	var marketScheduler *scheduler.Latest
	if marketEngine != nil {
		var recordedCoalesced uint64
		executionAssets := assetSet(marketCfg.ExecutionAssets())
		marketScheduler = scheduler.New(func(runCtx context.Context, trigger scheduler.Trigger) {
			marketBusy.Store(true)
			defer marketBusy.Store(false)
			cycleStarted := time.Now()
			before := m.Snapshot()
			settings := runtimeconfig.Defaults(runtimeconfig.Balanced)
			if runtimeRisk != nil {
				settings = runtimeRisk.Snapshot()
			}
			searchOptions := marketSearchOptions(settings)
			fullReconcile := marketEngine.RequiresFullReconcile(settings.RouteSearchDepth, routeBudget(settings), searchOptions)
			cycleTimeout := 45 * time.Second
			if fullReconcile {
				cycleTimeout = time.Duration(envIntBounded("FULL_RECONCILE_TIMEOUT_SECONDS", 60, 45, 120)) * time.Second
			}
			timeoutCtx, stopTimeout := context.WithTimeout(runCtx, cycleTimeout)
			defer stopTimeout()
			maxWorkLag := uint64(envIntBounded("MAX_DETECTOR_WORK_LAG_BLOCKS", 12, 0, 64))
			cycleCtx := timeoutCtx
			stopStale := context.CancelFunc(func() {})
			if !fullReconcile {
				cycleCtx, stopStale = cancelWhenSuperseded(timeoutCtx, marketScheduler.LatestBlock, trigger.Block, maxWorkLag)
			}
			defer stopStale()
			report, cycleErr := marketEngine.CycleAtWithSearchOptions(cycleCtx, trigger.Block, settings.RouteSearchDepth, routeBudget(settings), settings.VolatilityWeight, searchOptions)
			if cycleErr != nil {
				if errors.Is(cycleErr, context.Canceled) || cycleCtx.Err() == context.Canceled {
					publish(operationSink, observability.Performance, "market_cycle_superseded", telegram.Info, "stale market work cancelled", map[string]any{"source_block": trigger.Block, "latest_block": marketScheduler.LatestBlock(), "max_work_lag_blocks": maxWorkLag})
					return
				}
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
						mergeFields(fields, opportunityTelemetry(outcome.Opportunity))
					}
					switch outcome.Type {
					case opportunity.Rejected:
						rejected++
						publish(operationSink, observability.Opportunities, "route_evaluation_rejected", telegram.Info, "route rejected by economics", fields)
					case opportunity.QuoteFailed:
						quoteFailed++
						publish(operationSink, observability.Opportunities, string(outcome.Type), telegram.Warning, "quote failed", fields)
					case opportunity.Found:
						found++
						log.Event(logger.Info, string(outcome.Type), "market", "profitable candidate detected", fields)
						alert(operationSink, observability.Opportunities, string(outcome.Type), telegram.Info, "profitable candidate detected", fields)
						if outcome.Opportunity != nil && !routeStartsAndEndsWithExecutionAsset(outcome.Opportunity.Route.Symbols, executionAssets) {
							fields["execution_eligible"] = false
							fields["decision"] = "market_only_dynamic_universe"
							log.Event(logger.Info, "dynamic_candidate_found", "market", "profitable dynamic-universe candidate is not in execution allow-list", fields)
							publish(operationSink, observability.Opportunities, "dynamic_candidate_found", telegram.Info, "profitable dynamic-universe candidate is not in execution allow-list", fields)
							continue
						}
						if executionPipeline != nil {
							criticalCtx := rpc.WithRequestMetadata(rpc.WithRequestClass(ctx, rpc.Critical), "execution", trigger.Block)
							executionCtx, cancelExecution := context.WithTimeout(criticalCtx, 45*time.Second)
							result := executionPipeline.Process(executionCtx, outcome.Opportunity)
							cancelExecution()
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
			avgRPS := 0.0
			if duration > 0 {
				avgRPS = float64(rpcCalls) / duration.Seconds()
			}
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
			rolling := m.Snapshot()
			fields := map[string]any{
				"block": report.StateBlock, "full_reconcile": report.FullReconcile, "cycles": len(report.Routes),
				"routes_recomputed": report.RoutesRecomputed, "routes_reused": report.RoutesReused, "routes_evaluated_cycle": report.RoutesEvaluated,
				"quotes": after.Quotes - before.Quotes, "quote_failures": quoteFailed,
				"rejected_min_profit": rejected, "profitable_candidates": found, "stale_candidates": stale,
				"candidate_lag_1_admitted":       after.CandidateLag1Admitted - before.CandidateLag1Admitted,
				"candidate_lag_2_admitted":       after.CandidateLag2Admitted - before.CandidateLag2Admitted,
				"candidate_stale_rejected":       after.CandidateStaleRejected - before.CandidateStaleRejected,
				"max_candidate_staleness_blocks": cfg.MaxCandidateStalenessBlocks,
				"dirty_pools":                    report.DirtyPools, "block_to_cycle_start_ms": blockToStart.Milliseconds(), "duration_ms": duration.Milliseconds(),
				"quote_duration_ms": report.QuoteDuration.Milliseconds(), "optimizer_duration_ms": report.OptimizerDuration.Milliseconds(),
				"optimizer_runs": report.OptimizerRuns, "optimizer_samples": report.OptimizerSamples, "cycle_lag_blocks": lag,
				"blocks_coalesced": coalesced, "rpc_calls": rpcCalls,
				"rpc_calls_by_stage":                 stringUintMapString(report.RPCCallsByStage),
				"rpc_calls_pool_refresh":             report.RPCCallsPoolRefresh,
				"rpc_calls_initial_quotes":           report.RPCCallsInitialQuotes,
				"rpc_calls_optimizer":                report.RPCCallsOptimizer,
				"rpc_calls_economics":                report.RPCCallsEconomics,
				"quote_cache_hits":                   report.QuoteCacheHits,
				"quote_cache_misses":                 report.QuoteCacheMisses,
				"quote_dedup_hits":                   report.QuoteDedupHits,
				"quote_cache_invalidations":          report.QuoteCacheInvalidations,
				"optimizer_samples_requested":        report.OptimizerRequested,
				"optimizer_samples_executed":         report.OptimizerSamples,
				"optimizer_samples_saved":            report.OptimizerSaved,
				"routes_deep_optimized":              report.RoutesDeepOptimized,
				"routes_probe_only":                  report.RoutesProbeOnly,
				"routes_skipped_deep_optimization":   report.RoutesSkippedDeep,
				"routes_skipped_by_prequote":         report.RoutesSkippedByPreQuote,
				"rpc_per_evaluated_route":            fmt.Sprintf("%.2f", report.RPCPerEvaluatedRoute),
				"rpc_per_optimizer_route":            fmt.Sprintf("%.2f", report.RPCPerOptimizerRoute),
				"avg_rps":                            fmt.Sprintf("%.2f", rolling.AverageRPS),
				"cycle_rps":                          fmt.Sprintf("%.2f", avgRPS),
				"p95_rps":                            fmt.Sprintf("%.2f", rolling.P95RPS),
				"p95_cycle_duration_ms":              rolling.P95CycleMS,
				"score_decile_economics":             report.ScoreDeciles,
				"active_universe_assets":             strings.Join(report.UniverseAssets, ","),
				"added_dynamic_assets":               strings.Join(report.DynamicAssets, ","),
				"universe_decisions":                 strings.Join(report.UniverseDecisions, "; "),
				"route_count_before":                 report.RouteCountBefore,
				"route_count_after":                  report.RouteCountAfter,
				"cycles_by_hop":                      intMapString(report.RoutesByHop),
				"dex_diversity":                      stringIntMapString(report.DEXRoutes),
				"top_near_misses":                    report.TopNearMisses,
				"route_score_distribution":           stringIntMapString(report.RouteScores),
				"optimizer_budget_allocation":        stringIntMapString(report.OptimizerBudget),
				"rejection_reasons":                  stringIntMapString(report.RejectionReasons),
				"prequote_ranking_enabled":           report.PreQuoteRanking,
				"exploit_selected":                   report.ExploitSelected,
				"explore_selected":                   report.ExploreSelected,
				"new_selected":                       report.NewSelected,
				"memory_routes":                      report.MemoryRoutes,
				"avg_pre_quote_score":                report.AvgPreQuoteScore,
				"cross_venue_evaluated":              report.CrossVenueQuoted,
				"same_dex_evaluated":                 report.SameDEXQuoted,
				"routes_considered":                  report.RoutesConsidered,
				"routes_quoted":                      report.RoutesEvaluated,
				"quote_age_blocks":                   report.QuoteAgeBlocks,
				"quote_budget":                       report.QuoteBudget,
				"quote_budget_used":                  report.QuoteBudgetUsed,
				"quote_budget_dropped":               report.QuoteBudgetDropped,
				"no_quote_reason":                    report.NoQuoteReason,
				"active_rpc_provider":                rpcClient.ActiveProvider(),
				"active_wss_provider":                w.ActiveProvider(),
				"rpc_provider_health":                rpcProviderHealthString(rpcClient.Snapshots()),
				"rpc_tier_traffic":                   rpcTierTrafficString(rpcClient.Snapshots()),
				"rpc_provider_stage_usage":           rpcProviderStageString(rpcClient.Snapshots()),
				"reconciliation_pending":             report.ReconcilePending,
				"reconciliation_batch_size":          report.ReconcileBatchSize,
				"reconciliation_pairs_done":          report.ReconcilePairsDone,
				"reconciliation_pairs_total":         report.ReconcilePairsTotal,
				"reconciliation_units_done":          report.ReconcileUnitsDone,
				"reconciliation_units_total":         report.ReconcileUnitsTotal,
				"reconciliation_rpc_calls":           report.ReconcileRPC,
				"reconciliation_duration_ms":         report.ReconcileDuration.Milliseconds(),
				"reconciliation_completed":           report.ReconcileCompleted,
				"reconciliation_error":               report.ReconcileError,
				"reconciliation_units_this_cycle":    report.ReconcileUnitsThisCycle,
				"reconciliation_failures_this_cycle": report.ReconcileFailures,
				"reconciliation_total_duration_ms":   report.ReconcileTotalDuration.Milliseconds(),
				"reconciliation_failed_units":        report.ReconcileFailedUnits,
				"reconciliation_discovered_pools":    report.ReconcileDiscoveredPools,
				"reconciliation_generated_routes":    report.ReconcileGeneratedRoutes,
				"route_cache_ready":                  report.RouteCacheReady,
				"route_cache_size":                   report.RouteCacheSize,
				"getlogs_chunks_attempted":           report.GetLogsChunksAttempted,
				"getlogs_chunks_succeeded":           report.GetLogsChunksSucceeded,
				"getlogs_chunks_failed":              report.GetLogsChunksFailed,
				"getlogs_blocks_scanned":             report.GetLogsBlocksScanned,
				"refresh_pools_attempted":            report.RefreshPoolsAttempted,
				"refresh_pools_succeeded":            report.RefreshPoolsSucceeded,
				"refresh_pools_failed":               report.RefreshPoolsFailed,
				"unresolved_dirty_pools":             report.UnresolvedDirtyPools,
				"routes_skipped_unresolved_refresh":  report.RoutesSkippedUnresolved,
				"market_state_incomplete":            report.MarketStateIncomplete,
			}
			if pairService != nil {
				pairStats := pairService.Snapshot()
				mode, tracked, shadow, topScore := pairService.Memory.Summary()
				fields["pair_intelligence_mode"] = mode
				fields["pair_candidates_tracked"] = tracked
				fields["pair_shadow_count"] = shadow
				fields["pair_top_score"] = topScore
				fields["pair_factory_pools"] = pairStats.FactoryPools
				fields["pair_duplicate_events"] = pairStats.DuplicateEvents
				fields["pair_rejections"] = pairStats.Rejections
				fields["pair_depth_probes"] = pairStats.DepthProbes
			}
			log.Event(logger.Info, "market_cycle", "market", "market cycle complete", fields)
			publish(operationSink, observability.Performance, "market_cycle", telegram.Info, "market cycle complete", fields)
		})
		if executionPipeline != nil {
			executionPipeline.SetLatestBlockSource(marketScheduler.LatestBlock)
		}
		go marketScheduler.Run(ctx)
	}
	if marketEngine != nil && executionPipeline != nil {
		forcedConfig, forcedErr := loadForcedTradeConfig(operationsDir, runtimeConfigPath)
		if forcedErr != nil {
			log.Event(logger.Error, "forced_trade_failed", "execution", forcedErr.Error(), nil)
			publish(operationSink, observability.Errors, "forced_trade_failed", telegram.Error, forcedErr.Error(), nil)
		} else if forcedConfig.Enabled {
			go runForcedTrade(ctx, forcedConfig, marketEngine, executionPipeline, rpcClient, operationSink, log)
		}
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
			if pairService != nil {
				mode, tracked, shadow, topScore := pairService.Memory.Summary()
				publish(operationSink, observability.Performance, "pair_intelligence_snapshot", telegram.Info, "shadow pair intelligence snapshot", map[string]any{"mode": mode, "tracked_pairs": tracked, "shadow_pairs": shadow, "top_score": topScore, "pairs": pairService.Memory.Telemetry(16)})
			}
		}
	}
}

func buildMarketEngine(client *rpc.Client, metrics *metrics.Metrics, operationsDir string, busy *atomic.Bool) (*market.Engine, config.MarketConfig, *pairintel.Service, error) {
	if os.Getenv("TITANARB_MARKET_ENGINE") != "true" {
		return nil, config.MarketConfig{}, nil, nil
	}
	coreMarketConfig, err := loadMarketConfig()
	if err != nil {
		return nil, config.MarketConfig{}, nil, err
	}
	amount, err := rawEnv("TITANARB_MARKET_AMOUNT_RAW")
	if err != nil {
		return nil, config.MarketConfig{}, nil, err
	}
	minimum, err := rawEnv("TITANARB_MARKET_MIN_PROFIT_RAW")
	if err != nil {
		return nil, config.MarketConfig{}, nil, err
	}
	coreDiscoverer := pools.NewDiscoverer(client, coreMarketConfig.UniswapFactory, coreMarketConfig.CamelotFactory, coreMarketConfig.UniswapFeeTiers)
	coreUni := quotes.NewUniswapV3(client, coreMarketConfig.UniswapQuoterV2, metrics)
	coreCamelot := quotes.NewCamelot(client, coreMarketConfig.CamelotQuoter, metrics)
	coreAmounts := marketAmounts(coreMarketConfig, amount)
	marketConfig, dynamicAssets, universeDecisions, universeManager := marketConfigWithApprovedDynamicAssets(coreMarketConfig, coreDiscoverer, client, coreUni, coreCamelot, coreAmounts)
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
	marketCost := opportunity.NewCachedCostModel(opportunity.ArbitrumCostModel{Service: feeService}, 10*time.Second)
	evaluator := opportunity.New(client, marketConfig, uni, camelot, marketCost, minimum, quoteWorkers, metrics)
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

	engine := market.NewWithAmounts(marketConfig, discoverer, cache.NewPoolCache(metrics), evaluator, coreAmounts, 4, metrics, liquidityProvider)
	engine.SetUniverseTelemetry(assetSymbols(marketConfig, marketConfig.ExecutionAssets()), assetSymbols(marketConfig, dynamicAssets), universeDecisions)
	engine.SetUniverseFeedback(func(asset string, evaluations, useful uint64) {
		universeManager.ApplyFeedback(universe.Feedback{Asset: asset, Evaluations: evaluations, Useful: useful})
	})
	pairCfg := pairintel.FromEnv()
	pairPath := filepath.Join(operationsDir, "pair_intelligence.json")
	pairMemory, loadErr := pairintel.Load(pairPath, pairCfg)
	if loadErr != nil {
		pairMemory = pairintel.NewMemory(pairCfg)
	}
	coreSet := assetSet(coreMarketConfig.ExecutionAssets())
	for _, token := range marketConfig.Tokens {
		_, core := coreSet[token.Symbol]
		_ = pairMemory.RegisterToken(pairintel.TokenMeta{Address: token.Address, Decimals: token.Decimals, HasCode: true, Core: core, Symbol: token.Symbol})
	}
	pairService := pairintel.NewService(pairMemory, client, discoverer, []pairintel.Factory{
		{Name: "uniswap_v3", Address: marketConfig.UniswapFactory, DEX: pools.UniswapV3},
		{Name: "camelot_v3", Address: marketConfig.CamelotFactory, DEX: pools.CamelotV3},
	}, func(pool pools.Pool) quotes.Quoter {
		if pool.DEX == pools.UniswapV3 {
			return uni
		}
		if pool.DEX == pools.CamelotV3 {
			return camelot
		}
		return nil
	}, pairPath, func() bool { return busy != nil && busy.Load() })
	pairService.OnAdmission = func(snapshot pairintel.AdmissionSnapshot) {
		next := cloneMarketConfig(marketConfig)
		pairKeys := make([]string, 0, len(snapshot.Pairs))
		for _, pair := range snapshot.Pairs {
			pairKeys = append(pairKeys, pair.Key.String())
		}
		dynamicSymbols := make([]string, 0, len(snapshot.Tokens))
		for _, meta := range snapshot.Tokens {
			symbol := meta.Symbol
			if symbol == "" {
				symbol = pairintel.DynamicSymbol(meta.Address)
			}
			if symbol == "" {
				continue
			}
			next.Tokens[symbol] = config.Token{Symbol: symbol, Address: meta.Address, Decimals: meta.Decimals}
			next.MarketAssetNames = append(next.MarketAssetNames, symbol)
			dynamicSymbols = append(dynamicSymbols, symbol)
		}
		engine.QueueLiveMarket(next, pairKeys, dynamicSymbols)
	}
	crossShare := 5000
	if raw := strings.TrimSpace(os.Getenv("PAIR_2HOP_CROSS_VENUE_SHARE_BPS")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil {
			crossShare = v
		}
	}
	engine.SetPairRouting(pairMemory.PairScore, crossShare)
	for symbol, amount := range coreAmounts {
		if token, ok := marketConfig.Tokens[symbol]; ok {
			pairService.SetRepresentativeAmount(token.Address, amount)
		}
	}
	discoverer.SetObserver(pairService)
	return engine, coreMarketConfig, pairService, nil
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

func marketConfigWithApprovedDynamicAssets(core config.MarketConfig, discoverer universe.PairDiscoverer, caller universe.ChainCaller, uni, camelot quotes.Quoter, amounts map[string]*big.Int) (config.MarketConfig, []string, []string, *universe.Manager) {
	managerTokens := make(map[string]config.Token)
	for _, symbol := range core.ExecutionAssets() {
		if token, ok := core.Tokens[symbol]; ok {
			managerTokens[symbol] = token
		}
	}
	registry := universe.New(managerTokens)
	manager := universe.NewManager(core.ExecutionAssets(), universe.DefaultPolicy())
	report, err := (universe.Scanner{
		Market:     core,
		Discoverer: discoverer,
		Caller:     caller,
		Quoter: func(pool pools.Pool) quotes.Quoter {
			switch pool.DEX {
			case pools.UniswapV3:
				return uni
			case pools.CamelotV3:
				return camelot
			default:
				return nil
			}
		},
		AmountRaw: amounts,
	}).Scan(context.Background())
	var decision universe.Decision
	if err == nil {
		decision = manager.Select(report)
	}
	addedSymbols := decision.AddedAssets
	if len(addedSymbols) == 0 {
		// Deterministic bootstrap: USDC.e remains the first operator-approved
		// market-only dynamic candidate if a startup scan cannot complete.
		addedSymbols = approvedDynamicUniverseCandidates(core)
	}
	var added []string
	for _, symbol := range addedSymbols {
		token, ok := tokenBySymbol(core, symbol)
		if !ok {
			continue
		}
		registry.AddToken(symbol, token)
		added = append(added, symbol)
	}
	expanded := core
	expanded.MarketAssetNames = registry.TokenNames()
	return expanded, added, universeDecisionStrings(decision, err), manager
}

func approvedDynamicUniverseCandidates(marketConfig config.MarketConfig) []string {
	for key, token := range marketConfig.Tokens {
		if key == "USDC_E_BRIDGED_ALTERNATIVE" || token.Symbol == "USDC.e" {
			return []string{key}
		}
	}
	return nil
}

func tokenBySymbol(marketConfig config.MarketConfig, symbol string) (config.Token, bool) {
	if token, ok := marketConfig.Tokens[symbol]; ok {
		return token, true
	}
	for key, token := range marketConfig.Tokens {
		if token.Symbol == symbol {
			return token, key != ""
		}
	}
	return config.Token{}, false
}

func universeDecisionStrings(decision universe.Decision, scanErr error) []string {
	var out []string
	if scanErr != nil {
		out = append(out, "scan_failed:"+fmt.Sprintf("%T", scanErr))
	}
	for _, item := range decision.Candidates {
		out = append(out, fmt.Sprintf("%s=%s(score=%.2f,reason=%s)", item.Symbol, item.Action, item.Score, item.Reason))
	}
	for _, asset := range decision.RemovedAssets {
		out = append(out, asset+"=removed")
	}
	sort.Strings(out)
	return out
}

func assetSet(assets []string) map[string]struct{} {
	out := make(map[string]struct{}, len(assets))
	for _, asset := range assets {
		out[asset] = struct{}{}
	}
	return out
}

func assetSymbols(marketConfig config.MarketConfig, assets []string) []string {
	out := make([]string, 0, len(assets))
	for _, asset := range assets {
		if token, ok := marketConfig.Tokens[asset]; ok && token.Symbol != "" {
			out = append(out, token.Symbol)
		} else {
			out = append(out, asset)
		}
	}
	sort.Strings(out)
	return out
}

func routeUsesOnlyAssets(route []string, allowed map[string]struct{}) bool {
	if len(route) == 0 {
		return false
	}
	for _, asset := range route {
		if _, ok := allowed[asset]; !ok {
			return false
		}
	}
	return true
}

func routeStartsAndEndsWithExecutionAsset(route []string, allowed map[string]struct{}) bool {
	if len(route) < 2 || route[0] != route[len(route)-1] {
		return false
	}
	_, ok := allowed[route[0]]
	return ok
}

func cloneMarketConfig(in config.MarketConfig) config.MarketConfig {
	out := in
	out.ExecutionAssetNames = append([]string(nil), in.ExecutionAssetNames...)
	out.MarketAssetNames = append([]string(nil), in.MarketAssetNames...)
	out.IntermediateTokens = append([]string(nil), in.IntermediateTokens...)
	out.UniswapFeeTiers = append([]uint32(nil), in.UniswapFeeTiers...)
	out.Tokens = make(map[string]config.Token, len(in.Tokens))
	for k, v := range in.Tokens {
		out.Tokens[k] = v
	}
	return out
}

func opportunityTelemetry(opp *opportunity.Opportunity) map[string]any {
	fields := map[string]any{
		"route":                       opp.Route.String(),
		"source_block":                opp.SourceBlock,
		"amount_in":                   decimalString(opp.AmountIn),
		"amount_out":                  decimalString(opp.AmountOut),
		"gross_profit":                decimalString(opp.GrossProfit),
		"aave_premium":                decimalString(opp.AavePremium),
		"l2_fee":                      decimalString(opp.GasEstimate),
		"l1_data_fee":                 decimalString(opp.L1DataFee),
		"expected_profit":             decimalString(opp.ExpectedProfit),
		"net_profit":                  decimalString(opp.ExpectedProfit),
		"min_profit":                  decimalString(opp.MinProfit),
		"min_profit_threshold":        decimalString(opp.MinProfit),
		"distance_from_profitability": decimalString(nearmiss.Gap(opp.ExpectedProfit, opp.MinProfit)),
		"price_impact_bps":            nearmiss.PriceImpactBPS(opp.AmountIn, opp.AmountOut),
		"hop_count":                   len(opp.Hops),
		"dex_path":                    dexPath(opp),
	}
	return fields
}

func mergeFields(dst map[string]any, src map[string]any) {
	for key, value := range src {
		dst[key] = value
	}
}

func decimalString(value *big.Int) string {
	if value == nil {
		return "0"
	}
	return value.String()
}

func dexPath(opp *opportunity.Opportunity) string {
	parts := make([]string, 0, len(opp.Hops))
	for _, hop := range opp.Hops {
		parts = append(parts, string(hop.Pool.DEX))
	}
	return strings.Join(parts, " -> ")
}

func intMapString(values map[int]int) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, fmt.Sprintf("%d:%d", key, values[key]))
	}
	return strings.Join(out, ",")
}

func stringIntMapString(values map[string]int) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, fmt.Sprintf("%s:%d", key, values[key]))
	}
	return strings.Join(out, ",")
}

func stringUintMapString(values map[string]uint64) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, fmt.Sprintf("%s:%d", key, values[key]))
	}
	return strings.Join(out, ",")
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

func marketSearchOptions(settings runtimeconfig.Settings) market.SearchOptions {
	depth := settings.OptimizerDepth
	if depth < 2 {
		depth = 2
	}
	if depth > 32 {
		depth = 32
	}
	return market.SearchOptions{
		EvaluationRoutesPerAsset: clampInt(depth*4, 8, 96),
		OptimizerRoutesPerAsset:  clampInt(depth/2, 2, 16),
		OptimizerSamplesPerRoute: envIntBounded("HIGH_SCORE_MAX_SAMPLES", clampInt(depth/2, 3, 5), 2, 16),
		OptimizerSamplesPerCycle: optimizerSampleBudget(),
		MaxQuotesPerCycle:        envIntBounded("MAX_QUOTES_PER_CYCLE", 40, 4, 256),
		MaxOptimizedRoutes:       envIntBounded("MAX_OPTIMIZED_ROUTES_PER_CYCLE", 8, 1, 32),
		NormalOptimizerSamples:   envIntBounded("NORMAL_MAX_SAMPLES", 3, 2, 16),
		FullReconcileEvery:       uint64(envIntBounded("FULL_RECONCILE_EVERY_CYCLES", 2_400, 240, 100_000)),
		ReconcileBatchPairs:      reconcileBatchUnits(),
		DisablePreQuoteRanking:   strings.EqualFold(strings.TrimSpace(os.Getenv("PREQUOTE_RANKING_ENABLED")), "false"),
		ExploreRatioBPS:          preQuoteExploreBPS(),
		PersistentQuoteCache:     envEnabled("QUOTE_CACHE_ENABLED", true),
		AdaptiveOptimizer:        envEnabled("ADAPTIVE_OPTIMIZER_ENABLED", true),
		EarlyStop:                envEnabled("RPC_EARLY_STOP_ENABLED", true),
		OptimizationFlagsSet:     true,
	}
}

func reconcileBatchUnits() int {
	if raw := strings.TrimSpace(os.Getenv("TITANARB_RECONCILE_BATCH_UNITS")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil {
			return clampInt(value, 1, 8)
		}
	}
	return 4
}

func envIntBounded(name string, fallback, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return clampInt(value, min, max)
}

func blockBeyondTolerance(source, latest, tolerance uint64) bool {
	return latest > source && latest-source > tolerance
}

func cancelWhenSuperseded(parent context.Context, latest func() uint64, source, tolerance uint64) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	if latest == nil || source == 0 {
		return ctx, cancel
	}
	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if blockBeyondTolerance(source, latest(), tolerance) {
					cancel()
					return
				}
			}
		}
	}()
	return ctx, cancel
}

func optimizerSampleBudget() int {
	raw := strings.TrimSpace(os.Getenv("OPTIMIZER_SAMPLE_BUDGET"))
	if raw == "" {
		return 32
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 32
	}
	return clampInt(value, 2, 512)
}

func envEnabled(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}

func rpcProviderConfigs(cfg config.Config) []rpc.ProviderConfig {
	if len(cfg.RPCProviders) == 0 {
		return []rpc.ProviderConfig{{Name: "primary", HTTP: cfg.HTTPRPCURL}}
	}
	out := make([]rpc.ProviderConfig, 0, len(cfg.RPCProviders))
	for _, provider := range cfg.RPCProviders {
		if strings.TrimSpace(provider.HTTP) == "" {
			continue
		}
		out = append(out, rpc.ProviderConfig{Name: provider.Name, HTTP: provider.HTTP, MaxRPS: provider.MaxRPS, TargetRPS: provider.TargetRPS, Burst: provider.Burst, MaxBlockLag: provider.MaxBlockLag, Tier: provider.Tier})
	}
	if len(out) == 0 {
		out = append(out, rpc.ProviderConfig{Name: "primary", HTTP: cfg.HTTPRPCURL})
	}
	return out
}

func wssEndpoints(cfg config.Config) []ws.Endpoint {
	if len(cfg.RPCProviders) == 0 {
		return []ws.Endpoint{{Name: "primary", URL: cfg.WSRPCURL}}
	}
	out := make([]ws.Endpoint, 0, len(cfg.RPCProviders))
	for _, provider := range cfg.RPCProviders {
		if strings.TrimSpace(provider.WSS) == "" {
			continue
		}
		out = append(out, ws.Endpoint{Name: provider.Name, URL: provider.WSS})
	}
	if len(out) == 0 {
		out = append(out, ws.Endpoint{Name: "primary", URL: cfg.WSRPCURL})
	}
	return out
}

func rpcProviderHealthString(snapshots []rpc.ProviderSnapshot) string {
	if len(snapshots) == 0 {
		return ""
	}
	parts := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		state := "healthy"
		if !snapshot.Healthy {
			state = "cooldown"
		}
		active := ""
		if snapshot.Active {
			active = ":active"
		}
		parts = append(parts, fmt.Sprintf("%s=%s%s,tier=%s,block=%d,latency_ms=%d,latency_p95_ms=%d,requests=%d,eth_calls=%d,quote_requests=%d,success_pct=%.1f,share_pct=%.1f,target_rps=%d,burst=%d,inflight=%d,instantaneous_rps=%.1f,rate_limited=%d,forbidden=%d,cooldown_events=%d,cooldown_remaining_ms=%d,probation_remaining_ms=%d,failures=%d", snapshot.Name, state, active, snapshot.Tier, snapshot.LatestBlock, snapshot.Latency.Milliseconds(), snapshot.LatencyP95.Milliseconds(), snapshot.Requests, snapshot.EthCallRequests, snapshot.QuoteRequests, snapshot.SuccessRatePct, snapshot.SharePct, snapshot.TargetRPS, snapshot.Burst, snapshot.Inflight, snapshot.InstantaneousRPS, snapshot.RateLimited, snapshot.Forbidden, snapshot.CooldownEvents, snapshot.CooldownRemaining.Milliseconds(), snapshot.ProbationRemaining.Milliseconds(), snapshot.Failures))
	}
	return strings.Join(parts, ";")
}

func rpcTierTrafficString(snapshots []rpc.ProviderSnapshot) string {
	counts := make(map[string]uint64)
	var total uint64
	for _, snapshot := range snapshots {
		counts[snapshot.Tier] += snapshot.Requests
		total += snapshot.Requests
	}
	if total == 0 {
		return "premium:0.0,secondary:0.0,limited:0.0,emergency:0.0"
	}
	parts := make([]string, 0, 4)
	for _, tier := range []string{"premium", "secondary", "limited", "emergency"} {
		parts = append(parts, fmt.Sprintf("%s:%.1f", tier, float64(counts[tier])*100/float64(total)))
	}
	return strings.Join(parts, ",")
}

func rpcProviderStageString(snapshots []rpc.ProviderSnapshot) string {
	parts := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		stages := make([]string, 0, len(snapshot.RequestsByStage))
		for stage, count := range snapshot.RequestsByStage {
			stages = append(stages, fmt.Sprintf("%s:%d", stage, count))
		}
		sort.Strings(stages)
		parts = append(parts, snapshot.Name+"="+strings.Join(stages, ","))
	}
	return strings.Join(parts, ";")
}

func safeRPCMessage(message string) string {
	message = strings.TrimSpace(message)
	if strings.Contains(message, "://") {
		return "[redacted]"
	}
	if len(message) > 240 {
		return message[:240]
	}
	return message
}

func safeRPCField(value string) string {
	value = strings.TrimSpace(value)
	if strings.Contains(value, "://") {
		return "[redacted]"
	}
	if len(value) > 240 {
		return value[:240]
	}
	return value
}

func preQuoteExploreBPS() int {
	raw := strings.TrimSpace(os.Getenv("PREQUOTE_EXPLORE_BPS"))
	if raw == "" {
		return 2_000
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 2_000
	}
	return clampInt(value, 0, 5_000)
}

func clampInt(value, minimum, maximum int) int {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
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

type forcedTradeConfig struct {
	Enabled    bool
	Asset      string
	Amount     *big.Int
	Route      []string
	MarkerPath string
}

type forcedOpportunityBuilder interface {
	ForcedOpportunity(context.Context, uint64, []string, *big.Int) (*opportunity.Opportunity, error)
}

type forcedExecutionPipeline interface {
	ProcessWithObserver(context.Context, *opportunity.Opportunity, execution.Observer) execution.Outcome
}

type forcedBlockReader interface {
	BlockNumber(context.Context) (uint64, error)
}

func loadForcedTradeConfig(observabilityDir, runtimeConfigPath string) (forcedTradeConfig, error) {
	if !truthy(os.Getenv("FORCE_ONE_TRADE")) {
		return forcedTradeConfig{}, nil
	}
	asset := strings.TrimSpace(os.Getenv("FORCE_TRADE_ASSET"))
	if asset == "" {
		return forcedTradeConfig{}, fmt.Errorf("FORCE_TRADE_ASSET is required when FORCE_ONE_TRADE=true")
	}
	amount, err := rawEnv("FORCE_TRADE_AMOUNT")
	if err != nil || amount.Sign() <= 0 {
		return forcedTradeConfig{}, fmt.Errorf("FORCE_TRADE_AMOUNT must be a positive raw token amount")
	}
	route := parseForcedRoute(os.Getenv("FORCE_TRADE_ROUTE"))
	if len(route) < 3 || len(route) > 5 {
		return forcedTradeConfig{}, fmt.Errorf("FORCE_TRADE_ROUTE must contain a 2-4 hop cycle")
	}
	if route[0] != asset || route[len(route)-1] != asset {
		return forcedTradeConfig{}, fmt.Errorf("FORCE_TRADE_ROUTE must start and end with FORCE_TRADE_ASSET")
	}
	return forcedTradeConfig{Enabled: true, Asset: asset, Amount: amount, Route: route, MarkerPath: forcedTradeMarkerPath(observabilityDir, runtimeConfigPath)}, nil
}

func runForcedTrade(ctx context.Context, cfg forcedTradeConfig, engine forcedOpportunityBuilder, pipeline forcedExecutionPipeline, client forcedBlockReader, sink *operations.Sink, log *logger.Logger) {
	fields := map[string]any{"asset": cfg.Asset, "amount": cfg.Amount.String(), "route": strings.Join(cfg.Route, ",")}
	log.Event(logger.Info, "forced_trade_requested", "execution", "one-shot forced trade requested", fields)
	publish(sink, observability.Trades, "forced_trade_requested", telegram.Info, "one-shot forced trade requested", fields)

	consumed, err := consumeForcedTrade(cfg.MarkerPath, fields)
	if err != nil {
		failForcedTrade(sink, log, "forced_trade_failed", "failed to persist forced trade marker: "+err.Error(), fields)
		return
	}
	if !consumed {
		log.Event(logger.Info, "forced_trade_skipped", "execution", "one-shot forced trade already consumed", fields)
		publish(sink, observability.Trades, "forced_trade_skipped", telegram.Info, "one-shot forced trade already consumed", fields)
		return
	}

	stateBlock, err := client.BlockNumber(ctx)
	if err != nil {
		failForcedTrade(sink, log, "forced_trade_failed", "latest block unavailable: "+err.Error(), fields)
		return
	}
	opp, err := engine.ForcedOpportunity(ctx, stateBlock, cfg.Route, cfg.Amount)
	if err != nil {
		failForcedTrade(sink, log, "forced_trade_failed", err.Error(), fields)
		return
	}
	mergeFields(fields, opportunityTelemetry(opp))
	fields["source_block"] = opp.SourceBlock

	outcome := pipeline.ProcessWithObserver(ctx, opp, func(event string, eventFields map[string]any) {
		merged := cloneAnyMap(fields)
		mergeFields(merged, eventFields)
		log.Event(logger.Info, event, "execution", event, merged)
		publish(sink, observability.Trades, event, telegram.Info, event, merged)
	})

	resultFields := cloneAnyMap(fields)
	resultFields["decision"] = outcome.Decision
	resultFields["reason"] = outcome.Reason
	resultFields["tx_hash"] = outcome.TxHash
	if outcome.FinalProfit != nil {
		resultFields["final_net_profit"] = outcome.FinalProfit.String()
	}
	if outcome.Simulation != nil {
		resultFields["gas_estimate"] = outcome.Simulation.GasEstimate
	}
	if outcome.Receipt != nil {
		resultFields["receipt_status"] = outcome.Receipt.Status
		resultFields["gas_used"] = outcome.Receipt.GasUsed
	}
	if outcome.TxHash != "" {
		log.Event(logger.Info, "forced_trade_submitted", "execution", "forced trade submitted", resultFields)
		publish(sink, observability.Trades, "forced_trade_submitted", telegram.Info, "forced trade submitted", resultFields)
	}
	if outcome.Decision == "confirmed" {
		log.Event(logger.Info, "forced_trade_confirmed", "execution", "forced trade confirmed", resultFields)
		publish(sink, observability.Trades, "forced_trade_confirmed", telegram.Info, "forced trade confirmed", resultFields)
		return
	}
	failForcedTrade(sink, log, "forced_trade_failed", outcome.Reason, resultFields)
}

func failForcedTrade(sink *operations.Sink, log *logger.Logger, event, reason string, fields map[string]any) {
	copyFields := cloneAnyMap(fields)
	copyFields["reason"] = reason
	log.Event(logger.Warn, event, "execution", reason, copyFields)
	publish(sink, observability.Trades, event, telegram.Warning, reason, copyFields)
}

func consumeForcedTrade(path string, fields map[string]any) (bool, error) {
	if strings.TrimSpace(path) == "" {
		return false, fmt.Errorf("forced trade marker path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()
	return true, json.NewEncoder(file).Encode(map[string]any{"consumed_at": time.Now().UTC(), "fields": fields})
}

func forcedTradeMarkerPath(observabilityDir, runtimeConfigPath string) string {
	base := strings.TrimSpace(observabilityDir)
	if base == "" && strings.TrimSpace(runtimeConfigPath) != "" {
		base = filepath.Dir(runtimeConfigPath)
	}
	if base == "" {
		base = "."
	}
	return filepath.Join(base, "force_one_trade_consumed.json")
}

func parseForcedRoute(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func truthy(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func cloneAnyMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
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
