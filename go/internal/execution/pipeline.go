package execution

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/titanarb/titanarb-go/internal/config"
	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/fees"
	"github.com/titanarb/titanarb-go/internal/metrics"
	"github.com/titanarb/titanarb-go/internal/opportunity"
	"github.com/titanarb/titanarb-go/internal/pricing"
	"github.com/titanarb/titanarb-go/internal/profit"
	"github.com/titanarb/titanarb-go/internal/rpc"
	"github.com/titanarb/titanarb-go/internal/safety"
	"github.com/titanarb/titanarb-go/internal/simulation"
	"github.com/titanarb/titanarb-go/internal/tx"
	"github.com/titanarb/titanarb-go/internal/wallet"
)

type Chain interface {
	TransactionCount(context.Context, string, string) (uint64, error)
	GasPrice(context.Context) (*big.Int, error)
	MaxPriorityFeePerGas(context.Context) (*big.Int, error)
	SendRawTransaction(context.Context, string) (string, error)
	TransactionReceipt(context.Context, string) (*rpc.Receipt, error)
	EthCall(context.Context, map[string]string) (string, error)
	EstimateGas(context.Context, rpc.CallMessage) (uint64, error)
}

type Pipeline struct {
	config                      config.Config
	market                      config.MarketConfig
	chain                       Chain
	fees                        *fees.Service
	wallet                      *wallet.Wallet
	tracker                     *profit.Tracker
	gate                        *safety.Gate
	metrics                     *metrics.Metrics
	minProfit                   *big.Int
	latestObservedBlock         func() uint64
	maxCandidateStalenessBlocks uint64
}

type Outcome struct {
	Decision    string
	Reason      string
	Request     Request
	Simulation  *simulation.Result
	FinalProfit *big.Int
	TxHash      string
	Receipt     *rpc.Receipt
}

type Observer func(event string, fields map[string]any)

func (p *Pipeline) WalletAddress() common.Address { return p.wallet.Address() }

// SetLatestBlockSource connects the execution gate to the market scheduler's
// latest observed head. It does not alter economics: it only prevents a quote
// produced from an older pool snapshot from reaching simulation or broadcast.
func (p *Pipeline) SetLatestBlockSource(source func() uint64) {
	p.latestObservedBlock = source
}

func NewPipeline(cfg config.Config, market config.MarketConfig, chain Chain, feeService *fees.Service, gate *safety.Gate, minProfit *big.Int, tracker *profit.Tracker, metricStore *metrics.Metrics) (*Pipeline, error) {
	if minProfit == nil || minProfit.Sign() <= 0 {
		return nil, fmt.Errorf("positive raw minProfit is required")
	}
	if gate == nil {
		return nil, fmt.Errorf("oracle/sequencer safety gate is required")
	}
	w, err := wallet.New(cfg.PrivateKey, chain)
	if err != nil {
		return nil, err
	}
	return &Pipeline{config: cfg, market: market, chain: chain, fees: feeService, wallet: w, tracker: tracker, gate: gate, metrics: metricStore, minProfit: new(big.Int).Set(minProfit), maxCandidateStalenessBlocks: cfg.MaxCandidateStalenessBlocks}, nil
}

// BuildRequest converts a quoted opportunity into precisely the Solidity
// SwapStep[] ABI layout and derives a non-zero per-hop minOut from the current
// configured slippage policy.
func (p *Pipeline) BuildRequest(ctx context.Context, opp *opportunity.Opportunity) (Request, error) {
	if opp == nil || len(opp.Hops) < 2 || len(opp.Hops) > 4 || len(opp.Route.Symbols) != len(opp.Hops)+1 {
		return Request{}, fmt.Errorf("invalid opportunity")
	}
	assetSymbol := opp.Route.Symbols[0]
	asset, ok := p.market.Tokens[assetSymbol]
	if !ok {
		return Request{}, fmt.Errorf("loan asset %s not configured", assetSymbol)
	}
	if opp.Route.Symbols[len(opp.Route.Symbols)-1] != assetSymbol {
		return Request{}, fmt.Errorf("route must return to its loan asset")
	}
	steps := make([]SwapStep, 0, len(opp.Hops))
	for i, hop := range opp.Hops {
		in := p.market.Tokens[opp.Route.Symbols[i]]
		out := p.market.Tokens[opp.Route.Symbols[i+1]]
		adapter := p.config.UniswapV3Adapter
		data, err := EncodeUniswapFee(hop.Fee)
		if string(hop.Pool.DEX) == "camelot_v3" {
			adapter = p.config.CamelotV3Adapter
			data = []byte{}
			err = nil
		}
		if err != nil {
			return Request{}, err
		}
		if !common.IsHexAddress(adapter) || !common.IsHexAddress(in.Address) || !common.IsHexAddress(out.Address) {
			return Request{}, fmt.Errorf("invalid configured adapter or token address")
		}
		minimum := new(big.Int).Mul(hop.AmountOut, new(big.Int).SetUint64(10_000-p.config.SlippageBPS))
		minimum.Div(minimum, big.NewInt(10_000))
		if minimum.Sign() <= 0 {
			return Request{}, fmt.Errorf("step %d minOut is zero", i)
		}
		steps = append(steps, SwapStep{Adapter: common.HexToAddress(adapter), TokenIn: common.HexToAddress(in.Address), TokenOut: common.HexToAddress(out.Address), AmountOutMinimum: minimum, Data: data})
	}
	block, err := p.latestTimestamp(ctx)
	if err != nil {
		return Request{}, err
	}
	return Request{Asset: common.HexToAddress(asset.Address), Amount: new(big.Int).Set(opp.AmountIn), Steps: steps, Deadline: new(big.Int).SetUint64(block + p.config.DeadlineSeconds), MinProfit: new(big.Int).Set(p.minProfit)}, nil
}

// Process applies every production safety gate. Broadcast requires both the
// existing DRY_RUN/live flags and GO_LIVE_EXECUTION=true; the latter
// makes migration cut-over an explicit operational decision rather than an
// accidental consequence of reusing a legacy environment file.
func (p *Pipeline) Process(ctx context.Context, opp *opportunity.Opportunity) Outcome {
	return p.ProcessWithObserver(ctx, opp, nil)
}

func (p *Pipeline) ProcessWithObserver(ctx context.Context, opp *opportunity.Opportunity, observer Observer) Outcome {
	ctx = rpc.WithRequestClass(ctx, rpc.Critical)
	if opp != nil && p.latestObservedBlock != nil {
		latestObservedBlock := p.latestObservedBlock()
		lag := candidateBlockLag(opp.SourceBlock, latestObservedBlock)
		if !candidateIsFresh(opp.SourceBlock, latestObservedBlock, p.maxCandidateStalenessBlocks) {
			if p.metrics != nil {
				p.metrics.IncCandidateStaleRejected()
			}
			return Outcome{Decision: "reject", Reason: "stale candidate: newer chain state observed"}
		}
		if p.metrics != nil {
			switch lag {
			case 1:
				p.metrics.IncCandidateLag1Admitted()
			case 2:
				p.metrics.IncCandidateLag2Admitted()
			}
		}
	}
	if err := p.gate.Check(ctx, time.Now().UTC()); err != nil {
		return Outcome{Decision: "reject", Reason: "safety gate: " + err.Error()}
	}
	req, err := p.BuildRequest(ctx, opp)
	if err != nil {
		return Outcome{Decision: "reject", Reason: err.Error()}
	}
	calldata, err := req.Calldata()
	if err != nil {
		return Outcome{Decision: "reject", Reason: err.Error(), Request: req}
	}
	if p.metrics != nil {
		p.metrics.IncSimulationAttempts()
	}
	sim, err := simulation.Simulate(ctx, p.chain, p.fees, p.wallet.Address().Hex(), p.config.FlashExecutorAddress, calldata, nil)
	if err != nil {
		if p.metrics != nil {
			p.metrics.IncSimulationFailures()
		}
		p.record("trade_failed", opp, "", nil, nil, err.Error())
		return Outcome{Decision: "reject", Reason: err.Error(), Request: req}
	}
	premium, err := p.aavePremium(ctx, req.Amount)
	if err != nil {
		return Outcome{Decision: "reject", Reason: err.Error(), Request: req, Simulation: &sim}
	}
	asset, ok := p.market.Tokens[opp.Route.Symbols[0]]
	if !ok {
		return Outcome{Decision: "reject", Reason: "loan asset not configured", Request: req, Simulation: &sim}
	}
	l2 := feeToBaseRaw(sim.Fee.L2Cost, sim.Fee.TotalUSD, sim.Fee.TotalETH, asset.Decimals)
	l1 := feeToBaseRaw(sim.Fee.L1Cost, sim.Fee.TotalUSD, sim.Fee.TotalETH, asset.Decimals)
	final := new(big.Int).Set(opp.Hops[len(opp.Hops)-1].AmountOut)
	economics := pricing.Evaluate(pricing.Inputs{AmountIn: req.Amount, AmountOut: final, AavePremium: premium, L2Fee: l2, L1DataFee: l1, MinProfit: req.MinProfit})
	if observer != nil {
		repayment := new(big.Int).Add(req.Amount, premium)
		observer("forced_trade_simulation", map[string]any{
			"result":          "pass",
			"expected_output": final.String(),
			"l2_fee":          l2.String(),
			"l1_fee":          l1.String(),
			"repayment":       repayment.String(),
			"gas_estimate":    sim.GasEstimate,
		})
	}
	if !economics.Profitable {
		if p.metrics != nil {
			p.metrics.IncPostGasRejections()
		}
		p.record("post_gas_rejected", opp, "", economics.ExpectedProfit, premium, "final gas repricing")
		return Outcome{Decision: "reject", Reason: "unprofitable after final gas repricing", Request: req, Simulation: &sim, FinalProfit: economics.ExpectedProfit}
	}
	if p.config.DryRun || p.config.ExecutionMode != "live" || !p.config.BroadcastEnabled {
		p.record("simulation_succeeded", opp, "", economics.ExpectedProfit, premium, "broadcast disabled")
		return Outcome{Decision: "simulated", Reason: "broadcast disabled", Request: req, Simulation: &sim, FinalProfit: economics.ExpectedProfit}
	}
	nonce, err := p.wallet.NextNonce(ctx)
	if err != nil {
		if p.metrics != nil {
			p.metrics.IncTransactionsFailed()
		}
		return Outcome{Decision: "reject", Reason: err.Error(), Request: req, Simulation: &sim, FinalProfit: economics.ExpectedProfit}
	}
	gasPrice, err := p.chain.GasPrice(ctx)
	if err != nil {
		p.wallet.Release(nonce)
		return Outcome{Decision: "reject", Reason: err.Error(), Request: req, Simulation: &sim, FinalProfit: economics.ExpectedProfit}
	}
	priorityFee, priorityErr := p.chain.MaxPriorityFeePerGas(ctx)
	if priorityErr != nil {
		priorityFee = big.NewInt(0)
	}
	limit := sim.GasEstimate * p.config.GasLimitMultiplierBPS / 10_000
	if observer != nil {
		observer("forced_trade_ready", map[string]any{"tx_hash": "", "gas_estimate": sim.GasEstimate, "gas_limit": limit})
	}
	hash, err := tx.Broadcast(ctx, p.chain, p.wallet, big.NewInt(p.config.ChainID), nonce, limit, gasPrice, priorityFee, p.config.FlashExecutorAddress, calldata)
	if err != nil {
		p.wallet.Release(nonce)
		p.record("trade_failed", opp, "", economics.ExpectedProfit, premium, err.Error())
		return Outcome{Decision: "reject", Reason: err.Error(), Request: req, Simulation: &sim, FinalProfit: economics.ExpectedProfit}
	}
	if p.metrics != nil {
		p.metrics.IncTransactionsBroadcast()
	}
	p.record("trade_sent", opp, hash, economics.ExpectedProfit, premium, "")
	receiptResult := tx.WaitReceipt(ctx, p.chain, hash, time.Second)
	if receiptResult.Err != nil {
		if p.metrics != nil {
			p.metrics.IncTransactionsFailed()
		}
		p.record("trade_failed", opp, hash, economics.ExpectedProfit, premium, receiptResult.Err.Error())
		return Outcome{Decision: "sent", Reason: receiptResult.Err.Error(), Request: req, Simulation: &sim, FinalProfit: economics.ExpectedProfit, TxHash: hash, Receipt: receiptResult.Receipt}
	}
	if p.metrics != nil {
		p.metrics.IncTransactionsSucceeded()
	}
	p.recordReceipt("trade_success", opp, hash, economics.ExpectedProfit, premium, receiptResult.Receipt)
	return Outcome{Decision: "confirmed", Request: req, Simulation: &sim, FinalProfit: economics.ExpectedProfit, TxHash: hash, Receipt: receiptResult.Receipt}
}

func candidateIsFresh(sourceBlock, latestObservedBlock, maxStalenessBlocks uint64) bool {
	// SourceBlock zero is retained for deterministic fixtures and callers that
	// do not yet provide block-aware pool state. Production market cycles always
	// attach the pinned pool-state block.
	return sourceBlock == 0 || candidateBlockLag(sourceBlock, latestObservedBlock) <= maxStalenessBlocks
}

func candidateBlockLag(sourceBlock, latestObservedBlock uint64) uint64 {
	if sourceBlock == 0 || latestObservedBlock <= sourceBlock {
		return 0
	}
	return latestObservedBlock - sourceBlock
}

func (p *Pipeline) aavePremium(ctx context.Context, amount *big.Int) (*big.Int, error) {
	raw, err := p.chain.EthCall(ctx, map[string]string{"to": p.market.AavePool, "data": dex.StaticCall("FLASHLOAN_PREMIUM_TOTAL()")})
	if err != nil {
		return nil, err
	}
	words, err := dex.DecodeWords(raw)
	if err != nil || len(words) < 1 {
		return nil, fmt.Errorf("Aave premium read: %w", err)
	}
	return new(big.Int).Div(new(big.Int).Mul(amount, dex.WordUint(words[0])), big.NewInt(10_000)), nil
}
func (p *Pipeline) latestTimestamp(ctx context.Context) (uint64, error) {
	// eth_getBlockByNumber is intentionally used rather than host time so
	// deadlines are evaluated against the same chain context as eth_call.
	var b struct {
		Timestamp string `json:"timestamp"`
	}
	if err := p.chainCall(ctx, "eth_getBlockByNumber", []any{"latest", false}, &b); err != nil {
		return 0, err
	}
	v := new(big.Int)
	if _, ok := v.SetString(strings.TrimPrefix(b.Timestamp, "0x"), 16); !ok {
		return 0, fmt.Errorf("invalid block timestamp")
	}
	return v.Uint64(), nil
}

type rawCaller interface {
	Call(context.Context, string, any, any) error
}

func (p *Pipeline) chainCall(ctx context.Context, method string, params any, out any) error {
	c, ok := p.chain.(rawCaller)
	if !ok {
		return fmt.Errorf("chain does not expose %s", method)
	}
	return c.Call(ctx, method, params, out)
}
func feeToBaseRaw(component *big.Int, totalUSD, totalETH *big.Rat, decimals uint8) *big.Int {
	if component == nil || component.Sign() == 0 || totalETH == nil || totalETH.Sign() == 0 {
		return big.NewInt(0)
	}
	eth := new(big.Rat).SetFrac(component, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	price := new(big.Rat).Quo(totalUSD, totalETH)
	value := new(big.Rat).Mul(eth, price)
	value.Mul(value, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)))
	return new(big.Int).Quo(value.Num(), value.Denom())
}
func (p *Pipeline) record(event string, opp *opportunity.Opportunity, hash string, expected, premium *big.Int, reason string) {
	if p.tracker == nil {
		return
	}
	route := ""
	if opp != nil {
		route = opp.Route.String()
	}
	_ = p.tracker.Write(profit.Record{Event: event, Route: route, TxHash: hash, ExpectedProfit: profit.Decimal(expected), AavePremium: profit.Decimal(premium), Reason: reason})
}

func (p *Pipeline) recordReceipt(event string, opp *opportunity.Opportunity, hash string, expected, premium *big.Int, receipt *rpc.Receipt) {
	if p.tracker == nil {
		return
	}
	record := profit.Record{Event: event, Route: opp.Route.String(), TxHash: hash, ExpectedProfit: profit.Decimal(expected), AavePremium: profit.Decimal(premium)}
	if receipt != nil {
		record.ReceiptStatus = receipt.Status
		if gasUsed, ok := hexQuantity(receipt.GasUsed); ok {
			if unit, ok := hexQuantity(receipt.EffectiveGasPrice); ok {
				record.GasSpentWei = new(big.Int).Mul(gasUsed, unit).String()
			}
		}
		if realized, ok := RealizedProfit(receipt.Logs, p.config.FlashExecutorAddress); ok {
			record.ActualProfit = realized.String()
		}
	}
	_ = p.tracker.Write(record)
}
func hexQuantity(raw string) (*big.Int, bool) {
	v := new(big.Int)
	_, ok := v.SetString(strings.TrimPrefix(raw, "0x"), 16)
	return v, ok
}
