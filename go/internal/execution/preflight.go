package execution

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/titanarb/titanarb-go/internal/config"
)

// Verifier performs direct on-chain read-back before the Go execution pipeline
// is allowed to process opportunities. It has no write capability.
type Verifier interface {
	EthCall(context.Context, map[string]string) (string, error)
	GetCode(context.Context, string) (string, error)
}

func VerifyDeployment(ctx context.Context, chain Verifier, cfg config.Config, market config.MarketConfig, expectedExecutor string) error {
	for _, target := range []string{cfg.FlashExecutorAddress, cfg.UniswapV3Adapter, cfg.CamelotV3Adapter} {
		code, err := chain.GetCode(ctx, target)
		if err != nil {
			return fmt.Errorf("code %s: %w", target, err)
		}
		if code == "" || code == "0x" {
			return fmt.Errorf("no bytecode at %s", target)
		}
	}
	if err := expectAddress(ctx, chain, cfg.FlashExecutorAddress, "authorizedExecutor", expectedExecutor); err != nil {
		return err
	}
	if cfg.ProfitRecipient != "" {
		if err := expectAddress(ctx, chain, cfg.FlashExecutorAddress, "profitRecipient", cfg.ProfitRecipient); err != nil {
			return err
		}
	}
	if err := expectAddress(ctx, chain, cfg.FlashExecutorAddress, "AAVE_POOL", market.AavePool); err != nil {
		return err
	}
	if err := expectAddress(ctx, chain, cfg.UniswapV3Adapter, "ROUTER", market.UniswapSwapRouter); err != nil {
		return err
	}
	if err := expectAddress(ctx, chain, cfg.CamelotV3Adapter, "ROUTER", market.CamelotSwapRouter); err != nil {
		return err
	}
	for _, adapter := range []string{cfg.UniswapV3Adapter, cfg.CamelotV3Adapter} {
		if err := expectBool(ctx, chain, cfg.FlashExecutorAddress, "approvedAdapters", common.HexToAddress(adapter), true); err != nil {
			return err
		}
	}
	// Validate precisely the configured execution universe. The token registry
	// also contains optional discovery-only assets (for example USDC.e) that
	// are not part of this deployment's allow-list.
	symbols := market.ExecutionAssets()
	for _, symbol := range symbols {
		token, ok := market.Tokens[symbol]
		if !ok {
			return fmt.Errorf("configured execution token %s is missing", symbol)
		}
		if err := expectBool(ctx, chain, cfg.FlashExecutorAddress, "allowedTokens", common.HexToAddress(token.Address), true); err != nil {
			return err
		}
	}
	return nil
}

func expectAddress(ctx context.Context, c Verifier, to, method, expected string) error {
	contract := executorABI
	if method == "ROUTER" {
		contract = adapterABI
	}
	data, err := contract.Pack(method)
	if err != nil {
		return err
	}
	raw, err := c.EthCall(ctx, map[string]string{"to": to, "data": "0x" + hex.EncodeToString(data)})
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	values, err := contract.Unpack(method, common.FromHex(raw))
	if err != nil || len(values) < 1 {
		return fmt.Errorf("%s invalid response", method)
	}
	got := values[0].(common.Address).Hex()
	if !strings.EqualFold(got, expected) {
		return fmt.Errorf("%s mismatch: got %s", method, got)
	}
	return nil
}

func expectBool(ctx context.Context, c Verifier, to, method string, arg common.Address, expected bool) error {
	data, err := executorABI.Pack(method, arg)
	if err != nil {
		return err
	}
	raw, err := c.EthCall(ctx, map[string]string{"to": to, "data": "0x" + hex.EncodeToString(data)})
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	values, err := executorABI.Unpack(method, common.FromHex(raw))
	if err != nil || len(values) < 1 {
		return fmt.Errorf("%s invalid response", method)
	}
	if got := values[0].(bool); got != expected {
		return fmt.Errorf("%s mismatch", method)
	}
	return nil
}
