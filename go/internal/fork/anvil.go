// Package fork contains local-Anvil-only validation helpers. It is intentionally
// separate from the runtime and execution packages so it can never be selected
// for mainnet operation.
package fork

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/titanarb/titanarb-go/internal/dex"
	"github.com/titanarb/titanarb-go/internal/rpc"
)

type Caller interface {
	Call(context.Context, string, any, any) error
}

type Controller struct{ caller Caller }

// NewController rejects every non-loopback URL. The methods below are Anvil
// dev-RPC calls and must never be usable against Arbitrum One.
func NewController(endpoint string, caller Caller) (*Controller, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || !isLoopback(u.Hostname()) {
		return nil, fmt.Errorf("fork controller requires a local http Anvil endpoint")
	}
	if caller == nil {
		return nil, fmt.Errorf("fork RPC caller is required")
	}
	return &Controller{caller: caller}, nil
}
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Controller) SetBalance(ctx context.Context, account string, wei *big.Int) error {
	if wei == nil || wei.Sign() < 0 {
		return fmt.Errorf("non-negative local balance required")
	}
	return c.devCall(ctx, "anvil_setBalance", []any{account, "0x" + wei.Text(16)})
}
func (c *Controller) Impersonate(ctx context.Context, account string) error {
	return c.devCall(ctx, "anvil_impersonateAccount", []any{account})
}
func (c *Controller) StopImpersonating(ctx context.Context, account string) error {
	return c.devCall(ctx, "anvil_stopImpersonatingAccount", []any{account})
}
func (c *Controller) Mine(ctx context.Context) error { return c.devCall(ctx, "evm_mine", []any{}) }

// SyncTimestamp advances a local Anvil fork to the host clock before a
// validation transaction. A pinned historical fork block otherwise makes a
// production-derived deadline appear expired to Anvil's pending-block
// simulator. It is a time-only dev-RPC operation; it never changes protocol,
// token, or pool state.
func (c *Controller) SyncTimestamp(ctx context.Context, unixSeconds int64) error {
	if unixSeconds <= 0 {
		return fmt.Errorf("positive local timestamp required")
	}
	if err := c.devCall(ctx, "evm_setNextBlockTimestamp", []any{fmt.Sprintf("0x%x", unixSeconds)}); err != nil {
		return err
	}
	return c.Mine(ctx)
}

// SendFromImpersonated deliberately uses eth_sendTransaction, not a signed raw
// transaction. Anvil owns the temporary signer after impersonation; attempting
// to pass this account through the production wallet signer was the former
// harness failure.
func (c *Controller) SendFromImpersonated(ctx context.Context, from, to, data string, gas uint64) (string, error) {
	return c.SendLocal(ctx, from, to, data, gas)
}

// SendLocal sends through Anvil's node-managed local accounts with an explicit
// gas limit. It deliberately never calls eth_estimateGas, which can be costly
// for state-changing real-pool conditioning simulations.
func (c *Controller) SendLocal(ctx context.Context, from, to, data string, gas uint64) (string, error) {
	transaction := map[string]string{"from": from, "to": to, "data": data}
	if gas > 0 {
		transaction["gas"] = fmt.Sprintf("0x%x", gas)
	}
	var hash string
	if err := c.caller.Call(ctx, "eth_sendTransaction", []any{transaction}, &hash); err != nil {
		return "", err
	}
	if hash == "" {
		return "", fmt.Errorf("Anvil returned an empty transaction hash")
	}
	return hash, nil
}

func (c *Controller) WaitReceipt(ctx context.Context, hash string, interval time.Duration) (*rpc.Receipt, error) {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	for {
		var receipt *rpc.Receipt
		if err := c.caller.Call(ctx, "eth_getTransactionReceipt", []any{hash}, &receipt); err != nil {
			return nil, err
		}
		if receipt != nil {
			return receipt, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// FundERC20 transfers real forked ERC-20 state from an impersonated holder to a
// local test account. It changes no token/pool storage and does not mock a
// router. Native ETH is granted only to pay local Anvil gas for that holder.
func (c *Controller) FundERC20(ctx context.Context, token, holder, recipient string, amount, holderGas *big.Int, gasLimit uint64) (*rpc.Receipt, string, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, "", fmt.Errorf("positive token amount required")
	}
	if err := c.SetBalance(ctx, holder, holderGas); err != nil {
		return nil, "", err
	}
	if err := c.Impersonate(ctx, holder); err != nil {
		return nil, "", err
	}
	defer c.StopImpersonating(context.Background(), holder)
	word, err := dex.AddressWord(recipient)
	if err != nil {
		return nil, "", err
	}
	data := dex.StaticCall("transfer(address,uint256)", word, dex.UintWord(amount))
	hash, err := c.SendFromImpersonated(ctx, holder, token, data, gasLimit)
	if err != nil {
		return nil, "", err
	}
	receipt, err := c.WaitReceipt(ctx, hash, 100*time.Millisecond)
	if err != nil {
		return nil, hash, err
	}
	return receipt, hash, nil
}

func (c *Controller) devCall(ctx context.Context, method string, params []any) error {
	var ignored any
	return c.caller.Call(ctx, method, params, &ignored)
}
