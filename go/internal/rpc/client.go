// Package rpc implements the read-only Arbitrum JSON-RPC foundation client.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/titanarb/titanarb-go/internal/metrics"
)

type ErrorKind string

const (
	Network ErrorKind = "network"
	Timeout ErrorKind = "timeout"
	RPC     ErrorKind = "rpc"
	HTTP    ErrorKind = "http"
)

type Error struct {
	Kind ErrorKind
	Err  error
}

func (e *Error) Error() string { return e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

type Client struct {
	endpoint string
	http     *http.Client
	retries  int
	metrics  *metrics.Metrics
}

func New(endpoint string, timeout time.Duration, retries int, m *metrics.Metrics) *Client {
	return &Client{endpoint: endpoint, http: &http.Client{Timeout: timeout}, retries: retries, metrics: m}
}

type Block struct {
	Number    string `json:"number"`
	Hash      string `json:"hash"`
	Timestamp string `json:"timestamp"`
}

// CallMessage is the JSON-RPC transaction shape used by eth_call and
// eth_estimateGas. Every field is hexadecimal when present.
type CallMessage struct {
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
	Data  string `json:"data,omitempty"`
	Value string `json:"value,omitempty"`
	Gas   string `json:"gas,omitempty"`
}

// Receipt deliberately keeps raw quantities as hex strings. Conversion is
// performed by the transaction layer so RPC formatting never leaks upward.
type Receipt struct {
	TransactionHash   string `json:"transactionHash"`
	BlockNumber       string `json:"blockNumber"`
	Status            string `json:"status"`
	GasUsed           string `json:"gasUsed"`
	EffectiveGasPrice string `json:"effectiveGasPrice"`
	Logs              []Log  `json:"logs"`
}

type Log struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	var last error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if c.metrics != nil {
			c.metrics.IncRPCCalls()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			last = classify(err)
		} else {
			data, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				last = classify(readErr)
			} else if resp.StatusCode/100 != 2 {
				last = &Error{HTTP, fmt.Errorf("rpc HTTP status %d", resp.StatusCode)}
			} else {
				var envelope struct {
					Result json.RawMessage `json:"result"`
					Error  *struct {
						Message string `json:"message"`
					} `json:"error"`
				}
				if err = json.Unmarshal(data, &envelope); err != nil {
					return err
				}
				if envelope.Error != nil {
					return &Error{RPC, errors.New(envelope.Error.Message)}
				}
				return json.Unmarshal(envelope.Result, out)
			}
		}
		if c.metrics != nil {
			c.metrics.IncRPCErrors()
		}
		if attempt < c.retries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}
		}
	}
	return last
}
func (c *Client) ChainID(ctx context.Context) (int64, error) {
	var raw string
	err := c.Call(ctx, "eth_chainId", []any{}, &raw)
	if err != nil {
		return 0, err
	}
	return hexInt(raw)
}
func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	var raw string
	err := c.Call(ctx, "eth_blockNumber", []any{}, &raw)
	if err != nil {
		return 0, err
	}
	n, e := strconv.ParseUint(strings.TrimPrefix(raw, "0x"), 16, 64)
	return n, e
}
func (c *Client) GetBlockByNumber(ctx context.Context, number string) (Block, error) {
	var b Block
	err := c.Call(ctx, "eth_getBlockByNumber", []any{number, false}, &b)
	return b, err
}
func (c *Client) EthCall(ctx context.Context, call map[string]string) (string, error) {
	var raw string
	err := c.Call(ctx, "eth_call", []any{call, "latest"}, &raw)
	return raw, err
}

func (c *Client) EstimateGas(ctx context.Context, call CallMessage) (uint64, error) {
	var raw string
	if err := c.Call(ctx, "eth_estimateGas", []any{call}, &raw); err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimPrefix(raw, "0x"), 16, 64)
	return v, err
}

func (c *Client) GasPrice(ctx context.Context) (*big.Int, error) {
	var raw string
	if err := c.Call(ctx, "eth_gasPrice", []any{}, &raw); err != nil {
		return nil, err
	}
	return hexBig(raw)
}

func (c *Client) MaxPriorityFeePerGas(ctx context.Context) (*big.Int, error) {
	var raw string
	if err := c.Call(ctx, "eth_maxPriorityFeePerGas", []any{}, &raw); err != nil {
		return nil, err
	}
	return hexBig(raw)
}

func (c *Client) TransactionCount(ctx context.Context, address, block string) (uint64, error) {
	if block == "" {
		block = "pending"
	}
	var raw string
	if err := c.Call(ctx, "eth_getTransactionCount", []any{address, block}, &raw); err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimPrefix(raw, "0x"), 16, 64)
	return v, err
}

func (c *Client) SendRawTransaction(ctx context.Context, rawTx string) (string, error) {
	var hash string
	err := c.Call(ctx, "eth_sendRawTransaction", []any{rawTx}, &hash)
	return hash, err
}

func (c *Client) TransactionReceipt(ctx context.Context, hash string) (*Receipt, error) {
	var receipt *Receipt
	if err := c.Call(ctx, "eth_getTransactionReceipt", []any{hash}, &receipt); err != nil {
		return nil, err
	}
	return receipt, nil
}
func (c *Client) GetCode(ctx context.Context, address string) (string, error) {
	var raw string
	err := c.Call(ctx, "eth_getCode", []any{address, "latest"}, &raw)
	return raw, err
}
func (c *Client) Healthy(ctx context.Context) (uint64, error) { return c.BlockNumber(ctx) }
func hexInt(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimPrefix(raw, "0x"), 16, 64)
}

func hexBig(raw string) (*big.Int, error) {
	v := new(big.Int)
	if _, ok := v.SetString(strings.TrimPrefix(raw, "0x"), 16); !ok {
		return nil, fmt.Errorf("invalid hex quantity")
	}
	return v, nil
}
func classify(err error) *Error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Timeout, err}
	}
	return &Error{Network, err}
}
