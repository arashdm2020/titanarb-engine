// Package pools discovers real supported DEX pools using their native ABIs.
package pools

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/titanarb/titanarb-go/internal/dex"
)

type DEX string

const (
	UniswapV3 DEX = "uniswap_v3"
	CamelotV3 DEX = "camelot_v3"
)

type Pool struct {
	Address          string
	Token0           string
	Token1           string
	DEX              DEX
	Fee              uint32 // Camelot Algebra fees are dynamic and represented as zero.
	Liquidity        *big.Int
	SqrtPriceX96     *big.Int
	LastUpdatedBlock uint64
}

// Swap is the canonical subset of a supported V3/Algebra Swap event needed by
// pair intelligence. Amounts retain their signed int256 semantics.
type Swap struct {
	Pool         string
	Amount0      *big.Int
	Amount1      *big.Int
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Block        uint64
}

// Observer receives already-fetched pool and Swap state. Implementations must
// be non-blocking because this hook is on the market hot path.
type Observer interface {
	ObservePool(Pool)
	ObserveSwap(Swap)
}

func (p Pool) Supports(tokenIn, tokenOut string) bool {
	return (strings.EqualFold(tokenIn, p.Token0) && strings.EqualFold(tokenOut, p.Token1)) ||
		(strings.EqualFold(tokenIn, p.Token1) && strings.EqualFold(tokenOut, p.Token0))
}

type Caller interface {
	EthCall(ctx context.Context, call map[string]string) (string, error)
	BlockNumber(ctx context.Context) (uint64, error)
}

type historicalCaller interface {
	EthCallAt(context.Context, map[string]string, string) (string, error)
}

type logCaller interface {
	Call(context.Context, string, any, any) error
}

var ErrLogQueryUnsupported = errors.New("pool log query unsupported")

type Discoverer struct {
	caller         Caller
	uniswapFactory string
	camelotFactory string
	fees           []uint32
	observerMu     sync.RWMutex
	observer       Observer
}

func NewDiscoverer(caller Caller, uniswapFactory, camelotFactory string, fees []uint32) *Discoverer {
	return &Discoverer{caller: caller, uniswapFactory: uniswapFactory, camelotFactory: camelotFactory, fees: append([]uint32(nil), fees...)}
}

func (d *Discoverer) SetObserver(observer Observer) {
	d.observerMu.Lock()
	d.observer = observer
	d.observerMu.Unlock()
}

func (d *Discoverer) notifyPool(pool Pool) {
	d.observerMu.RLock()
	observer := d.observer
	d.observerMu.RUnlock()
	if observer != nil {
		observer.ObservePool(pool)
	}
}

func (d *Discoverer) notifySwap(swap Swap) {
	d.observerMu.RLock()
	observer := d.observer
	d.observerMu.RUnlock()
	if observer != nil {
		observer.ObserveSwap(swap)
	}
}

// DiscoverPair returns every liquid real Uniswap pool at the configured fee
// tiers plus the native Algebra/Camelot pool. No ABI is shared by assumption.
func (d *Discoverer) DiscoverPair(ctx context.Context, tokenA, tokenB string) ([]Pool, error) {
	block, err := d.caller.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}
	return d.DiscoverPairAt(ctx, tokenA, tokenB, block)
}

// DiscoverPairAt gives every state read in one market cycle the same pinned
// block tag when the RPC caller supports historical eth_call.
func (d *Discoverer) DiscoverPairAt(ctx context.Context, tokenA, tokenB string, block uint64) ([]Pool, error) {
	var found []Pool
	for _, fee := range d.fees {
		pool, err := d.uniswapPool(ctx, tokenA, tokenB, fee, block)
		if err != nil {
			return nil, err
		}
		if pool != nil && pool.Liquidity.Sign() > 0 {
			found = append(found, *pool)
		}
	}
	pool, err := d.camelotPool(ctx, tokenA, tokenB, block)
	if err != nil {
		return nil, err
	}
	if pool != nil && pool.Liquidity.Sign() > 0 {
		found = append(found, *pool)
	}
	return found, nil
}

// DiscoveryFeeTiers returns a copy of the configured Uniswap fee tiers so a
// background reconciler can checkpoint work at one venue/fee unit at a time.
func (d *Discoverer) DiscoveryFeeTiers() []uint32 {
	return append([]uint32(nil), d.fees...)
}

// DiscoverUniswapFeeAt discovers one bounded Uniswap fee-tier unit.
func (d *Discoverer) DiscoverUniswapFeeAt(ctx context.Context, tokenA, tokenB string, fee uint32, block uint64) ([]Pool, error) {
	pool, err := d.uniswapPool(ctx, tokenA, tokenB, fee, block)
	if err != nil || pool == nil || pool.Liquidity.Sign() <= 0 {
		return nil, err
	}
	return []Pool{*pool}, nil
}

// DiscoverCamelotPairAt discovers one bounded Camelot/Algebra pair unit.
func (d *Discoverer) DiscoverCamelotPairAt(ctx context.Context, tokenA, tokenB string, block uint64) ([]Pool, error) {
	pool, err := d.camelotPool(ctx, tokenA, tokenB, block)
	if err != nil || pool == nil || pool.Liquidity.Sign() <= 0 {
		return nil, err
	}
	return []Pool{*pool}, nil
}

// RefreshPoolAt updates only mutable pool state. Factory, token and static fee
// reads are intentionally reused from the cache during incremental cycles.
func (d *Discoverer) RefreshPoolAt(ctx context.Context, pool Pool, block uint64) (Pool, error) {
	liquidityRaw, err := d.call(ctx, pool.Address, dex.StaticCall("liquidity()"), block)
	if err != nil {
		return Pool{}, err
	}
	liquidityWords, err := dex.DecodeWords(liquidityRaw)
	if err != nil || len(liquidityWords) < 1 {
		return Pool{}, fmt.Errorf("pool liquidity response invalid")
	}
	stateMethod := "slot0()"
	if pool.DEX == CamelotV3 {
		stateMethod = "globalState()"
	} else if pool.DEX != UniswapV3 {
		return Pool{}, fmt.Errorf("unsupported pool DEX")
	}
	stateRaw, err := d.call(ctx, pool.Address, dex.StaticCall(stateMethod), block)
	if err != nil {
		return Pool{}, err
	}
	stateWords, err := dex.DecodeWords(stateRaw)
	if err != nil || len(stateWords) < 1 {
		return Pool{}, fmt.Errorf("pool state response invalid")
	}
	updated := pool
	updated.Liquidity = dex.WordUint(liquidityWords[0])
	updated.SqrtPriceX96 = dex.WordUint(stateWords[0])
	updated.LastUpdatedBlock = block
	d.notifyPool(updated)
	return updated, nil
}

// ChangedPoolAddressesAt obtains the dirty pool set for an inclusive block
// range with one eth_getLogs request. Any event from a supported pool marks the
// pool dirty; this conservatively includes swaps and in/out-of-range liquidity
// changes without assuming identical Uniswap and Algebra event ABIs.
func (d *Discoverer) ChangedPoolAddressesAt(ctx context.Context, poolAddresses []string, fromBlock, toBlock uint64) (map[string]struct{}, error) {
	changed := make(map[string]struct{})
	if len(poolAddresses) == 0 || toBlock == 0 {
		return changed, nil
	}
	caller, ok := d.caller.(logCaller)
	if !ok {
		return nil, ErrLogQueryUnsupported
	}
	if fromBlock == 0 || fromBlock > toBlock {
		fromBlock = toBlock
	}
	var logs []struct {
		Address     string   `json:"address"`
		Data        string   `json:"data"`
		Topics      []string `json:"topics"`
		BlockNumber string   `json:"blockNumber"`
	}
	filter := map[string]any{
		"fromBlock": fmt.Sprintf("0x%x", fromBlock),
		"toBlock":   fmt.Sprintf("0x%x", toBlock),
		"address":   poolAddresses,
	}
	if err := caller.Call(ctx, "eth_getLogs", []any{filter}, &logs); err != nil {
		return nil, err
	}
	for _, entry := range logs {
		if entry.Address != "" {
			changed[strings.ToLower(entry.Address)] = struct{}{}
		}
		if swap, ok := decodeSwapLog(entry.Address, entry.Data, entry.Topics, entry.BlockNumber); ok {
			d.notifySwap(swap)
		}
	}
	return changed, nil
}

func (d *Discoverer) uniswapPool(ctx context.Context, a, b string, fee uint32, block uint64) (*Pool, error) {
	aw, err := dex.AddressWord(a)
	if err != nil {
		return nil, err
	}
	bw, err := dex.AddressWord(b)
	if err != nil {
		return nil, err
	}
	raw, err := d.call(ctx, d.uniswapFactory, dex.StaticCall("getPool(address,address,uint24)", aw, bw, dex.Uint64Word(uint64(fee))), block)
	if err != nil {
		return nil, err
	}
	words, err := dex.DecodeWords(raw)
	if err != nil {
		return nil, err
	}
	address := dex.WordAddress(words[0])
	if isZeroAddress(address) {
		return nil, nil
	}
	token0, token1, liquidity, err := d.corePoolState(ctx, address, block)
	if err != nil {
		return nil, err
	}
	feeRaw, err := d.call(ctx, address, dex.StaticCall("fee()"), block)
	if err != nil {
		return nil, err
	}
	feeWords, err := dex.DecodeWords(feeRaw)
	if err != nil {
		return nil, err
	}
	slot0Raw, err := d.call(ctx, address, dex.StaticCall("slot0()"), block)
	if err != nil {
		return nil, err
	}
	slotWords, err := dex.DecodeWords(slot0Raw)
	if err != nil {
		return nil, err
	}
	pool := &Pool{Address: address, Token0: token0, Token1: token1, DEX: UniswapV3, Fee: uint32(dex.WordUint(feeWords[0]).Uint64()), Liquidity: liquidity, SqrtPriceX96: dex.WordUint(slotWords[0]), LastUpdatedBlock: block}
	d.notifyPool(*pool)
	return pool, nil
}

func (d *Discoverer) camelotPool(ctx context.Context, a, b string, block uint64) (*Pool, error) {
	aw, err := dex.AddressWord(a)
	if err != nil {
		return nil, err
	}
	bw, err := dex.AddressWord(b)
	if err != nil {
		return nil, err
	}
	raw, err := d.call(ctx, d.camelotFactory, dex.StaticCall("poolByPair(address,address)", aw, bw), block)
	if err != nil {
		return nil, err
	}
	words, err := dex.DecodeWords(raw)
	if err != nil {
		return nil, err
	}
	address := dex.WordAddress(words[0])
	if isZeroAddress(address) {
		return nil, nil
	}
	token0, token1, liquidity, err := d.corePoolState(ctx, address, block)
	if err != nil {
		return nil, err
	}
	// Algebra's globalState is intentionally not decoded as Uniswap slot0. The
	// first word is captured only as a read-only state marker.
	stateRaw, err := d.call(ctx, address, dex.StaticCall("globalState()"), block)
	if err != nil {
		return nil, err
	}
	stateWords, err := dex.DecodeWords(stateRaw)
	if err != nil {
		return nil, err
	}
	pool := &Pool{Address: address, Token0: token0, Token1: token1, DEX: CamelotV3, Liquidity: liquidity, SqrtPriceX96: dex.WordUint(stateWords[0]), LastUpdatedBlock: block}
	d.notifyPool(*pool)
	return pool, nil
}

func (d *Discoverer) corePoolState(ctx context.Context, address string, block uint64) (string, string, *big.Int, error) {
	t0, err := d.call(ctx, address, dex.StaticCall("token0()"), block)
	if err != nil {
		return "", "", nil, err
	}
	t1, err := d.call(ctx, address, dex.StaticCall("token1()"), block)
	if err != nil {
		return "", "", nil, err
	}
	liq, err := d.call(ctx, address, dex.StaticCall("liquidity()"), block)
	if err != nil {
		return "", "", nil, err
	}
	w0, err := dex.DecodeWords(t0)
	if err != nil {
		return "", "", nil, err
	}
	w1, err := dex.DecodeWords(t1)
	if err != nil {
		return "", "", nil, err
	}
	wl, err := dex.DecodeWords(liq)
	if err != nil {
		return "", "", nil, err
	}
	return dex.WordAddress(w0[0]), dex.WordAddress(w1[0]), dex.WordUint(wl[0]), nil
}

func (d *Discoverer) call(ctx context.Context, to, data string, block uint64) (string, error) {
	if to == "" {
		return "", fmt.Errorf("protocol address missing")
	}
	if historical, ok := d.caller.(historicalCaller); ok && block > 0 {
		return historical.EthCallAt(ctx, map[string]string{"to": to, "data": data}, fmt.Sprintf("0x%x", block))
	}
	return d.caller.EthCall(ctx, map[string]string{"to": to, "data": data})
}

func isZeroAddress(address string) bool {
	return strings.EqualFold(address, "0x0000000000000000000000000000000000000000")
}

var swapTopic = strings.ToLower(dex.EventTopic("Swap(address,address,int256,int256,uint160,uint128,int24)"))

func decodeSwapLog(address, data string, topics []string, blockHex string) (Swap, bool) {
	if len(topics) == 0 || !strings.EqualFold(topics[0], swapTopic) {
		return Swap{}, false
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(data, "0x"))
	if err != nil || len(raw) < 32*4 {
		return Swap{}, false
	}
	block := uint64(0)
	if strings.HasPrefix(blockHex, "0x") {
		_, _ = fmt.Sscanf(blockHex, "0x%x", &block)
	}
	return Swap{
		Pool: address, Amount0: signedWord(raw[0:32]), Amount1: signedWord(raw[32:64]),
		SqrtPriceX96: new(big.Int).SetBytes(raw[64:96]), Liquidity: new(big.Int).SetBytes(raw[96:128]), Block: block,
	}, true
}

func signedWord(raw []byte) *big.Int {
	value := new(big.Int).SetBytes(raw)
	if len(raw) == 32 && raw[0]&0x80 != 0 {
		value.Sub(value, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return value
}
