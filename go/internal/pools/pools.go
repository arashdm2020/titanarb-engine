// Package pools discovers real supported DEX pools using their native ABIs.
package pools

import (
	"context"
	"fmt"
	"math/big"
	"strings"

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

func (p Pool) Supports(tokenIn, tokenOut string) bool {
	return (strings.EqualFold(tokenIn, p.Token0) && strings.EqualFold(tokenOut, p.Token1)) ||
		(strings.EqualFold(tokenIn, p.Token1) && strings.EqualFold(tokenOut, p.Token0))
}

type Caller interface {
	EthCall(ctx context.Context, call map[string]string) (string, error)
	BlockNumber(ctx context.Context) (uint64, error)
}

type Discoverer struct {
	caller         Caller
	uniswapFactory string
	camelotFactory string
	fees           []uint32
}

func NewDiscoverer(caller Caller, uniswapFactory, camelotFactory string, fees []uint32) *Discoverer {
	return &Discoverer{caller: caller, uniswapFactory: uniswapFactory, camelotFactory: camelotFactory, fees: append([]uint32(nil), fees...)}
}

// DiscoverPair returns every liquid real Uniswap pool at the configured fee
// tiers plus the native Algebra/Camelot pool. No ABI is shared by assumption.
func (d *Discoverer) DiscoverPair(ctx context.Context, tokenA, tokenB string) ([]Pool, error) {
	block, err := d.caller.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}
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

func (d *Discoverer) uniswapPool(ctx context.Context, a, b string, fee uint32, block uint64) (*Pool, error) {
	aw, err := dex.AddressWord(a)
	if err != nil {
		return nil, err
	}
	bw, err := dex.AddressWord(b)
	if err != nil {
		return nil, err
	}
	raw, err := d.call(ctx, d.uniswapFactory, dex.StaticCall("getPool(address,address,uint24)", aw, bw, dex.Uint64Word(uint64(fee))))
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
	token0, token1, liquidity, err := d.corePoolState(ctx, address)
	if err != nil {
		return nil, err
	}
	feeRaw, err := d.call(ctx, address, dex.StaticCall("fee()"))
	if err != nil {
		return nil, err
	}
	feeWords, err := dex.DecodeWords(feeRaw)
	if err != nil {
		return nil, err
	}
	slot0Raw, err := d.call(ctx, address, dex.StaticCall("slot0()"))
	if err != nil {
		return nil, err
	}
	slotWords, err := dex.DecodeWords(slot0Raw)
	if err != nil {
		return nil, err
	}
	return &Pool{Address: address, Token0: token0, Token1: token1, DEX: UniswapV3, Fee: uint32(dex.WordUint(feeWords[0]).Uint64()), Liquidity: liquidity, SqrtPriceX96: dex.WordUint(slotWords[0]), LastUpdatedBlock: block}, nil
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
	raw, err := d.call(ctx, d.camelotFactory, dex.StaticCall("poolByPair(address,address)", aw, bw))
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
	token0, token1, liquidity, err := d.corePoolState(ctx, address)
	if err != nil {
		return nil, err
	}
	// Algebra's globalState is intentionally not decoded as Uniswap slot0. The
	// first word is captured only as a read-only state marker.
	stateRaw, err := d.call(ctx, address, dex.StaticCall("globalState()"))
	if err != nil {
		return nil, err
	}
	stateWords, err := dex.DecodeWords(stateRaw)
	if err != nil {
		return nil, err
	}
	return &Pool{Address: address, Token0: token0, Token1: token1, DEX: CamelotV3, Liquidity: liquidity, SqrtPriceX96: dex.WordUint(stateWords[0]), LastUpdatedBlock: block}, nil
}

func (d *Discoverer) corePoolState(ctx context.Context, address string) (string, string, *big.Int, error) {
	t0, err := d.call(ctx, address, dex.StaticCall("token0()"))
	if err != nil {
		return "", "", nil, err
	}
	t1, err := d.call(ctx, address, dex.StaticCall("token1()"))
	if err != nil {
		return "", "", nil, err
	}
	liq, err := d.call(ctx, address, dex.StaticCall("liquidity()"))
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

func (d *Discoverer) call(ctx context.Context, to, data string) (string, error) {
	if to == "" {
		return "", fmt.Errorf("protocol address missing")
	}
	return d.caller.EthCall(ctx, map[string]string{"to": to, "data": data})
}

func isZeroAddress(address string) bool {
	return strings.EqualFold(address, "0x0000000000000000000000000000000000000000")
}
