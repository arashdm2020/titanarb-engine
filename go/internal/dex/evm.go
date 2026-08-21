// Package dex contains read-only ABI helpers shared by the supported DEXes.
package dex

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"golang.org/x/crypto/sha3"
)

func selector(signature string) []byte {
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write([]byte(signature))
	return h.Sum(nil)[:4]
}

// StaticCall encodes calls whose arguments are entirely 32-byte ABI words.
func StaticCall(signature string, words ...[]byte) string {
	data := append([]byte{}, selector(signature)...)
	for _, word := range words {
		data = append(data, word...)
	}
	return "0x" + hex.EncodeToString(data)
}

func AddressWord(address string) ([]byte, error) {
	raw := strings.TrimPrefix(strings.TrimSpace(address), "0x")
	if len(raw) != 40 {
		return nil, fmt.Errorf("invalid address")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid address: %w", err)
	}
	word := make([]byte, 32)
	copy(word[12:], decoded)
	return word, nil
}

func UintWord(value *big.Int) []byte {
	word := make([]byte, 32)
	if value != nil {
		value.FillBytes(word)
	}
	return word
}

func Uint64Word(value uint64) []byte { return UintWord(new(big.Int).SetUint64(value)) }

func BoolWord(value bool) []byte {
	if value {
		return Uint64Word(1)
	}
	return Uint64Word(0)
}

// DynamicBytesCall encodes static ABI words followed by one dynamic bytes argument.
func DynamicBytesCall(signature string, staticWords [][]byte, bytesArg []byte) string {
	data := append([]byte{}, selector(signature)...)
	for _, word := range staticWords {
		data = append(data, word...)
	}
	data = append(data, Uint64Word(uint64(32*(len(staticWords)+1)))...)
	data = append(data, Uint64Word(uint64(len(bytesArg)))...)
	data = append(data, bytesArg...)
	if remainder := len(bytesArg) % 32; remainder != 0 {
		data = append(data, make([]byte, 32-remainder)...)
	}
	return "0x" + hex.EncodeToString(data)
}

func DecodeWords(raw string) ([][]byte, error) {
	data, err := hex.DecodeString(strings.TrimPrefix(raw, "0x"))
	if err != nil || len(data) == 0 || len(data)%32 != 0 {
		return nil, fmt.Errorf("invalid ABI response")
	}
	words := make([][]byte, 0, len(data)/32)
	for len(data) > 0 {
		words, data = append(words, data[:32]), data[32:]
	}
	return words, nil
}

func WordUint(word []byte) *big.Int { return new(big.Int).SetBytes(word) }

func WordInt(word []byte) *big.Int {
	v := WordUint(word)
	if len(word) == 32 && word[0]&0x80 != 0 {
		return v.Sub(v, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return v
}

func WordAddress(word []byte) string { return "0x" + hex.EncodeToString(word[12:]) }
