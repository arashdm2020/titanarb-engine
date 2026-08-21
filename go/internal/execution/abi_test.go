package execution

import (
	"math/big"
	"testing"
)

func TestDecodeActualInsufficientProfitError(t *testing.T) {
	errorABI := ExecutorABI().Errors["InsufficientProfit"]
	payload, err := errorABI.Inputs.Pack(big.NewInt(10_010_000_000), big.NewInt(10_000_000_000))
	if err != nil {
		t.Fatal(err)
	}
	data := append(errorABI.ID[:4], payload...)
	name, values, ok := DecodeError(data)
	if !ok || name != "InsufficientProfit" {
		t.Fatalf("decoded %q ok=%t", name, ok)
	}
	if values["required"].(*big.Int).Cmp(big.NewInt(10_010_000_000)) != 0 || values["available"].(*big.Int).Cmp(big.NewInt(10_000_000_000)) != 0 {
		t.Fatal("argument order changed")
	}
}
