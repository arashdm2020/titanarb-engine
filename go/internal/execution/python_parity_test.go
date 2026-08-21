package execution

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// This test compares production Python's actual ABI encoder with the Go
// builder for deterministic 2/3/4-hop fixtures. It is skipped only when the
// archived Python reference environment is intentionally absent.
func TestPythonCalldataParity(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(root, ".venv", "Scripts", "python.exe")
	if _, err := os.Stat(python); err != nil {
		t.Skip("Python reference environment unavailable")
	}
	script := `from web3 import Web3
from bot.executor_arbitrum import FLASH_ARBITRAGE_EXECUTOR_ABI, encode_uniswap_v3_data
cs=Web3.to_checksum_address
c=Web3().eth.contract(address=cs('0xdc63781E4f880F3911260Ecf0f1208eB32756666'),abi=FLASH_ARBITRAGE_EXECUTOR_ABI)
a=cs('0xaf88d065e77c8cC2239327C5EDb3A432268e5831');w=cs('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1');r=cs('0x912ce59144191c1204e64559fe8253a0e49e6548');u=cs('0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9');d=cs('0xD03BC6e7331E726dA03De74b37437F1ACa2dFD95')
for n,p in {2:[a,w,a],3:[a,w,r,a],4:[a,w,r,u,a]}.items():
 s=[(d,p[i],p[i+1],900000000-i,encode_uniswap_v3_data(3000) if i%2==0 else b'') for i in range(n)]
 print(c.functions.executeArbitrage(a,1000000000,s,1800000000,5000000)._encode_transaction_data())`
	cmd := exec.Command(python, "-c", script)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("Python reference encoder: %v", err)
	}
	lines := strings.Fields(string(out))
	if len(lines) != 3 {
		t.Fatalf("expected 3 Python vectors, got %d", len(lines))
	}
	for index, hops := range []int{2, 3, 4} {
		data, err := requestWithHops(t, hops).Calldata()
		if err != nil {
			t.Fatal(err)
		}
		got := "0x" + hex.EncodeToString(data)
		if !strings.EqualFold(got, lines[index]) {
			t.Fatalf("%d-hop Python/Go calldata mismatch", hops)
		}
	}
}
