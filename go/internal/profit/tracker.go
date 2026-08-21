// Package profit writes durable, secret-free execution records. JSONL is used
// deliberately so a partial write never corrupts prior operational evidence.
package profit

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Record struct {
	Timestamp      time.Time `json:"timestamp"`
	Event          string    `json:"event"`
	TxHash         string    `json:"tx_hash,omitempty"`
	Route          string    `json:"route,omitempty"`
	ExpectedProfit string    `json:"expected_profit,omitempty"`
	ActualProfit   string    `json:"actual_profit,omitempty"`
	AavePremium    string    `json:"aave_premium,omitempty"`
	GasSpentWei    string    `json:"gas_spent_wei,omitempty"`
	ReceiptStatus  string    `json:"receipt_status,omitempty"`
	Reason         string    `json:"reason,omitempty"`
}
type Tracker struct {
	path string
	mu   sync.Mutex
}

func New(path string) *Tracker { return &Tracker{path: path} }
func (t *Tracker) Write(record Record) error {
	if t == nil || t.path == "" {
		return nil
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}
	if err := os.MkdirAll(filepath.Dir(t.path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(t.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	t.mu.Lock()
	defer t.mu.Unlock()
	return json.NewEncoder(file).Encode(record)
}
func Decimal(v *big.Int) string {
	if v == nil {
		return "0"
	}
	return v.String()
}
