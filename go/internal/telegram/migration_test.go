package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCanonicalConfiguration(t *testing.T) {
	t.Setenv("TELEGRAM_BOT_TOKEN", "legacy")
	t.Setenv("TELEGRAM_CHAT_ID", "old-chat")
	t.Setenv("TITANARB_TELEGRAM_ENABLED", "true")
	t.Setenv("TITANARB_TELEGRAM_BOT_TOKEN", "test-credential")
	t.Setenv("TITANARB_TELEGRAM_CHANNEL_ID", "-100123")
	c := FromEnv()
	if !c.Enabled() || c.Token != "test-credential" || c.ChatID != "-100123" || !c.ReadOnly {
		t.Fatal("canonical configuration not authoritative")
	}
	t.Setenv("TITANARB_TELEGRAM_ENABLED", "false")
	if FromEnv().Enabled() {
		t.Fatal("explicit disable ignored")
	}
	t.Setenv("TITANARB_TELEGRAM_ENABLED", "true")
	t.Setenv("TITANARB_TELEGRAM_BOT_TOKEN", "")
	if FromEnv().Enabled() {
		t.Fatal("empty token fell back to obsolete token")
	}
	t.Setenv("TITANARB_TELEGRAM_BOT_TOKEN", "test-credential")
	t.Setenv("TITANARB_TELEGRAM_CHANNEL_ID", "")
	if FromEnv().Enabled() {
		t.Fatal("missing channel enabled")
	}
}

func TestDeliveryStatusAndDestination(t *testing.T) {
	for _, status := range []int{200, 401, 403, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]string
				if json.NewDecoder(r.Body).Decode(&body) != nil || body["chat_id"] != "-100123" {
					t.Error("incorrect destination")
				}
				if strings.Contains(body["text"], "test-credential") {
					t.Error("credential leaked into message")
				}
				w.WriteHeader(status)
				fmt.Fprint(w, `{"ok":true}`)
			}))
			defer server.Close()
			c := &Client{cfg: Config{Token: "test-credential", ChatID: "-100123"}, endpoint: server.URL, http: server.Client()}
			c.send(Message{Text: "test-credential"})
			sent, failed, _ := c.Snapshot()
			if status == 200 && sent != 1 || status != 200 && failed != 1 {
				t.Fatal("incorrect result counters")
			}
		})
	}
}

func TestUpdateErrorsNeverExposeCredentials(t *testing.T) {
	c := &Client{cfg: Config{Token: "test-credential", ChatID: "1"}, endpoint: "http://127.0.0.1:1", http: &http.Client{}}
	_, err := c.Updates(context.Background(), 0, 1)
	if err == nil || strings.Contains(err.Error(), "test-credential") || strings.Contains(err.Error(), "http://") {
		t.Fatal("unsafe transport error")
	}
	c.cfg.ReadOnly = true
	if _, err := c.Updates(context.Background(), 0, 1); err != nil {
		t.Fatal("notification-only client attempted polling")
	}
	c.cfg.Disabled = true
	c.Notify(Message{Text: "disabled"})
}
