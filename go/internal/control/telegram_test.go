package control

import (
	"strings"
	"testing"

	"github.com/titanarb/titanarb-go/internal/runtimeconfig"
)

func TestUnauthorizedTelegramUserCannotChangeRisk(t *testing.T) {
	risk, err := runtimeconfig.Open("", runtimeconfig.Defaults(runtimeconfig.Balanced))
	if err != nil {
		t.Fatal(err)
	}
	h := Handler{Auth: Authorizer{ChatID: "-100", AdminID: "42"}, Risk: risk}
	if _, handled := h.Handle(Request{ChatID: "-100", SenderID: "99", Text: "/risk aggressive"}); handled {
		t.Fatal("unauthorized command was handled")
	}
	if risk.Snapshot().Profile != runtimeconfig.Balanced {
		t.Fatal("unauthorized user changed runtime risk")
	}
}

func TestAuthorizedRiskCommandPersistsEffectiveValue(t *testing.T) {
	risk, err := runtimeconfig.Open("", runtimeconfig.Defaults(runtimeconfig.Balanced))
	if err != nil {
		t.Fatal(err)
	}
	h := Handler{Auth: Authorizer{ChatID: "-100", AdminID: "42"}, Risk: risk}
	response, handled := h.Handle(Request{ChatID: "-100", SenderID: "42", Text: "/set volatility_weight 1.5"})
	if !handled || !strings.Contains(response, "Runtime setting applied") || risk.Snapshot().VolatilityWeight != 1.5 {
		t.Fatalf("response=%q handled=%t settings=%+v", response, handled, risk.Snapshot())
	}
}
