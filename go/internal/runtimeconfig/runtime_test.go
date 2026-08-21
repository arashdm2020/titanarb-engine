package runtimeconfig

import "testing"

func TestProfileAndCustomValuesPersistWithoutEnvChanges(t *testing.T) {
	path := t.TempDir() + "/runtime_config.json"
	m, err := Open(path, Defaults(Balanced))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := m.SetProfile(Aggressive); err != nil || got.Profile != Aggressive {
		t.Fatalf("profile=%+v err=%v", got, err)
	}
	if got, err := m.Set("slippage_bps", "120"); err != nil || got.Profile != Custom || got.SlippageBPS != 120 {
		t.Fatalf("settings=%+v err=%v", got, err)
	}
	reopened, err := Open(path, Defaults(Balanced))
	if err != nil || reopened.Snapshot().SlippageBPS != 120 || reopened.Snapshot().Profile != Custom {
		t.Fatalf("persisted=%+v err=%v", reopened.Snapshot(), err)
	}
}

func TestUnsafeRuntimeSettingIsRejected(t *testing.T) {
	m, err := Open("", Defaults(Balanced))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Set("route_search_depth", "5"); err == nil {
		t.Fatal("expected max-hop rejection")
	}
}

func TestCustomProfileKeepsValidatedValues(t *testing.T) {
	m, err := Open("", Defaults(Balanced))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := m.SetProfile(Custom)
	if err != nil || updated.Profile != Custom || updated.MinProfitUSD != Defaults(Balanced).MinProfitUSD {
		t.Fatalf("custom profile=%+v err=%v", updated, err)
	}
}
