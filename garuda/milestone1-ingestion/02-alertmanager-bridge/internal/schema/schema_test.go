package schema

import "testing"

func TestFingerprint_Deterministic(t *testing.T) {
	a := Fingerprint(
		"alertmanager:GarudaApiserverHighErrorRate",
		"apiserver_request_total",
		map[string]string{"verb": "POST", "code": "5xx", "resource": "pods"},
		"pac-sfcluster01",
	)
	b := Fingerprint(
		"alertmanager:GarudaApiserverHighErrorRate",
		"apiserver_request_total",
		// Same matchers in different insertion order — fingerprint must match.
		map[string]string{"resource": "pods", "code": "5xx", "verb": "POST"},
		"pac-sfcluster01",
	)
	if a != b {
		t.Fatalf("fingerprint not stable across map order: %s vs %s", a, b)
	}
	if len(a) != 16 {
		t.Fatalf("fingerprint length = %d, want 16", len(a))
	}
}

func TestFingerprint_Differs_OnAnyChange(t *testing.T) {
	base := Fingerprint("d", "m", map[string]string{"k": "v"}, "c")
	cases := []struct {
		name string
		fp   string
	}{
		{"detector", Fingerprint("d2", "m", map[string]string{"k": "v"}, "c")},
		{"metric", Fingerprint("d", "m2", map[string]string{"k": "v"}, "c")},
		{"matcher", Fingerprint("d", "m", map[string]string{"k": "v2"}, "c")},
		{"cluster", Fingerprint("d", "m", map[string]string{"k": "v"}, "c2")},
	}
	for _, c := range cases {
		if c.fp == base {
			t.Errorf("fingerprint unchanged after %s mutation", c.name)
		}
	}
}
