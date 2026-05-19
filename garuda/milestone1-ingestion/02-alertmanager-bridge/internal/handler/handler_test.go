package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/myntra/garuda/alertmanager-bridge/internal/schema"
)

type capturePub struct {
	mu     sync.Mutex
	values [][]byte
	keys   []string
}

func (c *capturePub) Publish(ctx context.Context, key string, v []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values = append(c.values, append([]byte(nil), v...))
	c.keys = append(c.keys, key)
	return nil
}

func newWebhook(p *capturePub) *Webhook {
	c := func() prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: "x"})
	}
	return &Webhook{
		Pub:             p,
		AlertsTotal:     c(),
		AlertsFiltered:  c(),
		AlertsInvalid:   c(),
		AlertsPublished: c(),
		PublishErrors:   c(),
	}
}

const samplePayload = `{
	"version": "4",
	"status": "firing",
	"receiver": "garuda-bridge",
	"commonLabels": {"cluster": "pac-sfcluster01"},
	"alerts": [
		{
			"status": "firing",
			"labels": {
				"alertname": "GarudaApiserverHighErrorRate",
				"garuda_anomaly": "true",
				"severity": "critical",
				"garuda_signal_group": "apiserver",
				"garuda_metric": "apiserver_request_total",
				"verb": "POST",
				"resource": "pods",
				"code": "5xx",
				"cluster": "pac-sfcluster01",
				"namespace": "kube-system"
			},
			"annotations": {
				"summary": "API server POST/pods 5xx error rate above 2%",
				"description": "POST/pods returning 5xx",
				"garuda_promql": "rate(apiserver_request_total{code=~\"5..\"}[5m])"
			},
			"startsAt": "2026-04-25T09:13:30Z",
			"endsAt":   "0001-01-01T00:00:00Z",
			"fingerprint": "deadbeef"
		},
		{
			"status": "firing",
			"labels": { "alertname": "SomeNonGarudaAlert", "severity": "warning" }
		}
	]
}`

func TestWebhook_OK_PublishesGarudaAlertOnly(t *testing.T) {
	pub := &capturePub{}
	wh := newWebhook(pub)

	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(samplePayload))
	rr := httptest.NewRecorder()
	wh.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if len(pub.values) != 1 {
		t.Fatalf("expected 1 published msg, got %d", len(pub.values))
	}

	var out schema.AnomalySignal
	if err := json.Unmarshal(pub.values[0], &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Tier != 1 {
		t.Errorf("tier = %d, want 1", out.Tier)
	}
	if out.Detector != "alertmanager:GarudaApiserverHighErrorRate" {
		t.Errorf("detector = %q", out.Detector)
	}
	if out.Cluster != "pac-sfcluster01" {
		t.Errorf("cluster = %q", out.Cluster)
	}
	if out.Severity != "critical" {
		t.Errorf("severity = %q", out.Severity)
	}
	if out.Confidence != 1.0 {
		t.Errorf("confidence = %v, want 1.0", out.Confidence)
	}
	if out.Metric.Name != "apiserver_request_total" {
		t.Errorf("metric.name = %q", out.Metric.Name)
	}
	if out.Metric.Matchers["verb"] != "POST" {
		t.Errorf("matcher 'verb' missing")
	}
	if _, ok := out.Metric.Matchers["alertname"]; ok {
		t.Errorf("alertname leaked into matchers")
	}
	if _, ok := out.Metric.Matchers["garuda_anomaly"]; ok {
		t.Errorf("garuda_anomaly leaked into matchers")
	}
	if out.Evidence.PromQL == nil || !strings.Contains(*out.Evidence.PromQL, "rate(apiserver_request_total") {
		t.Errorf("promql annotation not propagated")
	}
	if pub.keys[0] != out.Fingerprint {
		t.Errorf("kafka key %q != fingerprint %q", pub.keys[0], out.Fingerprint)
	}
}

func TestWebhook_RejectsNonPost(t *testing.T) {
	wh := newWebhook(&capturePub{})
	req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
	rr := httptest.NewRecorder()
	wh.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestWebhook_AuthGate(t *testing.T) {
	pub := &capturePub{}
	wh := newWebhook(pub)
	wh.SharedSecret = "s3cret"

	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(samplePayload))
	rr := httptest.NewRecorder()
	wh.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(samplePayload))
	req2.Header.Set("X-Garuda-Bridge-Secret", "s3cret")
	rr2 := httptest.NewRecorder()
	wh.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 with secret, got %d", rr2.Code)
	}
}
