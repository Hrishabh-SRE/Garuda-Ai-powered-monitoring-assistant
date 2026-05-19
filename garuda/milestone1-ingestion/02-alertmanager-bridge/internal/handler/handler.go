// Package handler implements the HTTP handler that accepts Alertmanager
// webhook payloads and converts them into AnomalySignal envelopes.
//
// Alertmanager webhook payload reference:
//
//	https://prometheus.io/docs/alerting/latest/configuration/#webhook_config
//
// The handler is intentionally tolerant: malformed individual alerts in a
// batch are dropped (counter incremented), but the whole batch is never
// rejected for one bad alert.
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/myntra/garuda/alertmanager-bridge/internal/schema"
)

// Publisher is the surface area the handler needs from the Kafka producer.
// Defined as an interface so we can mock it in tests.
type Publisher interface {
	Publish(ctx context.Context, key string, value []byte) error
}

// Webhook is the HTTP handler.
type Webhook struct {
	Pub             Publisher
	SharedSecret    string // optional; if non-empty, X-Garuda-Bridge-Secret must match
	AlertsTotal     prometheus.Counter
	AlertsFiltered  prometheus.Counter
	AlertsInvalid   prometheus.Counter
	AlertsPublished prometheus.Counter
	PublishErrors   prometheus.Counter
}

// ServeHTTP implements http.Handler.
func (w *Webhook) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if w.SharedSecret != "" {
		if r.Header.Get("X-Garuda-Bridge-Secret") != w.SharedSecret {
			http.Error(rw, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	var payload amWebhookPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(rw, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	ctx := r.Context()
	for _, a := range payload.Alerts {
		w.AlertsTotal.Inc()

		// Defense-in-depth: route should already filter, but verify.
		if a.Labels["garuda_anomaly"] != "true" {
			w.AlertsFiltered.Inc()
			continue
		}

		sig, err := w.toSignal(a, payload.CommonLabels)
		if err != nil {
			w.AlertsInvalid.Inc()
			continue
		}

		body, err := json.Marshal(sig)
		if err != nil {
			w.AlertsInvalid.Inc()
			continue
		}

		// Key by fingerprint so the same anomaly lands on the same partition
		// — keeps re-emissions ordered for downstream deduplication.
		if err := w.Pub.Publish(ctx, sig.Fingerprint, body); err != nil {
			w.PublishErrors.Inc()
			continue
		}
		w.AlertsPublished.Inc()
	}

	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write([]byte("ok"))
}

// toSignal converts a single Alertmanager alert to an AnomalySignal.
func (w *Webhook) toSignal(a amAlert, common map[string]string) (*schema.AnomalySignal, error) {
	alertname := a.Labels["alertname"]
	if alertname == "" {
		return nil, fmt.Errorf("missing alertname")
	}
	cluster := firstNonEmpty(a.Labels["cluster"], common["cluster"])
	if cluster == "" {
		return nil, fmt.Errorf("missing cluster label")
	}

	severity := a.Labels["severity"]
	if severity == "" {
		severity = "warning" // permissive default
	}
	severity = strings.ToLower(severity)
	switch severity {
	case "info", "warning", "critical":
	default:
		// Map Myntra-internal `pager` to critical so downstream stays simple.
		if severity == "pager" {
			severity = "critical"
		} else {
			severity = "warning"
		}
	}

	metricName := firstNonEmpty(a.Labels["garuda_metric"], "unknown")

	// Build matcher set: all alert labels EXCEPT internal Garuda labels and
	// the alertname / severity scaffolding.
	matchers := map[string]string{}
	skip := map[string]struct{}{
		"alertname":           {},
		"severity":            {},
		"garuda_anomaly":      {},
		"garuda_signal_group": {},
		"garuda_metric":       {},
		"prometheus":          {}, // injected by AM, not useful
	}
	for k, v := range a.Labels {
		if _, drop := skip[k]; drop {
			continue
		}
		matchers[k] = v
	}

	detector := "alertmanager:" + alertname
	fp := schema.Fingerprint(detector, metricName, matchers, cluster)

	tsStarted := a.StartsAt
	if tsStarted.IsZero() {
		tsStarted = time.Now().UTC()
	}
	var tsResolved *time.Time
	if a.Status == "resolved" && !a.EndsAt.IsZero() {
		t := a.EndsAt
		tsResolved = &t
	}

	// Hoist namespace / service if present in matchers (best-effort).
	var namespace, service *string
	if v, ok := matchers["namespace"]; ok {
		namespace = &v
	}
	if v, ok := matchers["service"]; ok {
		service = &v
	} else if v, ok := matchers["deployment"]; ok {
		service = &v
	} else if v, ok := matchers["proxy"]; ok {
		service = &v
	}

	var promql *string
	if v, ok := a.Annotations["garuda_promql"]; ok && v != "" {
		promql = &v
	}

	now := time.Now().UTC()
	winStart := tsStarted.Add(-5 * time.Minute)
	winEnd := now

	return &schema.AnomalySignal{
		SchemaVersion: schema.SchemaVersion,
		AnomalyID:     uuid.NewString(),
		Tier:          1,
		Detector:      detector,
		TsDetected:    now,
		TsStarted:     tsStarted,
		TsResolved:    tsResolved,
		Fingerprint:   fp,
		Metric: schema.Metric{
			Name:     metricName,
			Matchers: matchers,
		},
		Cluster:    cluster,
		Namespace:  namespace,
		Service:    service,
		Severity:   severity,
		Confidence: 1.0,
		Values: map[string]any{
			"observed":   nil, // AM doesn't include the observed value; promql is the path to fetch it
			"duration_s": now.Sub(tsStarted).Seconds(),
		},
		Evidence: schema.Evidence{
			PromQL: promql,
			Window: schema.Window{
				Start: winStart,
				End:   winEnd,
			},
		},
		SourceLabels: a.Labels,
		Annotations:  a.Annotations,
	}, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// =============================================================================
// Alertmanager webhook payload types
// =============================================================================

type amWebhookPayload struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	Status            string            `json:"status"` // firing | resolved
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []amAlert         `json:"alerts"`
}

type amAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"` // AM's own fingerprint; we recompute ours
}
