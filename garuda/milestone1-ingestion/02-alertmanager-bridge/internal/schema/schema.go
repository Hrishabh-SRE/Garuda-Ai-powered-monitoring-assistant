// Package schema defines the canonical AnomalySignal envelope and the
// fingerprint helper used by the Alertmanager bridge (Tier 1 producer).
//
// The schema mirrors _shared/schemas/anomaly_signal.schema.json (v1).
// Keep the two in sync; the Tier 2 Python worker has the equivalent
// Pydantic model in 03-metrics-collector/shared/schema.py.
package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// SchemaVersion is the wire-protocol version of the AnomalySignal envelope.
// Bump only on backward-incompatible changes.
const SchemaVersion = "v1"

// AnomalySignal is the canonical envelope written to garuda.anomalies.tier1.
// JSON tag order matches the JSON Schema for human-readable output.
type AnomalySignal struct {
	SchemaVersion string            `json:"schema_version"`
	AnomalyID     string            `json:"anomaly_id"`
	Tier          int               `json:"tier"`
	Detector      string            `json:"detector"`
	TsDetected    time.Time         `json:"ts_detected"`
	TsStarted     time.Time         `json:"ts_started"`
	TsResolved    *time.Time        `json:"ts_resolved,omitempty"`
	Fingerprint   string            `json:"fingerprint"`
	Metric        Metric            `json:"metric"`
	Cluster       string            `json:"cluster"`
	Namespace     *string           `json:"namespace,omitempty"`
	Service       *string           `json:"service,omitempty"`
	Severity      string            `json:"severity"`
	Confidence    float64           `json:"confidence"`
	Values        map[string]any    `json:"values"`
	Evidence      Evidence          `json:"evidence"`
	SourceLabels  map[string]string `json:"source_labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

// Metric describes the metric the anomaly relates to.
type Metric struct {
	Name     string            `json:"name"`
	Matchers map[string]string `json:"matchers"`
}

// Evidence holds the supporting context for the anomaly.
type Evidence struct {
	PromQL  *string  `json:"promql,omitempty"`
	Window  Window   `json:"window"`
	Samples [][2]any `json:"samples,omitempty"` // (timestamp string, value number)
}

// Window is the time window used to evaluate / observe the anomaly.
type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Fingerprint computes the deterministic 16-char hex hash documented in
// _shared/schemas/README.md.
//
//	input  = detector + "|" + metric_name + "|" + sorted_kv(matchers) + "|" + cluster
//	hash   = sha256(input)
//	output = first 16 hex chars
//
// Producers MUST use this exact algorithm so Tier 1 and Tier 2 detections
// of the same underlying issue collide on fingerprint and the correlator
// can fuse them.
func Fingerprint(detector, metricName string, matchers map[string]string, cluster string) string {
	keys := make([]string, 0, len(matchers))
	for k := range matchers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(matchers[k])
	}

	input := detector + "|" + metricName + "|" + sb.String() + "|" + cluster
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])[:16]
}
