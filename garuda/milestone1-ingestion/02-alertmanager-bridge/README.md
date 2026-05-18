# 02 — Alertmanager → Kafka Bridge

**Goal:** receive Alertmanager webhook payloads from every workload cluster's per-cluster Alertmanager, transform each `garuda_anomaly=true` alert into the canonical `AnomalySignal` envelope, and produce to `garuda.anomalies.tier1` on chitragupta Kafka.

**Why it exists:** Alertmanager has no native Kafka producer, and we don't want to fan-out the bridge logic across every per-cluster AM. Centralizing this on the platform cluster means AM config is dumb (one webhook URL) and the data shape lives in one Go service we control.

---

## Architecture

```
[per-cluster Prometheus]            [per-cluster Alertmanager]               [platform cluster]
   garuda-tier1-* rules    ──fire──▶  monitoring-kube-prometheus-am  ──webhook▶  garuda-am-bridge
   ↑ component 01                                                                  │
                                                                                   │ produce
                                                                                   ▼
                                                                          [chitragupta Kafka]
                                                                          garuda.anomalies.tier1
```

- One bridge Deployment on `pac-platformcluster01` (replicas=2, anti-affinity, PDB).
- Exposed via Azure internal LoadBalancer; per-cluster AMs hit it cross-cluster.
- Schema: `_shared/schemas/anomaly_signal.schema.json` (v1).

---

## Files

```
02-alertmanager-bridge/
├── README.md                          # this file
├── go.mod
├── Dockerfile                         # multi-stage, distroless static
├── cmd/bridge/main.go                 # entrypoint (HTTP servers + producer wiring)
├── internal/
│   ├── config/config.go               # env-var parsing
│   ├── handler/
│   │   ├── handler.go                 # AM payload → AnomalySignal
│   │   └── handler_test.go            # unit tests
│   ├── kafka/kafka.go                 # franz-go producer wrapper
│   └── schema/
│       ├── schema.go                  # AnomalySignal struct + Fingerprint helper
│       └── schema_test.go             # determinism tests for fingerprint
├── helm/
│   ├── Chart.yaml
│   ├── values.yaml                    # default values
│   └── templates/
│       ├── _helpers.tpl
│       ├── deployment.yaml            # 2 replicas, distroless, ro fs
│       ├── service.yaml               # internal LB on :8080, ClusterIP for :9090
│       ├── servicemonitor.yaml        # release: monitoring → scraped by system-prom
│       └── pdb.yaml
└── deploy/
    └── alertmanager-config-delta.yaml # snippet to merge into per-cluster AM Helm values
```

---

## Configuration (env vars)

Set via the Helm chart; explicit list for documentation:

| Var | Default | Purpose |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | Webhook listener |
| `METRICS_ADDR` | `:9090` | Prometheus metrics listener (separate so we can scope auth on /webhook later) |
| `KAFKA_BROKERS` | (required) | Comma-separated `host:port` list |
| `KAFKA_TOPIC` | `garuda.anomalies.tier1` | Output topic |
| `KAFKA_AUTH_REQUIRED` | `false` | When true, refuse to start without SASL creds |
| `KAFKA_SASL_USER` | (optional) | SASL/SCRAM-SHA-512 username from secret |
| `KAFKA_SASL_PASS` | (optional) | SASL/SCRAM-SHA-512 password from secret |
| `WEBHOOK_SHARED_SECRET` | (optional) | If set, requests must carry `X-Garuda-Bridge-Secret` header |
| `SHUTDOWN_GRACE_MS` | `10000` | Graceful shutdown drain time |

---

## Metrics exposed (`/metrics`)

| Metric | Type | Meaning |
|---|---|---|
| `garuda_am_bridge_alerts_received_total` | counter | Every alert in every webhook batch |
| `garuda_am_bridge_alerts_filtered_total` | counter | Dropped because `garuda_anomaly!=true` (defense-in-depth) |
| `garuda_am_bridge_alerts_invalid_total` | counter | Could not be transformed (missing alertname/cluster) |
| `garuda_am_bridge_alerts_published_total` | counter | Successfully published to Kafka |
| `garuda_am_bridge_publish_errors_total` | counter | Kafka produce error |

Plus standard `process_*` and `go_*` collectors.

Alert on:

- `rate(garuda_am_bridge_publish_errors_total[5m]) > 0` — Kafka path broken
- `rate(garuda_am_bridge_alerts_invalid_total[15m]) / rate(garuda_am_bridge_alerts_received_total[15m]) > 0.05` — schema drift somewhere upstream

---

## Build + push image

```bash
cd 02-alertmanager-bridge
docker buildx build --platform=linux/amd64 \
  -t artifactory-ci.myntra.com/prodhub/garuda/alertmanager-bridge:0.1.0 \
  --push .
```

Until Garuda has its own Artifactory project, reuse the existing `prodhub` namespace (same as `prometheus`/`thanos` images). Coordinate with the Artifactory admin for a `prodhub/garuda/*` path prefix.

---

## Deploy on the platform cluster

```bash
# 1. Namespace + RBAC
kubectl --context pac-platformcluster01 create namespace garuda-ingest

# 2. (When Kafka SASL creds arrive) create the credential secret
kubectl --context pac-platformcluster01 -n garuda-ingest create secret generic \
  garuda-kafka-credentials \
  --from-literal=username=garuda-producer \
  --from-literal=password=<from-kafka-admin>

# 3. Install the bridge
helm --kube-context pac-platformcluster01 upgrade --install \
  garuda-am-bridge garuda/milestone1-ingestion/02-alertmanager-bridge/helm \
  -n garuda-ingest \
  --set kafka.existingSecret=garuda-kafka-credentials \
  --set kafka.authRequired=true   # flip true once SASL is in place

# 4. Read the LoadBalancer IP — this is what each AM webhook URL points to
kubectl --context pac-platformcluster01 -n garuda-ingest get svc garuda-am-bridge
# EXTERNAL-IP column is the internal-LB IP. Wire it into AM config (next step).
```

Until SASL is provisioned, leave `kafka.authRequired=false`. Don't go to production without flipping it.

---

## Wire each per-cluster Alertmanager

For each workload cluster (start with the pilot wave from component 01):

1. Open the cluster's kube-prometheus-stack Helm values.
2. Append the snippet from `deploy/alertmanager-config-delta.yaml` to the `alertmanager.config.receivers` and `alertmanager.config.route.routes` lists. **Preserve all existing entries** (email-sre, k8s-alerts-slack, zenduty-sre).
3. Replace the placeholder URL with the bridge's actual ILB IP/DNS.
4. `helm upgrade` the kube-prometheus-stack release on that cluster — Alertmanager hot-reloads its config; no pod restart needed.

---

## Tests

```bash
cd 02-alertmanager-bridge
go test ./...
```

Two test suites are included:

- `internal/schema/schema_test.go` — fingerprint determinism (matchers map order doesn't change output) and sensitivity (any input change → different fingerprint).
- `internal/handler/handler_test.go` — golden AM payload converts to a valid AnomalySignal; non-Garuda alerts are filtered; auth gate works.

---

## Failure modes considered

- **Kafka unreachable.** Producer returns error → `publish_errors_total` increments → bridge returns 200 to AM regardless (we don't want AM to retry storms; alerts will be re-sent on `repeat_interval`). The correlator uses fingerprint to dedupe.
- **Bridge restart mid-batch.** AM retries on next interval; the bridge is stateless. Producer uses `acks=all` + idempotency, so the same batch re-published after restart doesn't double-write — but we still expect occasional duplicates because AM's resend isn't deduplicated upstream. Fingerprint dedup handles this.
- **Malformed AM payload.** Per-alert errors increment `alerts_invalid_total` and the rest of the batch proceeds; whole-payload JSON parse failure returns 400.
- **Slow Kafka.** `ProduceSync` blocks; if the AM webhook times out we lose that batch. Acceptable for now (alerts re-send), but watch p99 latency on `/webhook` once we have signal volume.

---

## Open items

- [ ] CI: `go test ./...` + `go vet` + `gofmt` check on PR
- [ ] CI: docker buildx with provenance + SBOM
- [ ] Decide whether to add `X-Garuda-Bridge-Secret` (AM webhook_config doesn't natively support custom headers — needs a sidecar OR we accept "internal-LB only" as the trust boundary)
- [ ] DNS name for the bridge ILB — currently we paste the IP; ask platform networking for a static A record like `garuda-am-bridge.platform.myntra.com`
- [ ] Migrate from Azure ILB annotation to GCP equivalent when the cluster moves
