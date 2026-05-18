# 00 — Kafka Topics on chitragupta Cluster

**Goal:** create the `garuda.*` topics we need for Milestone 1, declared once and applied idempotently.

**Target Kafka:** chitragupta self-hosted, brokers `10.12.0.219-230:9092` (12 brokers, internal VLAN).

---

## Files

| File | Purpose |
|---|---|
| `topics.yaml` | Declarative spec — single source of truth |
| `create-topics.sh` | Idempotent applier (uses `kafka-topics.sh` + `yq`) |

---

## Apply (this milestone — Phase 1a only)

Phase 1a creates only the two anomaly topics:

```bash
export BOOTSTRAP=10.12.0.219:9092
export CMD_CFG=/etc/kafka/garuda-client.properties   # if SASL is enabled
./create-topics.sh
```

Dry-run first to print what would happen:

```bash
DRY_RUN=1 BOOTSTRAP=10.12.0.219:9092 ./create-topics.sh
```

Apply Phase 1b topics (logs + DLQ) when the log shaper is ready:

```bash
MILESTONE=1b BOOTSTRAP=10.12.0.219:9092 ./create-topics.sh
```

Apply Phase 2 topics (events + changes):

```bash
MILESTONE=2 BOOTSTRAP=10.12.0.219:9092 ./create-topics.sh
```

---

## Phase 1a topics (created now)

| Topic | Partitions | Retention | Producer |
|---|---|---|---|
| `garuda.anomalies.tier1` | 3 | 7d, 10 GiB cap | Alertmanager bridge (component 02) |
| `garuda.anomalies.tier2` | 3 | 7d, 10 GiB cap | ML worker (component 03) |

3 partitions is intentionally small — anomaly volume is bounded (rule fires + ML detections per minute). Increase later if a single-partition consumer becomes a bottleneck for the correlation engine.

`min.insync.replicas=2` + `acks=all` (set on the producer) gives durable writes that tolerate one broker outage.

---

## ACL ask for the Kafka admin team

Two principals to provision (see `topics.yaml` for the full plan):

- **`garuda-producer`** — `WRITE`, `DESCRIBE` on prefix `garuda.`
- **`garuda-consumer`** — `READ`, `DESCRIBE` on prefix `garuda.`; `READ` on group prefix `garuda.`

Auth: SASL/SCRAM-SHA-512 preferred. Credentials delivered to:
- `garuda-ingest/garuda-kafka-credentials` (Kubernetes Secret) on `pac-platformcluster01` (and any other cluster running a Garuda producer/consumer)

Until SASL is set up, the cluster's existing plaintext-on-VPC posture is acceptable for *bring-up* — but every Garuda producer must be hard-coded to refuse plaintext if `KAFKA_AUTH_REQUIRED=true` env var is set, which we'll flip when credentials land.

---

## Verification

After apply, confirm topics exist with the expected configs:

```bash
kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --describe --topic garuda.anomalies.tier1
kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --describe --topic garuda.anomalies.tier2

# spot-check configs
kafka-configs.sh --bootstrap-server "${BOOTSTRAP}" --describe --entity-type topics \
  --entity-name garuda.anomalies.tier1
```

Expect: 3 partitions, RF=3, `retention.ms=604800000`, `min.insync.replicas=2`, `compression.type=zstd`.

End-to-end smoke test (after the Alertmanager bridge is deployed in component 02):

```bash
# tail tier1 — should see one record per AM webhook
kcat -b "${BOOTSTRAP}" -t garuda.anomalies.tier1 -C -o -5 | jq .
```
