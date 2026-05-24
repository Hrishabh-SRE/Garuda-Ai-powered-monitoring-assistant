"""Runtime config + curated SLI list parsing."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class SLI:
    """One curated metric stream — what the poller asks Thanos for and how we name it.

    Attributes:
        id: short unique handle, e.g. ``apiserver_5xx``. Used in detector key.
        promql: PromQL expression as you'd type it into Thanos Query. Should
            return one or more series (we treat each result series as its own
            stream, keyed by labels).
        metric: the underlying metric name we record in the AnomalySignal.
        severity_thresholds: ``{score: severity_label}`` — driven by the
            isolation-forest score after STL detrending. Default thresholds
            below; override per-SLI when needed.
        baseline_window_s: how much history to retain in memory for the
            STL baseline. Default 24h.
        sample_interval_s: spacing of points fed to the detector. Should
            match the poller interval.
        seasonality_period: STL seasonal period in samples (e.g. 60 for
            daily seasonality at 1m sampling × 24h would be 1440 — but we
            generally want short seasonalities for fast warm-up).
    """

    id: str
    promql: str
    metric: str
    severity_thresholds: dict[float, str] = field(
        default_factory=lambda: {0.9: "critical", 0.7: "warning", 0.5: "info"}
    )
    baseline_window_s: int = 86_400
    sample_interval_s: int = 60
    seasonality_period: int = 60


@dataclass
class Config:
    """Process-level config. All values come from env or the SLIs YAML."""

    listen_addr: str = ":9090"               # /metrics for Prometheus
    thanos_query_url: str = ""               # http://thanos-querier.monitoring.svc.cluster.local:9090
    poll_interval_s: int = 60
    poll_timeout_s: int = 30
    slis_path: str = "/etc/garuda-tier2/slis.yaml"
    cluster: str = "pac-platformcluster01"   # used in fingerprints when the series doesn't carry it

    kafka_brokers: list[str] = field(default_factory=list)
    kafka_topic: str = "garuda.anomalies.tier2"
    kafka_user: str = ""
    kafka_pass: str = ""
    kafka_auth_required: bool = False
    kafka_sasl_mech: str = "PLAIN"  # "PLAIN" (Confluent Cloud) or "SCRAM-SHA-512"
    kafka_use_tls: bool = False     # TLS to broker (required for Confluent Cloud)

    # Detector tuning — global defaults, overridable per-SLI.
    iso_n_estimators: int = 100
    iso_contamination: float = 0.01
    refit_every_s: int = 3600                # rebuild baseline every hour
    cooldown_after_emit_s: int = 300         # suppress repeats per fingerprint

    @classmethod
    def from_env(cls) -> "Config":
        c = cls()
        c.listen_addr = os.getenv("LISTEN_ADDR", c.listen_addr)
        c.thanos_query_url = os.getenv("THANOS_QUERY_URL", c.thanos_query_url)
        if not c.thanos_query_url:
            raise RuntimeError("THANOS_QUERY_URL is required")
        c.poll_interval_s = int(os.getenv("POLL_INTERVAL_S", c.poll_interval_s))
        c.poll_timeout_s = int(os.getenv("POLL_TIMEOUT_S", c.poll_timeout_s))
        c.slis_path = os.getenv("SLIS_PATH", c.slis_path)
        c.cluster = os.getenv("CLUSTER", c.cluster)

        brokers = os.getenv("KAFKA_BROKERS", "")
        if not brokers:
            raise RuntimeError("KAFKA_BROKERS is required (comma-separated host:port)")
        c.kafka_brokers = [b.strip() for b in brokers.split(",") if b.strip()]
        c.kafka_topic = os.getenv("KAFKA_TOPIC", c.kafka_topic)
        c.kafka_user = os.getenv("KAFKA_SASL_USER", "")
        c.kafka_pass = os.getenv("KAFKA_SASL_PASS", "")
        c.kafka_auth_required = os.getenv("KAFKA_AUTH_REQUIRED", "false").lower() == "true"
        if c.kafka_auth_required and not c.kafka_user:
            raise RuntimeError("KAFKA_SASL_USER required when KAFKA_AUTH_REQUIRED=true")
        c.kafka_sasl_mech = os.getenv("KAFKA_SASL_MECHANISM", c.kafka_sasl_mech)
        # TLS defaults to authRequired (matches bridge behaviour: Confluent Cloud =
        # SASL_SSL + PLAIN, so when auth is on, TLS is on too).
        tls_default = "true" if c.kafka_auth_required else "false"
        c.kafka_use_tls = os.getenv("KAFKA_USE_TLS", tls_default).lower() == "true"

        c.iso_n_estimators = int(os.getenv("ISO_N_ESTIMATORS", c.iso_n_estimators))
        c.iso_contamination = float(os.getenv("ISO_CONTAMINATION", c.iso_contamination))
        c.refit_every_s = int(os.getenv("REFIT_EVERY_S", c.refit_every_s))
        c.cooldown_after_emit_s = int(os.getenv("COOLDOWN_AFTER_EMIT_S", c.cooldown_after_emit_s))
        return c


def load_slis(path: str) -> list[SLI]:
    """Parse the SLIs YAML into ``SLI`` objects."""
    raw = yaml.safe_load(Path(path).read_text())
    items = raw.get("slis", [])
    out: list[SLI] = []
    for d in items:
        out.append(
            SLI(
                id=d["id"],
                promql=d["promql"],
                metric=d["metric"],
                severity_thresholds=d.get(
                    "severity_thresholds",
                    {0.9: "critical", 0.7: "warning", 0.5: "info"},
                ),
                baseline_window_s=int(d.get("baseline_window_s", 86_400)),
                sample_interval_s=int(d.get("sample_interval_s", 60)),
                seasonality_period=int(d.get("seasonality_period", 60)),
            )
        )
    return out
