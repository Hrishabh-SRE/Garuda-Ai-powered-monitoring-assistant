"""AnomalySignal envelope (Tier 2 producer).

Mirrors:
- internal/schema/schema.go (Go, Tier 1 bridge)
- _shared/schemas/anomaly_signal.schema.json (canonical JSON Schema)

The fingerprint algorithm here MUST match the Go implementation byte-for-byte
so a Tier 1 alert and a Tier 2 ML detection on the same underlying anomaly
collide on fingerprint and the correlator can fuse them.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = "v1"


def fingerprint(detector: str, metric_name: str, matchers: dict[str, str], cluster: str) -> str:
    """Deterministic 16-char hex hash. Same algo as Go's schema.Fingerprint.

    >>> fingerprint("d", "m", {"k": "v"}, "c") == fingerprint("d", "m", {"k": "v"}, "c")
    True
    """
    keys = sorted(matchers)
    parts = ",".join(f"{k}={matchers[k]}" for k in keys)
    inp = f"{detector}|{metric_name}|{parts}|{cluster}"
    return hashlib.sha256(inp.encode("utf-8")).hexdigest()[:16]


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def new_anomaly_id() -> str:
    return str(uuid.uuid4())


class Window(BaseModel):
    model_config = ConfigDict(extra="forbid")
    start: datetime
    end: datetime


class Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid")
    promql: str | None = None
    window: Window
    samples: list[tuple[datetime, float]] | None = None


class Metric(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    matchers: dict[str, str]


class AnomalySignal(BaseModel):
    """Canonical envelope. Field names + JSON shape match the schema file."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: str = SCHEMA_VERSION
    anomaly_id: str = Field(default_factory=new_anomaly_id)
    tier: int  # always 2 for this producer; left explicit for clarity
    detector: str
    ts_detected: datetime
    ts_started: datetime
    ts_resolved: datetime | None = None
    fingerprint: str
    metric: Metric
    cluster: str
    namespace: str | None = None
    service: str | None = None
    severity: str
    confidence: float = Field(ge=0.0, le=1.0)
    values: dict[str, Any] = Field(default_factory=dict)
    evidence: Evidence
    source_labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)

    def to_kafka_bytes(self) -> bytes:
        return self.model_dump_json(exclude_none=True).encode("utf-8")
