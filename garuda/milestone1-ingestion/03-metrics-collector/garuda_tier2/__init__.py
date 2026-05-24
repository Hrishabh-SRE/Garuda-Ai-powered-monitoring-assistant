"""Garuda Tier 2 metrics collector.

Two coroutines run concurrently:

* :class:`poller.ThanosPoller` — every poll_interval_s, pulls the curated
  SLI list from Thanos Query, fans out into one ``MetricStream`` per
  series.
* :class:`detector.MLDetector` — owns per-stream STL baseline + Isolation
  Forest state, evaluates each new sample, and emits AnomalySignal
  envelopes to Kafka when the score crosses threshold.

The schema is the same canonical envelope used by the Tier 1 bridge —
see _shared/schemas/anomaly_signal.schema.json (v1).
"""

__version__ = "0.1.0"
