"""STL + Isolation Forest detector.

Per stream we maintain a rolling buffer of recent samples. On each new
sample:

1. If the buffer is too short for the configured seasonality, just append
   and emit no signal.
2. Decompose with STL → residual. The residual is what the isolation
   forest scores.
3. Refit the IsolationForest periodically (``refit_every_s``) on the most
   recent residuals.
4. Score the new sample's residual; convert to ``[0, 1]`` confidence;
   look up severity per the SLI's thresholds.

Why this combination:
- STL removes daily / weekly periodicity so a "Tuesday-noon spike" doesn't
  fire on every Tuesday.
- IsolationForest is robust to non-Gaussian residuals and gives a
  monotone-ish score that's easy to threshold.

We do NOT use STL alone (z-score on residual) because some metrics have
heavy-tailed residual distributions where even a 6σ event isn't unusual.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StreamState:
    """Per-stream state carried across samples."""

    samples: list[tuple[datetime, float]] = field(default_factory=list)  # most recent first
    last_fit_at: datetime | None = None
    model: IsolationForest | None = None
    last_residual_mean: float = 0.0
    last_residual_std: float = 1.0
    last_emit_at: datetime | None = None


@dataclass(slots=True)
class Score:
    """Detector output for a single sample."""

    is_anomaly: bool
    confidence: float                     # 0..1; threshold-able
    iso_score: float                      # raw IsolationForest score (-1..1ish)
    residual: float
    baseline_mean: float
    baseline_std: float
    severity: str | None                  # 'info'|'warning'|'critical' or None


class StlIsoForestDetector:
    """Stateful detector. One instance keeps state for many streams.

    Stream identity = ``(sli_id, frozenset(labels.items()))``. Same
    fingerprint policy as the schema/correlator so emissions land on the
    same partition + dedup correctly.
    """

    def __init__(
        self,
        *,
        n_estimators: int = 100,
        contamination: float = 0.01,
        baseline_window_s: int = 86_400,
        sample_interval_s: int = 60,
        seasonality_period: int = 60,
        refit_every_s: int = 3600,
        cooldown_after_emit_s: int = 300,
        min_samples_for_fit: int = 120,
    ):
        self.n_estimators = n_estimators
        self.contamination = contamination
        self.baseline_window_s = baseline_window_s
        self.sample_interval_s = sample_interval_s
        self.seasonality_period = seasonality_period
        self.refit_every_s = refit_every_s
        self.cooldown_after_emit_s = cooldown_after_emit_s
        self.min_samples_for_fit = min_samples_for_fit
        self._streams: dict[str, StreamState] = {}

    def update(self, stream_key: str, ts: datetime, value: float) -> Score | None:
        """Feed one sample; return a Score if confident, else None."""
        s = self._streams.setdefault(stream_key, StreamState())

        # Append + retire old samples.
        s.samples.append((ts, value))
        cutoff = ts - timedelta(seconds=self.baseline_window_s)
        s.samples = [(t, v) for (t, v) in s.samples if t >= cutoff]

        # Need enough history before we attempt anything.
        if len(s.samples) < self.min_samples_for_fit:
            return None

        # Refit if it's been long enough since last fit.
        needs_fit = (
            s.model is None
            or s.last_fit_at is None
            or (ts - s.last_fit_at).total_seconds() >= self.refit_every_s
        )
        if needs_fit:
            try:
                self._refit(s)
            except Exception as e:                       # noqa: BLE001
                logger.warning("refit failed for stream %s: %s", stream_key, e)
                return None

        if s.model is None:
            return None

        residual = value - s.last_residual_mean
        # Score the latest sample only (use a 1-row 2D array).
        iso = float(s.model.score_samples(np.array([[residual]]))[0])
        # IsolationForest returns *higher = less anomalous*. Invert and
        # squash to (0, 1) via a logistic. Empirically iso scores cluster
        # around -0.5 (normal) to -0.7+ (anomalous).
        confidence = 1.0 / (1.0 + math.exp(8.0 * (iso + 0.55)))

        severity = self._severity_for(confidence, default_thresholds={
            0.9: "critical", 0.7: "warning", 0.5: "info",
        })

        return Score(
            is_anomaly=(severity is not None),
            confidence=confidence,
            iso_score=iso,
            residual=residual,
            baseline_mean=s.last_residual_mean,
            baseline_std=s.last_residual_std,
            severity=severity,
        )

    def mark_emitted(self, stream_key: str, ts: datetime) -> None:
        s = self._streams.get(stream_key)
        if s is not None:
            s.last_emit_at = ts

    def in_cooldown(self, stream_key: str, ts: datetime) -> bool:
        s = self._streams.get(stream_key)
        if s is None or s.last_emit_at is None:
            return False
        return (ts - s.last_emit_at).total_seconds() < self.cooldown_after_emit_s

    # ------------------------------------------------------------------

    def _refit(self, s: StreamState) -> None:
        """Recompute STL residual stats + retrain IsolationForest."""
        ts_axis = np.array([t.timestamp() for (t, _) in s.samples], dtype=float)
        vals = np.array([v for (_, v) in s.samples], dtype=float)

        # Cheap, stable baseline: median over the last `seasonality_period`
        # points. Using a real STL decomposition is great when we have lots
        # of data, but for the first day of any stream we only have a few
        # hundred points and STL is unstable then. We can swap in a real
        # statsmodels.tsa.STL fit (period=seasonality_period) once the
        # buffer has > 2 * period samples.
        if len(vals) >= 2 * self.seasonality_period:
            try:
                from statsmodels.tsa.seasonal import STL  # local import: heavy
                stl = STL(vals, period=self.seasonality_period, robust=True).fit()
                residuals = stl.resid
                baseline = stl.trend + stl.seasonal
                s.last_residual_mean = float(np.nanmean(baseline[-self.seasonality_period:]))
            except Exception:                            # noqa: BLE001
                residuals = vals - float(np.median(vals))
                s.last_residual_mean = float(np.median(vals))
        else:
            residuals = vals - float(np.median(vals))
            s.last_residual_mean = float(np.median(vals))

        residuals = residuals[~np.isnan(residuals)]
        if residuals.size == 0:
            s.model = None
            return

        s.last_residual_std = float(np.nanstd(residuals)) or 1.0

        model = IsolationForest(
            n_estimators=self.n_estimators,
            contamination=self.contamination,
            random_state=42,
        )
        model.fit(residuals.reshape(-1, 1))
        s.model = model
        s.last_fit_at = datetime.fromtimestamp(float(ts_axis[-1]), tz=s.samples[-1][0].tzinfo)

    @staticmethod
    def _severity_for(confidence: float, default_thresholds: dict[float, str]) -> str | None:
        # Walk thresholds in descending order so 'critical' wins over 'warning'.
        for threshold in sorted(default_thresholds.keys(), reverse=True):
            if confidence >= threshold:
                return default_thresholds[threshold]
        return None
