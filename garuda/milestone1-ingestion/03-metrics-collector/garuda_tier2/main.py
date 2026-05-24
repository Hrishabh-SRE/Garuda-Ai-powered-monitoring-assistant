"""Entrypoint — wires the poller, detector and publisher together."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from datetime import timedelta

from prometheus_client import Counter, Gauge, start_http_server
from pythonjsonlogger import jsonlogger

from .config import Config, load_slis
from .detectors import StlIsoForestDetector
from .kafka_publisher import Publisher
from .poller import Sample, ThanosPoller
from .schema import (
    AnomalySignal,
    Evidence,
    Metric,
    Window,
    fingerprint,
    new_anomaly_id,
    now_utc,
)

logger = logging.getLogger("garuda.tier2")


# ---------------------------------------------------------------------------
# Prometheus metrics (so the system-prom shard scrapes us)
# ---------------------------------------------------------------------------

POLLS_TOTAL = Counter("garuda_tier2_polls_total", "Total Thanos poll cycles.")
POLL_ERRORS = Counter("garuda_tier2_poll_errors_total", "Thanos poll errors.")
SAMPLES_TOTAL = Counter("garuda_tier2_samples_received_total", "Samples received from poller.")
SCORED_TOTAL = Counter("garuda_tier2_samples_scored_total", "Samples that produced a score.")
EMITTED_TOTAL = Counter(
    "garuda_tier2_anomalies_emitted_total",
    "AnomalySignal records published to Kafka.",
    labelnames=("severity",),
)
SUPPRESSED_COOLDOWN = Counter(
    "garuda_tier2_anomalies_suppressed_cooldown_total",
    "Anomalies dropped because the stream is in cooldown.",
)
PUBLISH_ERRORS = Counter(
    "garuda_tier2_publish_errors_total",
    "Kafka produce errors.",
)
ACTIVE_STREAMS = Gauge(
    "garuda_tier2_active_streams",
    "Number of stream keys with live state.",
)


def _stream_key(sli_id: str, labels: dict[str, str]) -> str:
    items = sorted(labels.items())
    return f"{sli_id}|{','.join(f'{k}={v}' for k, v in items)}"


def _hoist(labels: dict[str, str], cluster_default: str) -> tuple[str, str | None, str | None]:
    cluster = labels.get("cluster") or cluster_default
    namespace = labels.get("namespace")
    service = labels.get("service") or labels.get("deployment") or labels.get("proxy")
    return cluster, namespace, service


async def _consume(
    cfg: Config,
    queue: asyncio.Queue[Sample],
    detector: StlIsoForestDetector,
    publisher: Publisher,
) -> None:
    while True:
        sample: Sample = await queue.get()
        SAMPLES_TOTAL.inc()
        try:
            stream_key = _stream_key(sample.sli_id, sample.labels)
            cluster, namespace, service = _hoist(sample.labels, cfg.cluster)

            # Cooldown gate first — cheap and avoids a fit/score for nothing.
            if detector.in_cooldown(stream_key, sample.ts):
                SUPPRESSED_COOLDOWN.inc()
                continue

            score = detector.update(stream_key, sample.ts, sample.value)
            ACTIVE_STREAMS.set(len(detector._streams))   # noqa: SLF001

            if score is None:
                continue
            SCORED_TOTAL.inc()
            if not score.is_anomaly:
                continue
            assert score.severity is not None  # noqa: S101

            detector_name = f"ml:isoforest:{sample.sli_id}"
            fp = fingerprint(detector_name, sample.metric, sample.labels, cluster)

            # Re-check cooldown right before emit (race-free against multiple
            # samples landing on the same stream in this cycle).
            if detector.in_cooldown(stream_key, sample.ts):
                SUPPRESSED_COOLDOWN.inc()
                continue
            detector.mark_emitted(stream_key, sample.ts)

            sig = AnomalySignal(
                tier=2,
                detector=detector_name,
                ts_detected=now_utc(),
                ts_started=sample.ts,
                fingerprint=fp,
                metric=Metric(name=sample.metric, matchers=sample.labels),
                cluster=cluster,
                namespace=namespace,
                service=service,
                severity=score.severity,
                confidence=round(score.confidence, 4),
                values={
                    "observed": sample.value,
                    "baseline_mean": round(score.baseline_mean, 6),
                    "baseline_std": round(score.baseline_std, 6),
                    "iso_score": round(score.iso_score, 6),
                    "residual": round(score.residual, 6),
                    "duration_s": 0.0,
                },
                evidence=Evidence(
                    promql=sample.promql,
                    window=Window(
                        start=sample.ts - timedelta(minutes=5),
                        end=sample.ts,
                    ),
                ),
                source_labels=sample.labels,
                annotations={},
            )
            try:
                await publisher.publish(sig)
                EMITTED_TOTAL.labels(severity=score.severity).inc()
            except Exception as e:                       # noqa: BLE001
                PUBLISH_ERRORS.inc()
                logger.warning("publish failed: %s", e)
        finally:
            queue.task_done()


async def _amain() -> None:
    cfg = Config.from_env()
    slis = load_slis(cfg.slis_path)
    if not slis:
        logger.error("no SLIs loaded from %s; exiting", cfg.slis_path)
        sys.exit(2)
    logger.info("loaded %d SLIs from %s", len(slis), cfg.slis_path)

    publisher = Publisher(cfg)
    await publisher.start()

    queue: asyncio.Queue[Sample] = asyncio.Queue(maxsize=10_000)
    poller = ThanosPoller(cfg, slis, queue)

    detector = StlIsoForestDetector(
        n_estimators=cfg.iso_n_estimators,
        contamination=cfg.iso_contamination,
        baseline_window_s=86_400,
        sample_interval_s=cfg.poll_interval_s,
        seasonality_period=60,
        refit_every_s=cfg.refit_every_s,
        cooldown_after_emit_s=cfg.cooldown_after_emit_s,
    )

    # Metrics server. The detector counters live on the Prometheus client
    # registry; the poller counters are mirrored once per cycle.
    port = int(cfg.listen_addr.lstrip(":"))
    start_http_server(port)
    logger.info("metrics on :%d", port)

    poller_task = asyncio.create_task(poller.run(), name="poller")
    consumer_task = asyncio.create_task(_consume(cfg, queue, detector, publisher), name="consumer")

    async def _mirror_poller_counters() -> None:
        last = (0, 0, 0)
        while True:
            await asyncio.sleep(5)
            cur = (poller.polls_total, poller.poll_errors_total, poller.samples_emitted_total)
            for prom_metric, prev_v, cur_v in (
                (POLLS_TOTAL, last[0], cur[0]),
                (POLL_ERRORS, last[1], cur[1]),
                (SAMPLES_TOTAL, 0, 0),
            ):
                if cur_v > prev_v:
                    prom_metric.inc(cur_v - prev_v)
            last = cur

    mirror_task = asyncio.create_task(_mirror_poller_counters(), name="metrics-mirror")

    # Graceful shutdown.
    stop = asyncio.Event()

    def _on_signal(*_):                                  # noqa: ANN001
        logger.info("shutdown requested")
        stop.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, _on_signal)

    await stop.wait()
    await poller.stop()
    poller_task.cancel()
    consumer_task.cancel()
    mirror_task.cancel()
    for t in (poller_task, consumer_task, mirror_task):
        try:
            await t
        except asyncio.CancelledError:
            pass
    await publisher.stop()


def cli() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    try:
        import uvloop  # type: ignore
        uvloop.install()
    except ImportError:
        pass

    asyncio.run(_amain())


if __name__ == "__main__":
    cli()
