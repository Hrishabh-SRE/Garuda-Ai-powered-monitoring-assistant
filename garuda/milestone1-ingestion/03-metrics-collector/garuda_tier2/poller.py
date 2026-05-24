"""Thanos Query poller.

Every ``poll_interval_s`` seconds, hits Thanos Query's instant-query API for
each curated SLI. Each Thanos result series becomes a ``Sample`` event that
the detector consumes.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp

from .config import SLI, Config

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Sample:
    """One time-series observation extracted from a Thanos result row."""

    sli_id: str
    metric: str           # the underlying Prometheus metric, for the AnomalySignal
    promql: str           # the rule expression, for the AnomalySignal evidence
    labels: dict[str, str]  # series labels (cluster, namespace, ...) used for fingerprint
    ts: datetime
    value: float


class ThanosPoller:
    """Polls Thanos Query and pushes Samples into a queue."""

    def __init__(self, cfg: Config, slis: list[SLI], out_queue: asyncio.Queue[Sample]):
        self.cfg = cfg
        self.slis = slis
        self.q = out_queue
        self._stop = asyncio.Event()
        # Counters surface as Prometheus metrics in main.py; we expose simple
        # attributes to be picked up.
        self.polls_total = 0
        self.poll_errors_total = 0
        self.samples_emitted_total = 0

    async def run(self) -> None:
        """Run forever (until :meth:`stop` is called)."""
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.cfg.poll_timeout_s),
        ) as session:
            while not self._stop.is_set():
                start = asyncio.get_running_loop().time()
                try:
                    await self._poll_once(session)
                except Exception as e:                    # noqa: BLE001
                    self.poll_errors_total += 1
                    logger.exception("poll cycle failed: %s", e)
                self.polls_total += 1

                # Drift-free interval — sleep for whatever's left of the cycle.
                elapsed = asyncio.get_running_loop().time() - start
                wait = max(0.5, self.cfg.poll_interval_s - elapsed)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass

    async def stop(self) -> None:
        self._stop.set()

    async def _poll_once(self, session: aiohttp.ClientSession) -> None:
        ts = datetime.now(tz=timezone.utc)
        # Fan out one HTTP call per SLI so a single slow query doesn't block
        # the others. Bounded gather (gather_concurrency could be added if
        # the SLI list grows past ~100).
        await asyncio.gather(
            *(self._poll_sli(session, sli, ts) for sli in self.slis),
            return_exceptions=False,
        )

    async def _poll_sli(self, session: aiohttp.ClientSession, sli: SLI, ts: datetime) -> None:
        url = self.cfg.thanos_query_url.rstrip("/") + "/api/v1/query"
        params = {"query": sli.promql, "time": ts.timestamp()}
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except Exception as e:                            # noqa: BLE001
            self.poll_errors_total += 1
            logger.warning("SLI %s poll failed: %s", sli.id, e)
            return

        if data.get("status") != "success":
            self.poll_errors_total += 1
            logger.warning("SLI %s returned non-success status: %s", sli.id, data)
            return

        result = data.get("data", {}).get("result", [])
        for series in result:
            labels = series.get("metric", {}) or {}
            v = series.get("value")
            if not v or len(v) != 2:
                continue
            try:
                val = float(v[1])
            except (TypeError, ValueError):
                continue
            if val != val:        # NaN
                continue

            # Use the timestamp Thanos returned to keep the detector's clock
            # tied to the data, not to wall time.
            sample_ts = datetime.fromtimestamp(float(v[0]), tz=timezone.utc)

            await self.q.put(
                Sample(
                    sli_id=sli.id,
                    metric=sli.metric,
                    promql=sli.promql,
                    labels=labels,
                    ts=sample_ts,
                    value=val,
                )
            )
            self.samples_emitted_total += 1
