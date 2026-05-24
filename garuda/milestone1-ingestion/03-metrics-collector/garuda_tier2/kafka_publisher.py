"""Kafka publisher for AnomalySignal records.

Uses aiokafka. Idempotent producer with acks=all + zstd compression.
Keys every record by ``AnomalySignal.fingerprint`` so re-emissions of
the same anomaly land on the same partition.

Transport selection mirrors the Tier 1 bridge so both producers can talk
to the same broker:

  * ``use_tls=True``  + SASL creds → ``SASL_SSL``  (Confluent Cloud)
  * ``use_tls=False`` + SASL creds → ``SASL_PLAINTEXT``
  * ``use_tls=True``  + no creds   → ``SSL``
  * ``use_tls=False`` + no creds   → ``PLAINTEXT`` (only when not auth-required)
"""

from __future__ import annotations

import logging
import ssl
from typing import Optional

from aiokafka import AIOKafkaProducer

from .config import Config
from .schema import AnomalySignal

logger = logging.getLogger(__name__)


def _security_protocol(*, use_tls: bool, has_sasl: bool) -> str:
    if has_sasl and use_tls:
        return "SASL_SSL"
    if has_sasl:
        return "SASL_PLAINTEXT"
    if use_tls:
        return "SSL"
    return "PLAINTEXT"


class Publisher:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        has_sasl = bool(self.cfg.kafka_user)
        if self.cfg.kafka_auth_required and not has_sasl:
            raise RuntimeError("KAFKA_AUTH_REQUIRED=true but no SASL credentials provided")

        proto = _security_protocol(use_tls=self.cfg.kafka_use_tls, has_sasl=has_sasl)

        kw: dict = {
            "bootstrap_servers": self.cfg.kafka_brokers,
            "client_id": "garuda-tier2",
            "acks": "all",
            "enable_idempotence": True,
            "compression_type": "zstd",
            "max_batch_size": 16 * 1024,
            "linger_ms": 50,
            "security_protocol": proto,
        }

        if self.cfg.kafka_use_tls:
            # Default trust store (system CAs). For self-signed managed clusters,
            # mount the CA bundle and set SSL_CAFILE env separately.
            kw["ssl_context"] = ssl.create_default_context()

        if has_sasl:
            kw.update({
                "sasl_mechanism": self.cfg.kafka_sasl_mech,
                "sasl_plain_username": self.cfg.kafka_user,
                "sasl_plain_password": self.cfg.kafka_pass,
            })

        logger.info(
            "kafka producer init: brokers=%s topic=%s security_protocol=%s sasl=%s",
            self.cfg.kafka_brokers,
            self.cfg.kafka_topic,
            proto,
            self.cfg.kafka_sasl_mech if has_sasl else "none",
        )

        self._producer = AIOKafkaProducer(**kw)
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def publish(self, sig: AnomalySignal) -> None:
        if self._producer is None:
            raise RuntimeError("publisher not started")
        body = sig.to_kafka_bytes()
        await self._producer.send_and_wait(
            self.cfg.kafka_topic,
            value=body,
            key=sig.fingerprint.encode("utf-8"),
        )
