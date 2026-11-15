"""Kafka job-events producer for crawled (external) jobs.

Replaces the direct Elasticsearch write. Each job becomes one envelope on the
shared `job-events` topic, keyed by the job id, and search-svc indexes it through
its own consumer (src/Search.Infrastructure/Workers/JobEventsConsumer.cs).

The envelope mirrors SharedKernel's EventEnvelope<T> and is serialized with
KafkaJson's camelCase policy, so the .NET consumer binds it unchanged. The
payload is a crawler-specific contract (`job.external.upserted`): the job id is
sha256(source_url), which is not a Guid, so the SharedKernel JobCreatedEvent
cannot carry it. The golden fixture in tests/fixtures/ pins the wire shape on
both sides.
"""

import json
import logging
import os
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Protocol

from crawler.es_document import job_doc_id

logger = logging.getLogger("crawler.job_events")

EVENT_TYPE = "job.external.upserted"  # search-svc JobEventTypes / ExternalJobEventTypes
EVENT_VERSION = 1
DEFAULT_TOPIC = "job-events"  # SharedKernel JobEvents topic; same default as search-svc
PRODUCE_TIMEOUT_MS = 10_000  # SharedKernel producer message.timeout.ms
_FLUSH_TIMEOUT_S = 30.0

_PROTOCOLS: dict[str, str] = {
    "plaintext": "PLAINTEXT",
    "ssl": "SSL",
    "saslplaintext": "SASL_PLAINTEXT",
    "saslssl": "SASL_SSL",
}
_MECHANISMS: dict[str, str] = {
    "plain": "PLAIN",
    "scramsha256": "SCRAM-SHA-256",
    "scramsha512": "SCRAM-SHA-512",
}


def _stamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_external_job_upserted(
    item: Mapping[str, Any],
    *,
    event_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the camelCase envelope for one crawled job.

    `jobId` is sha256(source_url), the same id the crawler always used as the ES
    `_id`, so search-svc indexes the document under the same key. Keys whose value
    is None are kept as JSON null, so the wire shape is fixed.
    """
    moment = now or datetime.now(timezone.utc)
    source_url = item["source_url"]
    return {
        "eventId": event_id or str(uuid.uuid4()),
        "eventType": EVENT_TYPE,
        "version": EVENT_VERSION,
        "timestamp": _stamp(moment),
        "payload": {
            "jobId": job_doc_id(source_url),
            "sourceUrl": source_url,
            "title": item.get("title"),
            "companyName": item.get("company"),
            "location": item.get("location"),
            "salaryMin": item.get("salary_min"),
            "salaryMax": item.get("salary_max"),
            "currency": item.get("salary_currency") or "VND",
            "description": item.get("description"),
            "requirements": item.get("requirements"),
            "categoryName": item.get("category"),
            "occurredAt": _stamp(moment),
        },
    }


class _Producer(Protocol):
    """The subset of confluent_kafka.Producer the publisher uses (test seam)."""

    def produce(
        self, topic: str, key: str, value: str, on_delivery: Any = None
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def producer_config_from_env(environ: Mapping[str, str]) -> dict[str, Any]:
    """Build confluent-kafka producer settings from the shared KAFKA_* env names.

    Mirrors SharedKernel's KafkaProducerService (acks=all, idempotence, 10s
    timeout) and KafkaSecurity (SASL user implies SASL_SSL). Raises ValueError on
    half-configured or unknown SASL/TLS settings, so a bad deploy fails at start.
    """
    bootstrap = (environ.get("KAFKA_BOOTSTRAP_SERVERS") or "").strip()
    config: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": PRODUCE_TIMEOUT_MS,
    }
    username = (environ.get("KAFKA_SASL_USERNAME") or "").strip()
    protocol_raw = (environ.get("KAFKA_SECURITY_PROTOCOL") or "").strip()
    if protocol_raw:
        protocol = _PROTOCOLS.get(
            protocol_raw.replace("_", "").replace("-", "").lower()
        )
        if protocol is None:
            raise ValueError(f"Unknown KAFKA_SECURITY_PROTOCOL: {protocol_raw}")
    else:
        protocol = "SASL_SSL" if username else "PLAINTEXT"
    config["security.protocol"] = protocol

    if username:
        password = environ.get("KAFKA_SASL_PASSWORD") or ""
        if not password:
            raise ValueError(
                "KAFKA_SASL_PASSWORD is required when KAFKA_SASL_USERNAME is set."
            )
        mechanism_raw = (environ.get("KAFKA_SASL_MECHANISM") or "Plain").strip()
        mechanism = _MECHANISMS.get(
            mechanism_raw.replace("-", "").replace("_", "").lower()
        )
        if mechanism is None:
            raise ValueError(f"Unknown KAFKA_SASL_MECHANISM: {mechanism_raw}")
        config["sasl.mechanism"] = mechanism
        config["sasl.username"] = username
        config["sasl.password"] = password

    ca_location = (environ.get("KAFKA_SSL_CA_LOCATION") or "").strip()
    if ca_location:
        if not os.path.isfile(ca_location):
            raise ValueError(f"KAFKA_SSL_CA_LOCATION does not exist: {ca_location}")
        config["ssl.ca.location"] = ca_location
    return config


class JobEventPublisher:
    """Publishes one `job.external.upserted` envelope per crawled job.

    Delivery is asynchronous: failures are counted in the delivery callback and
    reported at flush time. Same best-effort stance as the old ES sync: a broker
    failure is logged and the crawl continues; PostgreSQL stays the source of truth.
    """

    def __init__(self, producer: _Producer, topic: str = DEFAULT_TOPIC) -> None:
        self._producer = producer
        self.topic = topic
        self.delivered = 0
        self.failed = 0

    def publish(self, item: Mapping[str, Any]) -> str:
        """Queue one job. Returns the job id (the Kafka key)."""
        envelope = build_external_job_upserted(item)
        job_id = envelope["payload"]["jobId"]
        value = json.dumps(envelope, ensure_ascii=False)
        try:
            self._producer.produce(
                self.topic, key=job_id, value=value, on_delivery=self._on_delivery
            )
        except BufferError:
            # Local queue full: serve delivery callbacks, then retry once.
            self._producer.poll(1.0)
            self._producer.produce(
                self.topic, key=job_id, value=value, on_delivery=self._on_delivery
            )
        self._producer.poll(0)
        return job_id

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            logger.warning("Kafka delivery failed. key=%s Error: %s", msg.key(), err)
        else:
            self.delivered += 1

    def flush(self) -> int:
        """Wait for in-flight messages. Returns the number still undelivered."""
        remaining = self._producer.flush(_FLUSH_TIMEOUT_S)
        if remaining:
            logger.warning(
                "Kafka flush timed out with %d messages undelivered.", remaining
            )
        return remaining
