"""Kafka job-events producer: wire contract, env config, publisher and pipeline."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from crawler.es_document import job_doc_id
from crawler.job_events import (
    DEFAULT_TOPIC,
    EVENT_TYPE,
    JobEventPublisher,
    build_external_job_upserted,
    producer_config_from_env,
)
from crawler.pipelines import KafkaJobEventPipeline

FIXTURE = Path(__file__).parent / "fixtures" / "job_external_upserted.v1.json"
FIXED_NOW = datetime(2026, 11, 15, 8, 0, 0, tzinfo=timezone.utc)
SOURCE_URL = "https://vieclam.gov.vn/search/job-detail?id=101"


def make_item(**overrides) -> dict:
    item = {
        "source_url": SOURCE_URL,
        "title": "Nhan vien kinh doanh",
        "company": "Cong ty ABC",
        "location": "Ha Noi",
        "salary_raw": "5 - 7 trieu",
        "salary_min": 5_500_000,
        "salary_max": 7_000_000,
        "salary_currency": "VND",
        "description": "Mo ta cong viec",
        "requirements": None,
        "category": "Kinh doanh",
    }
    item.update(overrides)
    return item


# --- wire contract -----------------------------------------------------------


def test_envelope_matches_golden_fixture():
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    actual = build_external_job_upserted(
        make_item(),
        event_id=expected["eventId"],
        now=FIXED_NOW,
    )
    assert actual == expected


def test_job_id_is_sha256_of_source_url():
    envelope = build_external_job_upserted(make_item(), now=FIXED_NOW)
    assert envelope["payload"]["jobId"] == job_doc_id(SOURCE_URL)
    assert len(envelope["payload"]["jobId"]) == 64


def test_envelope_header_fields():
    envelope = build_external_job_upserted(make_item(), now=FIXED_NOW)
    assert envelope["eventType"] == EVENT_TYPE == "job.external.upserted"
    assert envelope["version"] == 1
    assert envelope["timestamp"] == "2026-11-15T08:00:00Z"
    assert len(envelope["eventId"]) == 36  # uuid4 string when not supplied


def test_missing_salary_and_currency_serialize_as_null_and_vnd():
    envelope = build_external_job_upserted(
        make_item(salary_min=None, salary_max=None, salary_currency=None),
        now=FIXED_NOW,
    )
    payload = envelope["payload"]
    assert payload["salaryMin"] is None
    assert payload["salaryMax"] is None
    assert payload["currency"] == "VND"


# --- env -> producer config --------------------------------------------------


def test_config_defaults_match_shared_producer():
    config = producer_config_from_env({"KAFKA_BOOTSTRAP_SERVERS": " broker:9092 "})
    assert config == {
        "bootstrap.servers": "broker:9092",
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": 10_000,
        "security.protocol": "PLAINTEXT",
    }


def test_sasl_user_implies_sasl_ssl_with_scram():
    config = producer_config_from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9093",
            "KAFKA_SASL_USERNAME": "svc",
            "KAFKA_SASL_PASSWORD": "secret",
            "KAFKA_SASL_MECHANISM": "ScramSha256",
        }
    )
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "SCRAM-SHA-256"
    assert config["sasl.username"] == "svc"
    assert config["sasl.password"] == "secret"


def test_sasl_username_without_password_fails_fast():
    with pytest.raises(ValueError, match="KAFKA_SASL_PASSWORD"):
        producer_config_from_env(
            {"KAFKA_BOOTSTRAP_SERVERS": "b:9093", "KAFKA_SASL_USERNAME": "svc"}
        )


def test_unknown_protocol_and_mechanism_fail_fast():
    with pytest.raises(ValueError, match="KAFKA_SECURITY_PROTOCOL"):
        producer_config_from_env(
            {"KAFKA_BOOTSTRAP_SERVERS": "b", "KAFKA_SECURITY_PROTOCOL": "Bogus"}
        )
    with pytest.raises(ValueError, match="KAFKA_SASL_MECHANISM"):
        producer_config_from_env(
            {
                "KAFKA_BOOTSTRAP_SERVERS": "b",
                "KAFKA_SASL_USERNAME": "svc",
                "KAFKA_SASL_PASSWORD": "p",
                "KAFKA_SASL_MECHANISM": "Kerberos",
            }
        )


def test_missing_ca_file_fails_fast(tmp_path):
    with pytest.raises(ValueError, match="KAFKA_SSL_CA_LOCATION"):
        producer_config_from_env(
            {
                "KAFKA_BOOTSTRAP_SERVERS": "b",
                "KAFKA_SSL_CA_LOCATION": str(tmp_path / "missing.crt"),
            }
        )


# --- publisher ---------------------------------------------------------------


def test_publish_keys_by_job_id_and_targets_topic():
    producer = MagicMock()
    publisher = JobEventPublisher(producer, DEFAULT_TOPIC)
    job_id = publisher.publish(make_item())

    assert job_id == job_doc_id(SOURCE_URL)
    args, kwargs = producer.produce.call_args
    assert args == ("job-events",)
    assert kwargs["key"] == job_id
    assert json.loads(kwargs["value"])["payload"]["jobId"] == job_id
    producer.poll.assert_called_with(0)


def test_publish_retries_once_when_local_queue_is_full():
    producer = MagicMock()
    producer.produce.side_effect = [BufferError("queue full"), None]
    JobEventPublisher(producer).publish(make_item())
    assert producer.produce.call_count == 2
    producer.poll.assert_any_call(1.0)


def test_delivery_callbacks_count_success_and_failure():
    publisher = JobEventPublisher(MagicMock())
    msg = MagicMock()
    msg.key.return_value = "k"
    publisher._on_delivery(None, msg)
    publisher._on_delivery("broker down", msg)
    assert (publisher.delivered, publisher.failed) == (1, 1)


def test_flush_reports_undelivered_count():
    producer = MagicMock()
    producer.flush.return_value = 3
    assert JobEventPublisher(producer).flush() == 3


# --- pipeline ----------------------------------------------------------------


@pytest.fixture
def clean_env(monkeypatch):
    for name in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC_JOB_EVENTS",
        "ELASTICSEARCH_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ELASTICSEARCH_INDEX", "jobs")


def _es_ready(index_exists: bool = True) -> MagicMock:
    es = MagicMock()
    es.ping.return_value = True
    es.indices.exists.return_value = index_exists
    return es


def test_pipeline_disabled_without_bootstrap_servers(clean_env):
    pipe = KafkaJobEventPipeline()
    with patch("crawler.pipelines.Producer") as producer_cls:
        pipe.open_spider(spider=None)
    assert pipe.publisher is None
    producer_cls.assert_not_called()
    pipe.process_item(make_item(), spider=None)  # must not raise
    pipe.close_spider(spider=None)


def test_pipeline_skips_publish_when_index_missing(clean_env, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("ELASTICSEARCH_URL", "http://es:9200")
    pipe = KafkaJobEventPipeline()
    with (
        patch(
            "elasticsearch.Elasticsearch", return_value=_es_ready(index_exists=False)
        ),
        patch("crawler.pipelines.Producer") as producer_cls,
    ):
        pipe.open_spider(spider=None)
    assert pipe.publisher is None
    producer_cls.assert_not_called()


def test_pipeline_skips_publish_without_elasticsearch_url(clean_env, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    pipe = KafkaJobEventPipeline()
    with patch("crawler.pipelines.Producer") as producer_cls:
        pipe.open_spider(spider=None)
    assert pipe.publisher is None
    producer_cls.assert_not_called()


def test_pipeline_publishes_each_item_and_flushes_on_close(clean_env, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("ELASTICSEARCH_URL", "http://es:9200")
    producer = MagicMock()
    producer.flush.return_value = 0
    pipe = KafkaJobEventPipeline()
    with (
        patch("elasticsearch.Elasticsearch", return_value=_es_ready()),
        patch("crawler.pipelines.Producer", return_value=producer),
    ):
        pipe.open_spider(spider=None)
    for i in range(3):
        pipe.process_item(make_item(source_url=f"{SOURCE_URL}&n={i}"), spider=None)
    pipe.close_spider(spider=None)

    assert producer.produce.call_count == 3
    keys = {c.kwargs["key"] for c in producer.produce.call_args_list}
    assert keys == {job_doc_id(f"{SOURCE_URL}&n={i}") for i in range(3)}
    producer.flush.assert_called_once()


def test_pipeline_publish_error_does_not_abort_crawl(clean_env, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("ELASTICSEARCH_URL", "http://es:9200")
    producer = MagicMock()
    producer.produce.side_effect = RuntimeError("broker unreachable")
    pipe = KafkaJobEventPipeline()
    with (
        patch("elasticsearch.Elasticsearch", return_value=_es_ready()),
        patch("crawler.pipelines.Producer", return_value=producer),
    ):
        pipe.open_spider(spider=None)
    item = make_item()
    assert pipe.process_item(item, spider=None) is item
