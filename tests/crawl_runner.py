"""Run the real VieclamSpider engine against local fixtures, with no network.

Only the I/O edges are faked: psycopg2.connect, the Kafka producer
(confluent_kafka.Producer, job-events), elasticsearch.Elasticsearch and
elasticsearch.helpers.bulk (seed loader only). The pipelines, middlewares, seed loader and spider
code run unchanged. Requests go to file:// fixtures, a localhost HTTP server
(403 route), and a closed localhost port (connection refused).

Run in a subprocess by tests/test_integration_crawl.py (the Twisted reactor
can only run once per process):
    python -m tests.crawl_runner <crawl|block> <out.json>
"""

import json
import logging
import os
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures"

# Set before crawler.settings is loaded (it reads the environment at import).
os.environ.update(
    {
        "DATABASE_URL_CRAWLER": "postgresql://fake/fake",
        "ELASTICSEARCH_URL": "http://fake-es:9200",
        "ELASTICSEARCH_INDEX": "jobs",
        "KAFKA_BOOTSTRAP_SERVERS": "fake-kafka:9092",
        "CRAWLER_TARGET": "https://vieclam.gov.vn",
        "CRAWLER_API_BASE": "https://api.fixture.invalid/webhook",
        "MAX_PAGES": "1",
        "PAGE_SIZE": "20",
        "SCRAPY_ENV": "production",
        "LOG_LEVEL": "INFO",
        "NETWORK_RETRY_BACKOFF": "0",
        "BLOCK_DETECTION_THRESHOLD": "1",
    }
)

PG_WRITES: list[dict] = []
ES_ACTIONS: list[dict] = []
ES_CREATED: list[str] = []
KAFKA_MESSAGES: list[dict] = []
BLOCK_LOG: list[str] = []


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None) -> None:
        if "INSERT INTO crawled_jobs" in sql:
            kind = "seed" if "DO NOTHING" in sql else "upsert"
            PG_WRITES.append({"kind": kind, "params": params})
            self.conn.next_id += 1
            self.conn.last_id = self.conn.next_id

    def executemany(self, sql, seq) -> None:
        for params in seq:
            PG_WRITES.append({"kind": "seed", "params": params})

    def fetchone(self):
        return (self.conn.last_id,)


class _FakeConn:
    def __init__(self) -> None:
        self.autocommit = False
        self.closed = False
        self.next_id = 0
        self.last_id = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class _FakeIndices:
    def exists(self, index: str) -> bool:
        return True

    def refresh(self, index: str) -> None:
        pass

    def create(self, index: str, **kwargs) -> None:
        ES_CREATED.append(index)


class _FakeES:
    def __init__(self, *args, **kwargs) -> None:
        self.indices = _FakeIndices()

    def ping(self) -> bool:
        return True


def _fake_bulk(client, actions, **kwargs):
    actions = list(actions)
    ES_ACTIONS.extend(actions)
    return len(actions), []


class _FakeMessage:
    def __init__(self, topic: str, key: str) -> None:
        self._topic = topic
        self._key = key

    def topic(self) -> str:
        return self._topic

    def key(self) -> str:
        return self._key


class _FakeKafkaProducer:
    """Records produced messages and acks each one synchronously (delivery callback)."""

    def __init__(self, config: dict) -> None:
        self.config = config

    def produce(self, topic: str, key: str, value: str, on_delivery=None) -> None:
        KAFKA_MESSAGES.append(
            {"topic": topic, "key": key, "envelope": json.loads(value)}
        )
        if on_delivery is not None:
            on_delivery(None, _FakeMessage(topic, key))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


class _BlockedHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        self.send_response(403)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write("<html><title>403</title></html>".encode())

    def log_message(self, *args) -> None:
        pass


def _closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _file_url(name: str) -> str:
    return (FIXTURES / name).as_uri()


class _BlockLogCapture(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        BLOCK_LOG.append(record.getMessage())


def main(scenario: str, out_path: str) -> None:
    from scrapy.crawler import CrawlerProcess
    from scrapy.http import Request
    from scrapy.settings import Settings

    from crawler.spiders.vieclam_spider import VieclamSpider

    server = ThreadingHTTPServer(("127.0.0.1", 0), _BlockedHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    blocked_url = f"http://127.0.0.1:{server.server_address[1]}/blocked"
    down_url = f"http://127.0.0.1:{_closed_port()}/down"

    class FixtureSpider(VieclamSpider):
        name = "vieclam_fixture"
        allowed_domains = None  # file:// and 127.0.0.1 have no offsite domain

        def start_requests(self):
            if scenario == "block":
                yield Request(
                    blocked_url, callback=self.parse, errback=self.err_listing
                )
                return
            yield Request(
                _file_url("html/search_page.html"),
                callback=self.parse,
                errback=self.err_listing,
                meta={"page": 1},
            )
            yield Request(
                _file_url("api_page.json"),
                callback=self.parse_api,
                errback=self.err_api,
                meta={"page_num": 1},
            )
            yield Request(blocked_url, callback=self.parse, errback=self.err_listing)
            yield Request(down_url, callback=self.parse, errback=self.err_listing)

    settings = Settings()
    settings.setmodule("crawler.settings")
    settings.update(
        {
            "HTTPCACHE_ENABLED": False,
            "DOWNLOAD_DELAY": 0,
            "AUTOTHROTTLE_ENABLED": False,
            "ROBOTSTXT_OBEY": False,
            "TELNETCONSOLE_ENABLED": False,
        }
    )
    if scenario == "block":
        settings.set("BLOCK_DETECTION_THRESHOLD", 0)

    logging.getLogger("crawler.middlewares").addHandler(_BlockLogCapture())
    logging.getLogger("crawler.middlewares").setLevel(logging.INFO)

    with (
        patch("psycopg2.connect", side_effect=lambda *a, **k: _FakeConn()),
        patch("elasticsearch.Elasticsearch", _FakeES),
        patch("elasticsearch.helpers.bulk", side_effect=_fake_bulk),
        patch("crawler.pipelines.Producer", _FakeKafkaProducer),
    ):
        process = CrawlerProcess(settings)
        crawler = process.create_crawler(FixtureSpider)
        process.crawl(crawler)
        process.start()
    server.shutdown()

    stats = crawler.stats.get_stats()
    keep = (
        "item_scraped_count",
        "item_dropped_count",
        "downloader/exception_count",
        "downloader/response_status_count/403",
    )
    result = {
        "scenario": scenario,
        "finish_reason": stats.get("finish_reason"),
        "stats": {k: stats.get(k, 0) for k in keep},
        "pg_writes": PG_WRITES,
        "es_actions": ES_ACTIONS,
        "es_created": ES_CREATED,
        "kafka_messages": KAFKA_MESSAGES,
        "block_log": BLOCK_LOG,
    }
    Path(out_path).write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
