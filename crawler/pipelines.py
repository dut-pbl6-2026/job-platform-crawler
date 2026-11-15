"""Scrapy item pipelines for job-platform-crawler (CRAWL-01-02 to CRAWL-01-05).

Pipeline chain (priorities defined in settings.py):
  CleaningPipeline (100)      -> strip HTML, normalize salary/location
  DedupPipeline    (200)      -> in-memory dedup per run by source_url
  PostgresPipeline (300)      -> upsert into crawled_jobs (pg_id kept on item)
  KafkaJobEventPipeline (400) -> publish job.external.upserted to Kafka (best-effort);
                                search-svc indexes it into the ES jobs index
"""

import html
import logging
import os
import re
import unicodedata
from collections.abc import Mapping
from decimal import ROUND_HALF_UP, Decimal
from html.parser import HTMLParser
from typing import Any

import psycopg2
import psycopg2.extras
from confluent_kafka import Producer
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem, NotConfigured

from crawler.es_document import index_exists
from crawler.job_events import (
    DEFAULT_TOPIC,
    JobEventPublisher,
    producer_config_from_env,
)

logger = logging.getLogger("crawler.pipelines")

# ---------------------------------------------------------------------------
# Helper: strip HTML tags
# ---------------------------------------------------------------------------


class _MLStripper(HTMLParser):
    """Simple HTML-tag stripper using stdlib html.parser (no deps)."""

    def __init__(self) -> None:
        super().__init__()
        self.reset()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_data(self) -> str:
        return " ".join(self._parts)


def _strip_html(raw: str | None) -> str | None:
    """Return plain text from *raw* HTML string; None if blank/None."""
    if not raw:
        return None
    s = _MLStripper()
    s.feed(html.unescape(raw))
    text = s.get_data().strip()
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text or None


# ---------------------------------------------------------------------------
# Helper: salary parser
# ---------------------------------------------------------------------------

_MILLION = Decimal(1_000_000)
_UNITS: dict[str, Decimal] = {
    "trieu": _MILLION,
    "tr": _MILLION,
    "million": _MILLION,
    "ty": Decimal(1_000_000_000),  # ty / tỷ = billion
}
_NUM = r"\d+(?:[.,]\d+)*"
_UNIT = r"(?:trieu|tr|million|ty)(?![a-z])"
# "from X to Y" with an optional unit after either number: "5 - 10 trieu",
# "20 den 50 trieu", "5tr - 10tr", "5 trieu - 10 trieu".
_RANGE_RE = re.compile(
    rf"(?P<lo>{_NUM})\s*(?P<lo_unit>{_UNIT})?\s*"
    rf"(?:-|~|\bden\b|\bto\b)\s*"
    rf"(?P<hi>{_NUM})\s*(?P<hi_unit>{_UNIT})?"
)
_SINGLE_RE = re.compile(rf"(?P<val>{_NUM})\s*(?P<unit>{_UNIT})?")
_MIN_ONLY_RE = re.compile(r"\b(?:tu|tren|hon)\b|>")  # "tu 10 trieu", "tren 10 trieu"
_MAX_ONLY_RE = re.compile(
    r"\b(?:duoi|toi|up to)\b|<"
)  # "duoi 15 trieu", "toi 15 trieu"
_GROUPED_RE = re.compile(r"\d{1,3}(?:[.,]\d{3})+")  # "15.000.000", "15,000"


def _fold(text: str) -> str:
    """Lowercase ASCII-fold Vietnamese text: 'Thỏa thuận' -> 'thoa thuan'."""
    text = text.replace("đ", "d").replace("Đ", "D")
    decomposed = unicodedata.normalize("NFD", text)
    stripped = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
    return stripped.replace("\xa0", " ").lower()


def _decimal(token: str) -> Decimal:
    """Read a number token. One '.' or ',' is a decimal point; several are thousands separators."""
    if len(re.findall(r"[.,]", token)) > 1:
        return Decimal(re.sub(r"[.,]", "", token))
    return Decimal(token.replace(",", "."))


def _amount(token: str, unit: Decimal | None) -> Decimal:
    """Convert one number token to VND.

    With a unit (triệu, tr, million, tỷ) the number is in that unit and '.'/','
    are decimal points: '5.5 triệu' -> 5.5M. Without a unit, a grouped number
    ('15.000.000') is an absolute VND amount; a plain number below 1000 is
    read as millions (legacy heuristic, e.g. '10 - 20' -> 10M - 20M).
    """
    if unit is None and _GROUPED_RE.fullmatch(token):
        return Decimal(re.sub(r"[.,]", "", token))
    value = _decimal(token)
    if unit is not None:
        return value * unit
    return value * _MILLION if value < 1000 else value


def _to_vnd(value: Decimal) -> int:
    return int(value.quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _parse_salary(salary_raw: str | None) -> tuple[int | None, int | None]:
    """Parse *salary_raw* into (salary_min, salary_max) in VND integers.

    Returns (None, None) for blank, negotiable ('Thoa thuan', 'Thuong luong',
    'Negotiable' - any text without digits), non-VND ('USD', '$'), or unparseable
    input. A single amount gives (value, value); 'tu X' / 'tren X' give (X, None);
    'duoi X' / 'toi X' give (None, X).
    """
    if not salary_raw or not salary_raw.strip():
        return None, None
    text = _fold(salary_raw)
    if not re.search(r"\d", text):
        return None, None
    if re.search(r"\$|\busd\b|dollar", text):
        return None, None

    m = _RANGE_RE.search(text)
    if m:
        lo_unit_name = m.group("lo_unit") or m.group("hi_unit")
        hi_unit_name = m.group("hi_unit") or m.group("lo_unit")
        lo = _amount(m.group("lo"), _UNITS[lo_unit_name] if lo_unit_name else None)
        hi = _amount(m.group("hi"), _UNITS[hi_unit_name] if hi_unit_name else None)
        if lo > hi:
            lo, hi = hi, lo
        return _to_vnd(lo), _to_vnd(hi)

    m = _SINGLE_RE.search(text)
    if not m:
        return None, None
    value = _amount(
        m.group("val"), _UNITS[m.group("unit")] if m.group("unit") else None
    )
    amount = _to_vnd(value)
    if _MIN_ONLY_RE.search(text):
        return amount, None
    if _MAX_ONLY_RE.search(text):
        return None, amount
    return amount, amount


# ---------------------------------------------------------------------------
# Helper: location normalizer
# ---------------------------------------------------------------------------

_LOCATION_MAP: dict[str, str] = {
    "hcm": "Ho Chi Minh",
    "tp.hcm": "Ho Chi Minh",
    "tp hcm": "Ho Chi Minh",
    "ho chi minh": "Ho Chi Minh",
    "hn": "Ha Noi",
    "ha noi": "Ha Noi",
    "hanoi": "Ha Noi",
    "dn": "Da Nang",
    "da nang": "Da Nang",
    "danang": "Da Nang",
    "ct": "Can Tho",
    "can tho": "Can Tho",
    "hp": "Hai Phong",
    "hai phong": "Hai Phong",
    "hue": "Hue",
    "thua thien hue": "Hue",
}


def _normalize_location(loc: str | None) -> str | None:
    """Normalize common Vietnamese location abbreviations to full names."""
    if not loc:
        return None
    stripped = re.sub(r"\s+", " ", loc.strip())
    key = stripped.lower()
    return _LOCATION_MAP.get(key, stripped) or None


# ---------------------------------------------------------------------------
# SQL DDL for crawled_jobs table (CRAWL-01-04)
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS crawled_jobs (
    id              SERIAL       PRIMARY KEY,
    source_url      TEXT         NOT NULL UNIQUE,
    title           VARCHAR(256) NOT NULL,
    company         VARCHAR(256),
    location        VARCHAR(256),
    salary_raw      VARCHAR(256),
    salary_min      BIGINT,
    salary_max      BIGINT,
    salary_currency VARCHAR(10)  DEFAULT 'VND',
    description     TEXT,
    requirements    TEXT,
    category        VARCHAR(128),
    crawled_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_crawled_jobs_category ON crawled_jobs(category);
CREATE INDEX IF NOT EXISTS idx_crawled_jobs_location ON crawled_jobs(location);
"""

# Column list and bind placeholders shared by the crawl upsert and the seed insert.
_PG_COLUMNS_SQL = """
  (source_url, title, company, location, salary_raw, salary_min, salary_max,
   salary_currency, description, requirements, category)"""
_PG_VALUES_SQL = """
VALUES (%(source_url)s, %(title)s, %(company)s, %(location)s, %(salary_raw)s,
        %(salary_min)s, %(salary_max)s, %(salary_currency)s,
        %(description)s, %(requirements)s, %(category)s)"""

_UPSERT_SQL = f"""
INSERT INTO crawled_jobs{_PG_COLUMNS_SQL}{_PG_VALUES_SQL}
ON CONFLICT (source_url) DO UPDATE SET
  title       = EXCLUDED.title,
  updated_at  = NOW()
RETURNING id;
"""

# ---------------------------------------------------------------------------
# CleaningPipeline (priority 100)  -- CRAWL-01-02
# ---------------------------------------------------------------------------


class CleaningPipeline:
    """Strip HTML, normalize location and salary fields (CRAWL-01-02)."""

    def process_item(self, item: dict, spider) -> dict:  # type: ignore[override]
        adapter = ItemAdapter(item)

        # Strip HTML from text fields
        for field in ("description", "requirements"):
            raw = adapter.get(field)
            adapter[field] = _strip_html(raw)

        # Strip whitespace from all string fields
        for field in ("title", "company", "location", "salary_raw", "category"):
            val = adapter.get(field)
            if isinstance(val, str):
                adapter[field] = val.strip() or None

        # Normalize location
        adapter["location"] = _normalize_location(adapter.get("location"))

        # Parse salary
        salary_min, salary_max = _parse_salary(adapter.get("salary_raw"))
        adapter["salary_min"] = salary_min
        adapter["salary_max"] = salary_max
        adapter["salary_currency"] = "VND"

        return item


# ---------------------------------------------------------------------------
# DedupPipeline (priority 200)  -- CRAWL-01-03
# ---------------------------------------------------------------------------


class DedupPipeline:
    """In-memory dedup by source_url per spider run (CRAWL-01-03).

    DB-level UNIQUE constraint on source_url is the cross-run safety net.
    """

    def open_spider(self, spider) -> None:  # type: ignore[override]
        self.seen_urls: set[str] = set()

    def process_item(self, item: dict, spider) -> dict:  # type: ignore[override]
        adapter = ItemAdapter(item)
        url: str = adapter.get("source_url") or ""
        if not url:
            raise DropItem("Missing source_url -- cannot dedup.")
        if url in self.seen_urls:
            raise DropItem(f"Duplicate URL: {url}")
        self.seen_urls.add(url)
        return item


# ---------------------------------------------------------------------------
# PostgresPipeline (priority 300)  -- CRAWL-01-04
# ---------------------------------------------------------------------------


def build_pg_params(adapter: Mapping[str, Any]) -> dict:
    """Bind parameters for _UPSERT_SQL. Keys must match the %(name)s placeholders."""
    return {
        "source_url": adapter.get("source_url"),
        "title": adapter.get("title") or "",
        "company": adapter.get("company"),
        "location": adapter.get("location"),
        "salary_raw": adapter.get("salary_raw"),
        "salary_min": adapter.get("salary_min"),
        "salary_max": adapter.get("salary_max"),
        "salary_currency": adapter.get("salary_currency", "VND"),
        "description": adapter.get("description"),
        "requirements": adapter.get("requirements"),
        "category": adapter.get("category"),
    }


class PostgresPipeline:
    """Upsert items into PostgreSQL crawled_jobs table (CRAWL-01-04)."""

    BATCH_SIZE = 50

    def open_spider(self, spider) -> None:  # type: ignore[override]
        database_url = os.environ.get("DATABASE_URL_CRAWLER")
        if not database_url:
            raise NotConfigured("DATABASE_URL_CRAWLER is not set.")
        self.conn = psycopg2.connect(database_url)
        self.conn.autocommit = False
        with self.conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
        self.conn.commit()
        self._batch_count = 0
        self._total_count = 0
        logger.info("PostgresPipeline opened. Table crawled_jobs ready.")

    def process_item(self, item: dict, spider) -> dict:  # type: ignore[override]
        adapter = ItemAdapter(item)
        params = build_pg_params(adapter)
        try:
            with self.conn.cursor() as cur:
                cur.execute("SAVEPOINT crawler_batch")
                try:
                    cur.execute(_UPSERT_SQL, params)
                    row = cur.fetchone()
                except Exception as exc:
                    try:
                        cur.execute("ROLLBACK TO SAVEPOINT crawler_batch")
                    except Exception:  # noqa: BLE001, S110
                        pass
                    logger.error(
                        "PostgreSQL upsert failed. source_url=%s Error: %s",
                        params.get("source_url"),
                        exc,
                    )
                    raise DropItem(f"DB upsert failed: {exc}") from exc
                try:
                    cur.execute("RELEASE SAVEPOINT crawler_batch")
                except Exception:  # noqa: BLE001, S110
                    pass
                pg_id = row[0] if row else None
        except DropItem:
            raise
        except Exception as exc:  # noqa: BLE001
            try:
                self.conn.rollback()
            except Exception:  # noqa: BLE001, S110
                pass
            self._batch_count = 0
            logger.error(
                "PostgreSQL cursor failed. source_url=%s Error: %s",
                params.get("source_url"),
                exc,
            )
            raise DropItem(f"DB upsert failed: {exc}") from exc
        adapter["pg_id"] = pg_id
        self._batch_count += 1
        self._total_count += 1
        if self._batch_count >= self.BATCH_SIZE:
            try:
                self.conn.commit()
            except Exception as exc:  # noqa: BLE001
                try:
                    self.conn.rollback()
                except Exception:  # noqa: BLE001, S110
                    pass
                self._total_count = max(0, self._total_count - self.BATCH_SIZE)
                self._batch_count = 0
                logger.error("PostgreSQL batch commit failed: %s", exc)
                raise DropItem(f"DB batch commit failed: {exc}") from exc
            logger.info(
                "PostgresPipeline batch commit. %d items flushed.", self.BATCH_SIZE
            )
            self._batch_count = 0
        return item

    def close_spider(self, spider) -> None:  # type: ignore[override]
        if getattr(self, "conn", None) is not None and not self.conn.closed:
            try:
                try:
                    self.conn.commit()
                except Exception as exc:  # noqa: BLE001
                    try:
                        self.conn.rollback()
                    except Exception:  # noqa: BLE001, S110
                        pass
                    logger.error("PostgreSQL final commit failed: %s", exc)
            finally:
                self.conn.close()
        logger.info(
            "PostgresPipeline closed. Total items: %d.",
            getattr(self, "_total_count", 0),
        )


# ---------------------------------------------------------------------------
# KafkaJobEventPipeline (priority 400)  -- CRAWL-01-05
# ---------------------------------------------------------------------------


class KafkaJobEventPipeline:
    """Publish crawled jobs to the Kafka job-events topic for search-svc (CRAWL-01-05).

    Replaces the direct Elasticsearch write: search-svc consumes each
    `job.external.upserted` event and indexes it under the same id (sha256 of
    source_url). The crawler never creates the index (search-svc owns the mapping).
    If the broker is not configured or the index is missing, publishing is skipped
    with a warning and the crawl continues (best-effort, as before).
    """

    def open_spider(self, spider) -> None:  # type: ignore[override]
        self.publisher: JobEventPublisher | None = None
        self.es_index = os.environ.get("ELASTICSEARCH_INDEX", "jobs")
        if not os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip():
            logger.warning(
                "KAFKA_BOOTSTRAP_SERVERS is not set. Job event publishing disabled."
            )
            return
        if not self._index_ready():
            return
        # Misconfigured SASL/TLS raises ValueError here and fails the crawl at start.
        config = producer_config_from_env(os.environ)
        topic = os.environ.get("KAFKA_TOPIC_JOB_EVENTS", DEFAULT_TOPIC)
        self.publisher = JobEventPublisher(Producer(config), topic)
        logger.info(
            "KafkaJobEventPipeline opened. Topic '%s', index '%s'.",
            topic,
            self.es_index,
        )

    def _index_ready(self) -> bool:
        """Return True when the ES index exists. The crawler only checks; it never creates."""
        es_url = os.environ.get("ELASTICSEARCH_URL")
        if not es_url:
            logger.warning(
                "ELASTICSEARCH_URL is not set. Job event publishing disabled (index check)."
            )
            return False
        try:
            from elasticsearch import Elasticsearch

            es = Elasticsearch([es_url])
            if not es.ping():
                raise ConnectionError("Elasticsearch ping failed.")
            return index_exists(es, self.es_index)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Elasticsearch index check failed (%s). Job event publishing disabled.",
                exc,
            )
            return False

    def process_item(self, item: dict, spider) -> dict:  # type: ignore[override]
        if self.publisher is None:
            title = ItemAdapter(item).get("title", "")
            logger.warning(
                "Job event publishing disabled. Skipping publish for item: %s", title
            )
            return item
        try:
            self.publisher.publish(ItemAdapter(item).asdict())
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Kafka publish failed. source_url=%s Error: %s",
                ItemAdapter(item).get("source_url"),
                exc,
            )
        return item

    def close_spider(self, spider) -> None:  # type: ignore[override]
        if self.publisher is None:
            logger.info("KafkaJobEventPipeline closed (publishing disabled).")
            return
        undelivered = self.publisher.flush()
        logger.info(
            "KafkaJobEventPipeline closed. delivered=%d failed=%d undelivered=%d.",
            self.publisher.delivered,
            self.publisher.failed,
            undelivered,
        )
