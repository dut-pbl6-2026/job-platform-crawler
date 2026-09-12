"""Scrapy item pipelines for job-platform-crawler (CRAWL-01-02 to CRAWL-01-05).

Pipeline chain (priorities defined in settings.py):
  CleaningPipeline (100)      -> strip HTML, normalize salary/location
  DedupPipeline    (200)      -> in-memory dedup per run by source_url
  PostgresPipeline (300)      -> upsert into crawled_jobs, returns pg_id
  ElasticsearchPipeline (400) -> index doc into ES jobs index (best-effort)
"""

import html
import logging
import os
import re
from html.parser import HTMLParser

import psycopg2
import psycopg2.extras
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem, NotConfigured

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

# Matches patterns like "5 - 10 trieu", "10tr - 15tr", "15.000.000 - 20.000.000"
_SALARY_RE = re.compile(
    r"([\d.,]+)\s*[-\u2013\u2014]\s*([\d.,]+)\s*(trieu|tri\u1ec7u|tri\u1ec7u \u0111\u1ed3ng|tr|million)?",
    re.IGNORECASE,
)

# Also match single value "10 trieu"
_SALARY_SINGLE_RE = re.compile(
    r"([\d.,]+)\s*(trieu|tri\u1ec7u|tri\u1ec7u \u0111\u1ed3ng|tr|million)?",
    re.IGNORECASE,
)

_MILLION = 1_000_000


def _parse_salary(salary_raw: str | None) -> tuple[int | None, int | None]:
    """Parse *salary_raw* into (salary_min, salary_max) VND integers.

    Returns (None, None) when salary_raw is blank or unparseable.
    Converts trieu/tr/million unit to VND by multiplying by 1_000_000.
    """
    if not salary_raw:
        return None, None

    cleaned = salary_raw.strip().replace(",", ".").replace("\xa0", " ")

    m = _SALARY_RE.search(cleaned)
    if m:
        low_str, high_str, unit = m.group(1), m.group(2), m.group(3)
        low = _to_int(low_str)
        high = _to_int(high_str)
        if unit:
            low = low * _MILLION if low is not None else None
            high = high * _MILLION if high is not None else None
        elif low is not None and low < 1_000:
            # Heuristic: bare number < 1000 treated as millions
            low = low * _MILLION
            high = high * _MILLION if high is not None else None
        return low, high

    # Single value (e.g. "10 trieu")
    m2 = _SALARY_SINGLE_RE.search(cleaned)
    if m2:
        val_str, unit = m2.group(1), m2.group(2)
        val = _to_int(val_str)
        if unit:
            val = val * _MILLION if val is not None else None
        elif val is not None and val < 1_000:
            val = val * _MILLION
        return val, val

    return None, None


def _to_int(s: str) -> int | None:
    """Convert a numeric string (may contain dots as thousand-separators) to int."""
    if not s:
        return None
    try:
        return int(float(s.replace(".", "").replace(",", ".")))
    except ValueError:
        return None


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

_UPSERT_SQL = """
INSERT INTO crawled_jobs
  (source_url, title, company, location, salary_raw, salary_min, salary_max,
   salary_currency, description, requirements, category)
VALUES (%(source_url)s, %(title)s, %(company)s, %(location)s, %(salary_raw)s,
        %(salary_min)s, %(salary_max)s, %(salary_currency)s,
        %(description)s, %(requirements)s, %(category)s)
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


class PostgresPipeline:
    """Upsert items into PostgreSQL crawled_jobs table (CRAWL-01-04)."""

    def open_spider(self, spider) -> None:  # type: ignore[override]
        database_url = os.environ.get("DATABASE_URL_CRAWLER")
        if not database_url:
            raise NotConfigured("DATABASE_URL_CRAWLER is not set.")
        self.conn = psycopg2.connect(database_url)
        self.conn.autocommit = False
        with self.conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
        self.conn.commit()
        logger.info("PostgresPipeline opened. Table crawled_jobs ready.")

    def process_item(self, item: dict, spider) -> dict:  # type: ignore[override]
        adapter = ItemAdapter(item)
        params = {
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
        try:
            with self.conn.cursor() as cur:
                cur.execute(_UPSERT_SQL, params)
                row = cur.fetchone()
                pg_id = row[0] if row else None
            self.conn.commit()
            adapter["pg_id"] = pg_id
        except Exception as exc:  # noqa: BLE001
            self.conn.rollback()
            logger.error(
                "PostgreSQL upsert failed. source_url=%s Error: %s",
                params.get("source_url"),
                exc,
            )
            raise DropItem(f"DB upsert failed: {exc}") from exc
        return item

    def close_spider(self, spider) -> None:  # type: ignore[override]
        if self.conn and not self.conn.closed:
            try:
                self.conn.commit()
            finally:
                self.conn.close()
        logger.info("PostgresPipeline closed.")


# ---------------------------------------------------------------------------
# ElasticsearchPipeline (priority 400)  -- CRAWL-01-05
# ---------------------------------------------------------------------------

_ES_MAPPING = {
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "company": {"type": "keyword"},
            "location": {"type": "keyword"},
            "salary_min": {"type": "long"},
            "salary_max": {"type": "long"},
            "salary_currency": {"type": "keyword"},
            "description": {"type": "text"},
            "requirements": {"type": "text"},
            "category": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "crawled_at": {"type": "date"},
        }
    }
}


class ElasticsearchPipeline:
    """Index items into Elasticsearch jobs index after PG upsert (CRAWL-01-05).

    ES failure logs warning but does NOT abort crawl (best-effort sync).
    """

    def open_spider(self, spider) -> None:  # type: ignore[override]
        es_url = os.environ.get("ELASTICSEARCH_URL")
        self.es_index = os.environ.get("ELASTICSEARCH_INDEX", "jobs")
        if not es_url:
            logger.warning(
                "ELASTICSEARCH_URL is not set. Elasticsearch indexing disabled."
            )
            self.es = None
            return
        try:
            from elasticsearch import Elasticsearch

            self.es = Elasticsearch([es_url])
            if not self.es.ping():
                raise ConnectionError("Elasticsearch ping failed.")
            if not self.es.indices.exists(index=self.es_index):
                self.es.indices.create(index=self.es_index, body=_ES_MAPPING)
                logger.info("ElasticsearchPipeline: created index '%s'.", self.es_index)
            else:
                logger.info(
                    "ElasticsearchPipeline opened. Index '%s' ready.", self.es_index
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "ElasticsearchPipeline: connection failed (%s). Indexing disabled.", exc
            )
            self.es = None

    def process_item(self, item: dict, spider) -> dict:  # type: ignore[override]
        if self.es is None:
            title = ItemAdapter(item).get("title", "")
            logger.warning(
                "Elasticsearch unavailable. Skipping index for item: %s", title
            )
            return item
        adapter = ItemAdapter(item)
        pg_id = adapter.get("pg_id")
        doc = {
            "title": adapter.get("title"),
            "company": adapter.get("company"),
            "location": adapter.get("location"),
            "salary_min": adapter.get("salary_min"),
            "salary_max": adapter.get("salary_max"),
            "salary_currency": adapter.get("salary_currency", "VND"),
            "description": adapter.get("description"),
            "requirements": adapter.get("requirements"),
            "category": adapter.get("category"),
            "source_url": adapter.get("source_url"),
            "crawled_at": None,
        }
        try:
            self.es.index(
                index=self.es_index,
                id=str(pg_id) if pg_id is not None else None,
                document=doc,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Elasticsearch index failed for item '%s': %s",
                adapter.get("title"),
                exc,
            )
        return item

    def close_spider(self, spider) -> None:  # type: ignore[override]
        if self.es is not None:
            try:
                self.es.indices.refresh(index=self.es_index)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Elasticsearch refresh failed: %s", exc)
        logger.info("ElasticsearchPipeline closed.")
