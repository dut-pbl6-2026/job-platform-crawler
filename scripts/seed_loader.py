"""seed_loader.py — Bulk insert seed/jobs.json into PostgreSQL + Elasticsearch.

Usage (standalone):
    python scripts/seed_loader.py

Also called by BlockDetectionMiddleware on consecutive block detection (CRAWL-01-07).
"""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("crawler.scripts.seed_loader")

# ---------------------------------------------------------------------------
# Default seed file path (relative to repo root)
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_SEED_PATH = str(_REPO_ROOT / "seed" / "jobs.json")


def load_seed(
    pg_url: str | None = None,
    es_url: str | None = None,
    es_index: str | None = None,
    seed_path: str = _DEFAULT_SEED_PATH,
) -> None:
    """Bulk insert seed records from *seed_path* JSON file into PG and ES.

    Args:
        pg_url:    PostgreSQL DSN.  Falls back to DATABASE_URL_CRAWLER env var.
        es_url:    Elasticsearch URL.  Falls back to ELASTICSEARCH_URL env var.
        es_index:  Elasticsearch index name.  Falls back to ELASTICSEARCH_INDEX env var.
        seed_path: Path to the seed JSON file (default: seed/jobs.json).
    """
    pg_url = pg_url or os.environ.get("DATABASE_URL_CRAWLER")
    es_url = es_url or os.environ.get("ELASTICSEARCH_URL")
    es_index = es_index or os.environ.get("ELASTICSEARCH_INDEX", "jobs")

    # Load seed file
    seed_file = Path(seed_path)
    if not seed_file.exists():
        logger.error("Seed file not found: %s", seed_path)
        return

    with open(seed_file, encoding="utf-8") as fh:
        records: list[dict] = json.load(fh)

    if not records:
        logger.warning("Seed file is empty: %s", seed_path)
        return

    _insert_pg(records, pg_url)
    _index_es(records, es_url, es_index)


def _insert_pg(records: list[dict], pg_url: str | None) -> None:
    """Bulk upsert records into PostgreSQL crawled_jobs (ON CONFLICT DO NOTHING)."""
    if not pg_url:
        logger.warning("DATABASE_URL_CRAWLER not set -- skipping PostgreSQL seed load.")
        return

    import psycopg2

    _CREATE = """
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
    """
    _INSERT = """
    INSERT INTO crawled_jobs
      (source_url, title, company, location, salary_raw, salary_min, salary_max,
       salary_currency, description, requirements, category)
    VALUES (%(source_url)s, %(title)s, %(company)s, %(location)s, %(salary_raw)s,
            %(salary_min)s, %(salary_max)s, %(salary_currency)s,
            %(description)s, %(requirements)s, %(category)s)
    ON CONFLICT (source_url) DO NOTHING;
    """

    try:
        conn = psycopg2.connect(pg_url)
        with conn, conn.cursor() as cur:
            cur.execute(_CREATE)
            cur.executemany(_INSERT, records)
        conn.close()
        logger.info("Loaded %d seed records into PostgreSQL.", len(records))
    except Exception as exc:
        logger.error("PostgreSQL seed load failed: %s", exc)


def _index_es(records: list[dict], es_url: str | None, es_index: str) -> None:
    """Bulk index records into Elasticsearch."""
    if not es_url:
        logger.warning(
            "ELASTICSEARCH_URL not set -- skipping Elasticsearch seed index."
        )
        return

    try:
        from elasticsearch import Elasticsearch
        from elasticsearch.helpers import bulk

        es = Elasticsearch([es_url])
        if not es.ping():
            logger.warning("Elasticsearch ping failed -- skipping seed index.")
            return

        actions = [
            {
                "_index": es_index,
                "_id": str(r.get("id", i + 1)),
                "_source": {
                    "title": r.get("title"),
                    "company": r.get("company"),
                    "location": r.get("location"),
                    "salary_min": r.get("salary_min"),
                    "salary_max": r.get("salary_max"),
                    "salary_currency": r.get("salary_currency", "VND"),
                    "description": r.get("description"),
                    "requirements": r.get("requirements"),
                    "category": r.get("category"),
                    "source_url": r.get("source_url"),
                    "crawled_at": r.get("crawled_at"),
                },
            }
            for i, r in enumerate(records)
        ]
        success_count, _ = bulk(es, actions)
        es.indices.refresh(index=es_index)
        logger.info("Indexed %d seed documents into Elasticsearch.", success_count)
    except Exception as exc:
        logger.error("Elasticsearch seed index failed: %s", exc)


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    load_seed()
