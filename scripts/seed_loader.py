"""seed_loader.py — Bulk insert seed/jobs.json into PostgreSQL + Elasticsearch.

Usage (standalone):
    python scripts/seed_loader.py

Also called by BlockDetectionMiddleware on consecutive block detection (CRAWL-01-07).
"""

import json
import logging
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if (
    str(_REPO_ROOT) not in sys.path
):  # `python scripts/seed_loader.py` has no repo root on sys.path
    sys.path.insert(0, str(_REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

from crawler.es_document import build_es_doc, index_exists, job_doc_id  # noqa: E402

load_dotenv()

logger = logging.getLogger("crawler.scripts.seed_loader")

# ---------------------------------------------------------------------------
# Default seed file path (relative to repo root)
# ---------------------------------------------------------------------------
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

    from crawler.pipelines import (
        _CREATE_TABLE_SQL,
        _PG_COLUMNS_SQL,
        _PG_VALUES_SQL,
        build_pg_params,
    )

    # Same DDL and columns as PostgresPipeline (single source in pipelines.py).
    # ON CONFLICT DO NOTHING: seed rows never overwrite crawled rows.
    _INSERT = (
        f"INSERT INTO crawled_jobs{_PG_COLUMNS_SQL}{_PG_VALUES_SQL}\n"
        "ON CONFLICT (source_url) DO NOTHING;"
    )

    try:
        conn = psycopg2.connect(pg_url)
        with conn, conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
            cur.executemany(_INSERT, [build_pg_params(r) for r in records])
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
        if not index_exists(es, es_index):
            return

        # Same _id and document shape as ElasticsearchPipeline (crawler/es_document.py):
        # a seed job and a crawled job with the same source_url are one document.
        actions = [
            {
                "_index": es_index,
                "_id": job_doc_id(r["source_url"]),
                "_source": build_es_doc(r),
            }
            for r in records
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
