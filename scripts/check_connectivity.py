"""check_connectivity.py — Verify PostgreSQL and Elasticsearch connections.

Usage:
    python scripts/check_connectivity.py

Exit codes:
    0 — both connections OK
    1 — one or more connections failed
"""

import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("crawler.scripts.check_connectivity")

_EXIT_OK = 0
_EXIT_FAIL = 1


def check_postgres(pg_url: str) -> bool:
    """Attempt a PostgreSQL connection. Returns True on success."""
    try:
        import psycopg2

        conn = psycopg2.connect(pg_url)
        conn.close()
        logger.info("PostgreSQL connection OK.")
        return True
    except Exception as exc:
        logger.error("PostgreSQL connection failed: %s", exc)
        return False


def check_elasticsearch(es_url: str) -> bool:
    """Attempt an Elasticsearch ping. Returns True on success."""
    try:
        from elasticsearch import Elasticsearch

        es = Elasticsearch([es_url])
        if es.ping():
            logger.info("Elasticsearch connection OK.")
            return True
        logger.error("Elasticsearch ping returned False. URL: %s", es_url)
        return False
    except Exception as exc:
        logger.error("Elasticsearch connection failed: %s", exc)
        return False


def main() -> int:
    pg_url = os.environ.get("DATABASE_URL_CRAWLER")
    es_url = os.environ.get("ELASTICSEARCH_URL")

    failed = False

    if not pg_url:
        logger.error("DATABASE_URL_CRAWLER is not set.")
        failed = True
    else:
        if not check_postgres(pg_url):
            failed = True

    if not es_url:
        logger.error("ELASTICSEARCH_URL is not set.")
        failed = True
    else:
        if not check_elasticsearch(es_url):
            failed = True

    return _EXIT_FAIL if failed else _EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
