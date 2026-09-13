"""verify_500.py — Verify Day Wed milestone: 500+ dedup jobs in PG + ES.

Checks:
  1. PG count >= 500
  2. PG dedup: total == unique source_urls (zero duplicates)
  3. ES count >= 500
  4. ES search spot-check: query returns hits

Exit code 0 if all pass, 1 if any fail.

Usage:
    python scripts/verify_500.py
    mise run verify-500
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
logger = logging.getLogger("crawler.scripts.verify_500")

TARGET_COUNT = 500


def check_pg(pg_url: str | None) -> tuple[bool, bool]:
    """Check PG count and dedup. Returns (count_ok, dedup_ok)."""
    if not pg_url:
        logger.error("DATABASE_URL_CRAWLER is not set. Skipping PostgreSQL checks.")
        return False, False
    try:
        import psycopg2

        conn = psycopg2.connect(pg_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT source_url) FROM crawled_jobs;"
            )
            row = cur.fetchone()
        conn.close()
        total, unique_urls = row[0], row[1]
        count_ok = total >= TARGET_COUNT
        dedup_ok = total == unique_urls
        if count_ok:
            logger.info("PostgreSQL count: %d (>= %d). PASS.", total, TARGET_COUNT)
        else:
            logger.error("PostgreSQL count: %d (< %d). FAIL.", total, TARGET_COUNT)
        logger.info("Unique URLs: %d.", unique_urls)
        if dedup_ok:
            logger.info("Dedup check: total=%d, unique=%d. PASS.", total, unique_urls)
        else:
            logger.error("Dedup check: total=%d, unique=%d. FAIL.", total, unique_urls)
        return count_ok, dedup_ok
    except Exception as exc:
        logger.error("PostgreSQL check failed: %s", exc)
        return False, False


def check_es(es_url: str | None, es_index: str) -> tuple[bool, bool]:
    """Check ES count and search spot-check. Returns (count_ok, search_ok)."""
    if not es_url:
        logger.error("ELASTICSEARCH_URL is not set. Skipping Elasticsearch checks.")
        return False, False
    try:
        from elasticsearch import Elasticsearch

        es = Elasticsearch([es_url])
        if not es.ping():
            logger.error("Elasticsearch ping failed.")
            return False, False
        count_resp = es.count(index=es_index)
        count = count_resp.get("count", 0)
        count_ok = count >= TARGET_COUNT
        if count_ok:
            logger.info("Elasticsearch count: %d (>= %d). PASS.", count, TARGET_COUNT)
        else:
            logger.error("Elasticsearch count: %d (< %d). FAIL.", count, TARGET_COUNT)

        search_resp = es.search(index=es_index, q="*", size=1)
        hits_total = search_resp.get("hits", {}).get("total", {})
        hits = hits_total.get("value", 0) if isinstance(hits_total, dict) else 0
        search_ok = hits > 0
        if search_ok:
            logger.info("ES search spot-check: hits found. PASS.")
        else:
            logger.error("ES search spot-check: no hits. FAIL.")
        return count_ok, search_ok
    except Exception as exc:
        logger.error("Elasticsearch check failed: %s", exc)
        return False, False


def main() -> int:
    pg_url = os.environ.get("DATABASE_URL_CRAWLER")
    es_url = os.environ.get("ELASTICSEARCH_URL")
    es_index = os.environ.get("ELASTICSEARCH_INDEX", "jobs")

    pg_count_ok, pg_dedup_ok = check_pg(pg_url)
    es_count_ok, es_search_ok = check_es(es_url, es_index)

    results = [pg_count_ok, pg_dedup_ok, es_count_ok, es_search_ok]
    passed = sum(1 for r in results if r)
    total = len(results)
    if passed == total:
        logger.info(
            "[PASS] %d/%d checks passed. Day Wed milestone verified.", passed, total
        )
        return 0
    logger.error("[FAIL] %d/%d checks passed. See details above.", passed, total)
    return 1


if __name__ == "__main__":
    sys.exit(main())
