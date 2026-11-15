"""Elasticsearch document contract shared by ElasticsearchPipeline and seed_loader.

Field names mirror search-svc's JobDocument (JSON names, src/Search.Core/Models/
JobDocument.cs in job-platform-search-svc, checked at commit 260752e). search-svc
owns the `jobs` mapping (analyzers, dense_vector `embedding`), so this module
never creates the index.
"""

import hashlib
import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("crawler.es_document")

ACTIVE_STATUS = "Active"  # search-svc default; semantic search only returns Active jobs

# item key -> search-svc JobDocument JSON field
_FIELD_MAP: dict[str, str] = {
    "title": "title",
    "company": "company_name",
    "location": "location",
    "salary_min": "salary_min",
    "salary_max": "salary_max",
    "salary_currency": "currency",
    "description": "description",
    "requirements": "requirements",
    "category": "category_name",
}


def job_doc_id(source_url: str) -> str:
    """Return the ES _id (== document `id`) for a job.

    Key: sha256 of `source_url`, the same key as PG UNIQUE(source_url) and
    DedupPipeline. Crawl and seed paths both derive the id from it, so one job
    maps to one ES document. Never derive it from a DB serial or seed index.
    """
    return hashlib.sha256(source_url.encode("utf-8")).hexdigest()


def build_es_doc(
    item: Mapping[str, Any], now: datetime | None = None
) -> dict[str, Any]:
    """Build the `_source` for one job. Keys whose value is None are omitted."""
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    doc: dict[str, Any] = {
        "id": job_doc_id(item["source_url"]),
        "status": ACTIVE_STATUS,
        "created_at": stamp,
        "updated_at": stamp,
    }
    for key, es_field in _FIELD_MAP.items():
        value = item.get(key)
        if value is not None:
            doc[es_field] = value
    return doc


def index_exists(es: Any, index: str) -> bool:
    """Return True if the index exists; otherwise log why ES writes are skipped.

    The crawler must not auto-create the index: a bulk write into a missing
    index creates it with dynamic mapping and no `embedding` field, which makes
    search-svc refuse to start (ElasticsearchInitializer dims check).
    """
    if es.indices.exists(index=index):
        return True
    logger.warning(
        "Elasticsearch index '%s' does not exist. search-svc creates it (owns the "
        "mapping); start search-svc first. Skipping ES writes.",
        index,
    )
    return False
