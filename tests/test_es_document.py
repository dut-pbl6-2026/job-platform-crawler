"""Unit tests for the ES _id derivation and document builder (crawler/es_document.py)."""

import hashlib
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

from crawler.es_document import ACTIVE_STATUS, build_es_doc, index_exists, job_doc_id

NOW = datetime(2026, 11, 15, 8, 30, tzinfo=timezone.utc)


def test_job_doc_id_is_sha256_of_source_url():
    url = "https://vieclam.gov.vn/search/job-detail?id=388116"
    assert job_doc_id(url) == hashlib.sha256(url.encode()).hexdigest()


def test_job_doc_id_is_stable_and_distinct():
    a = job_doc_id("https://vieclam.gov.vn/seed/1")
    assert a == job_doc_id("https://vieclam.gov.vn/seed/1")
    assert a != job_doc_id("https://vieclam.gov.vn/seed/2")


def test_seed_index_collision_is_gone():
    """The old scheme used str(i+1) for seeds and PG serial for crawls: both could be '1'."""
    seed_id = job_doc_id("https://vieclam.gov.vn/seed/1")
    crawl_id = job_doc_id("https://vieclam.gov.vn/search/job-detail?id=1")
    assert seed_id != crawl_id
    assert seed_id != "1" and crawl_id != "1"


def test_build_es_doc_uses_search_svc_field_names():
    item = {
        "source_url": "https://vieclam.gov.vn/seed/1",
        "title": "Ky su phan mem",
        "company": "Cong ty TNHH ABC",
        "location": "Ha Noi",
        "salary_min": 5_000_000,
        "salary_max": 10_000_000,
        "salary_currency": "VND",
        "description": "Mo ta",
        "requirements": None,
        "category": "CNTT",
    }
    doc = build_es_doc(item, now=NOW)
    assert doc == {
        "id": job_doc_id(item["source_url"]),
        "title": "Ky su phan mem",
        "company_name": "Cong ty TNHH ABC",
        "location": "Ha Noi",
        "salary_min": 5_000_000,
        "salary_max": 10_000_000,
        "currency": "VND",
        "description": "Mo ta",
        "category_name": "CNTT",
        "status": ACTIVE_STATUS,
        "created_at": "2026-11-15T08:30:00Z",
        "updated_at": "2026-11-15T08:30:00Z",
    }


def test_build_es_doc_omits_none_values():
    doc = build_es_doc(
        {"source_url": "https://x/1", "title": "T", "company": None}, now=NOW
    )
    assert "company_name" not in doc
    assert "description" not in doc
    assert "salary_min" not in doc


def test_index_exists_true_and_false(caplog):
    es = MagicMock()
    es.indices.exists.return_value = True
    assert index_exists(es, "jobs") is True
    es.indices.exists.return_value = False
    with caplog.at_level(logging.WARNING, logger="crawler.es_document"):
        assert index_exists(es, "jobs") is False
    assert "does not exist" in caplog.text
    es.indices.create.assert_not_called()
