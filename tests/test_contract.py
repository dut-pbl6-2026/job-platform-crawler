"""Contract tests: PostgreSQL columns and the ES document shape.

The search-svc field list is vendored from
job-platform-search-svc src/Search.Core/Models/JobDocument.cs (JSON names,
commit 260752e) and src/Search.Api/DTOs/JobSyncDto.cs. Update it when search-svc changes.
"""

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

from crawler.es_document import build_es_doc, job_doc_id
from crawler.items import JobItem
from crawler.pipelines import (
    _CREATE_TABLE_SQL,
    _UPSERT_SQL,
    CleaningPipeline,
    build_pg_params,
)
from scripts.seed_loader import load_seed

# search-svc JobDocument JSON property names (JobDocument.cs @ 260752e)
SEARCH_SVC_JOB_DOCUMENT_FIELDS = {
    "id",
    "title",
    "description",
    "company_id",
    "company_name",
    "location",
    "salary_min",
    "salary_max",
    "currency",
    "category_id",
    "category_name",
    "employment_type",
    "experience_level",
    "status",
    "recruiter_id",
    "created_at",
    "updated_at",
    "expires_at",
    "requirements",
    "benefits",
    "skills",
}
# search-svc mapping (ElasticsearchInitializer.cs): text/keyword/numeric/date fields
SEARCH_SVC_NUMERIC = {"salary_min", "salary_max"}
SEARCH_SVC_DATE = {"created_at", "updated_at", "expires_at"}

SEED_PATH = Path(__file__).parent.parent / "seed" / "jobs.json"
SAMPLE_ITEM = {
    "source_url": "https://vieclam.gov.vn/search/job-detail?id=388116",
    "title": "Nha Quan Ly",
    "company": "Cong ty ABC",
    "location": "Tinh Bac Ninh",
    "salary_raw": "20 den 50 trieu",
    "description": None,
    "requirements": None,
    "category": "Ky thuat vien",
}


def _sql_columns(sql: str) -> list[str]:
    cols = re.search(r"INSERT INTO crawled_jobs\s*\((.*?)\)\s*VALUES", sql, re.S)
    return [c.strip() for c in cols.group(1).split(",")]


def _ddl_columns() -> list[str]:
    body = re.search(
        r"CREATE TABLE IF NOT EXISTS crawled_jobs \((.*?)\n\);", _CREATE_TABLE_SQL, re.S
    )
    return [line.split()[0] for line in body.group(1).strip().splitlines()]


def test_upsert_columns_exist_in_ddl():
    assert set(_sql_columns(_UPSERT_SQL)) <= set(_ddl_columns())


def test_upsert_placeholders_match_bind_params():
    item = CleaningPipeline().process_item(JobItem(SAMPLE_ITEM), spider=None)
    params = build_pg_params(item)
    placeholders = set(re.findall(r"%\((\w+)\)s", _UPSERT_SQL))
    assert placeholders == set(params) == set(_sql_columns(_UPSERT_SQL))


def test_every_written_pg_value_has_a_ddl_column():
    item = CleaningPipeline().process_item(JobItem(SAMPLE_ITEM), spider=None)
    assert set(build_pg_params(item)) <= set(_ddl_columns())


def test_seed_insert_uses_same_columns_as_crawl_upsert():
    """The seed loader's real executemany SQL has the same column list as the crawl upsert."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = False
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = False
    with (
        patch("psycopg2.connect", return_value=mock_conn),
        patch("elasticsearch.Elasticsearch", return_value=MagicMock()),
        patch("elasticsearch.helpers.bulk", return_value=(0, [])),
    ):
        load_seed(
            pg_url="postgresql://test/db",
            es_url=None,
            es_index="jobs",
            seed_path=str(SEED_PATH),
        )
    seed_sql, seed_params = mock_cursor.executemany.call_args[0]
    assert _sql_columns(seed_sql) == _sql_columns(_UPSERT_SQL)
    assert "ON CONFLICT (source_url) DO NOTHING" in seed_sql
    assert set(re.findall(r"%\((\w+)\)s", seed_sql)) == set(
        build_pg_params(JobItem(SAMPLE_ITEM))
    )
    assert len(seed_params) == 550


def test_es_doc_fields_are_known_to_search_svc():
    doc = build_es_doc(
        CleaningPipeline().process_item(JobItem(SAMPLE_ITEM), spider=None)
    )
    assert set(doc) <= SEARCH_SVC_JOB_DOCUMENT_FIELDS


def test_es_doc_value_types_match_search_svc_mapping():
    doc = build_es_doc(
        CleaningPipeline().process_item(JobItem(SAMPLE_ITEM), spider=None)
    )
    assert isinstance(doc["salary_min"], int) and isinstance(doc["salary_max"], int)
    assert set(doc) & SEARCH_SVC_NUMERIC == SEARCH_SVC_NUMERIC
    for field in SEARCH_SVC_DATE & set(doc):
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", doc[field]), field
    assert doc["status"] == "Active"


def test_es_doc_id_is_the_only_identity_and_is_hex_sha256():
    doc = build_es_doc(SAMPLE_ITEM)
    assert doc["id"] == job_doc_id(SAMPLE_ITEM["source_url"])
    assert re.fullmatch(r"[0-9a-f]{64}", doc["id"])


def test_seed_records_produce_search_svc_shaped_docs():
    records = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    for record in records:
        doc = build_es_doc(record)
        assert set(doc) <= SEARCH_SVC_JOB_DOCUMENT_FIELDS
        assert doc["id"] == job_doc_id(record["source_url"])
    assert len({build_es_doc(r)["id"] for r in records}) == len(records)
