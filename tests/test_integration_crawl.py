"""Integration: real spider engine run against local fixtures, no network.

Each scenario runs tests/crawl_runner.py in a subprocess (one reactor per process).
Only the DB and ES clients are faked; everything else (spider, pipelines,
middlewares, seed loader) is the production code.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from crawler.es_document import job_doc_id
from crawler.settings import RETRY_TIMES

ROOT = Path(__file__).resolve().parent.parent
KAFKA_PAYLOAD_FIELDS = {
    "jobId",
    "sourceUrl",
    "title",
    "companyName",
    "location",
    "salaryMin",
    "salaryMax",
    "currency",
    "description",
    "requirements",
    "categoryName",
    "occurredAt",
}
SEARCH_SVC_FIELDS = {
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
}  # JobDocument.cs @ job-platform-search-svc 260752e


def run_scenario(name: str, out: Path) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in ("DATABASE_URL_CRAWLER",)}
    subprocess.run(
        [sys.executable, "-m", "tests.crawl_runner", name, str(out)],
        cwd=ROOT,
        env=env,
        check=True,
        timeout=300,
        capture_output=True,
    )
    return json.loads(out.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def crawl(tmp_path_factory):
    return run_scenario("crawl", tmp_path_factory.mktemp("crawl") / "out.json")


@pytest.fixture(scope="module")
def block(tmp_path_factory):
    return run_scenario("block", tmp_path_factory.mktemp("block") / "out.json")


def test_crawl_finishes_normally(crawl):
    assert crawl["finish_reason"] == "finished"


def test_crawl_writes_each_unique_job_once_to_pg(crawl):
    writes = crawl["pg_writes"]
    assert all(w["kind"] == "upsert" for w in writes)
    urls = [w["params"]["source_url"] for w in writes]
    # 2 API entries (one id repeated -> dropped) + 2 HTML detail pages
    assert len(urls) == 4
    assert len(set(urls)) == 4
    assert crawl["stats"]["item_dropped_count"] == 1


def test_crawl_publishes_one_event_per_job_keyed_by_job_id(crawl):
    messages = crawl["kafka_messages"]
    assert len(messages) == 4
    assert all(m["topic"] == "job-events" for m in messages)
    keys = [m["key"] for m in messages]
    assert len(set(keys)) == 4
    for message in messages:
        envelope = message["envelope"]
        assert envelope["eventType"] == "job.external.upserted"
        assert envelope["payload"]["jobId"] == message["key"]
        assert set(envelope["payload"]) <= KAFKA_PAYLOAD_FIELDS


def test_crawl_kafka_keys_match_pg_source_urls(crawl):
    pg_urls = {w["params"]["source_url"] for w in crawl["pg_writes"]}
    assert {m["key"] for m in crawl["kafka_messages"]} == {
        job_doc_id(u) for u in pg_urls
    }


def test_crawl_writes_no_documents_to_elasticsearch_directly(crawl):
    # Indexing moved to search-svc via job-events; the crawl never bulk-writes ES.
    assert crawl["es_actions"] == []


def test_crawl_salary_from_api_is_parsed_per_real_format(crawl):
    by_url = {w["params"]["source_url"]: w["params"] for w in crawl["pg_writes"]}
    api_101 = by_url["https://vieclam.gov.vn/search/job-detail?id=101"]
    api_102 = by_url["https://vieclam.gov.vn/search/job-detail?id=102"]
    assert (api_101["salary_min"], api_101["salary_max"]) == (5_500_000, 7_000_000)
    assert (api_102["salary_min"], api_102["salary_max"]) == (20_000_000, 50_000_000)


def test_crawl_html_negotiable_salary_is_null(crawl):
    html_rows = [
        w["params"]
        for w in crawl["pg_writes"]
        if w["params"]["source_url"].startswith("file:")
    ]
    negotiable = next(p for p in html_rows if p["title"] == "Nhan vien kinh doanh")
    assert negotiable["salary_raw"] == "Thỏa thuận"
    assert negotiable["salary_min"] is None and negotiable["salary_max"] is None


def test_crawl_network_error_is_retried_not_a_block(crawl):
    # Connection refused on one URL: 1 attempt + RETRY_TIMES retries, then errback.
    assert crawl["stats"]["downloader/exception_count"] == 1 + RETRY_TIMES
    retry_lines = [
        m for m in crawl["block_log"] if m.startswith("Network error, retry ")
    ]
    assert len(retry_lines) == RETRY_TIMES
    assert any("retries exhausted" in m for m in crawl["block_log"])
    assert not any("Activating seed fallback" in m for m in crawl["block_log"])


def test_crawl_single_403_is_counted_once_and_does_not_fall_back(crawl):
    assert crawl["stats"]["downloader/response_status_count/403"] == 1
    assert any(
        m.startswith("Block signal (HTTP 403). Consecutive: 1/")
        for m in crawl["block_log"]
    )
    # With BLOCK_DETECTION_THRESHOLD=1, one block is tolerated; no seed writes.
    assert not any(w["kind"] == "seed" for w in crawl["pg_writes"])
    assert crawl["es_created"] == []  # the crawler never creates the search-svc index


def test_block_threshold_exceeded_loads_seed_and_closes_engine(block):
    assert block["finish_reason"] == "blocked_fallback"
    seed_writes = [w for w in block["pg_writes"] if w["kind"] == "seed"]
    assert len(seed_writes) == 550
    assert len(block["es_actions"]) == 550
    ids = {a["_id"] for a in block["es_actions"]}
    assert len(ids) == 550
    assert all(a["_id"] == a["_source"]["id"] for a in block["es_actions"])
