"""Unit tests for CleaningPipeline and DedupPipeline (CRAWL-01-02, CRAWL-01-03)."""

from unittest.mock import MagicMock, patch

import pytest
from scrapy.exceptions import DropItem

from crawler.items import JobItem
from crawler.pipelines import (
    CleaningPipeline,
    DedupPipeline,
    ElasticsearchPipeline,
    PostgresPipeline,
)


def make_item(**overrides) -> JobItem:
    base = {
        "source_url": "https://vieclam.gov.vn/search/job-detail?id=1",
        "title": "Ky su phan mem",
        "company": "Cong ty TNHH ABC",
        "location": "Da Nang",
        "salary_raw": "10 - 20 trieu",
        "description": "Mo ta cong viec",
        "requirements": "Kinh nghiem 2 nam",
        "category": "CNTT",
    }
    base.update(overrides)
    return JobItem(base)


def test_cleaning_strips_html():
    pipe = CleaningPipeline()
    item = make_item(description="<b>Java</b> Developer")
    out = pipe.process_item(item, spider=None)
    assert out["description"] == "Java Developer"


def test_cleaning_normalizes_salary_range():
    pipe = CleaningPipeline()
    item = make_item(salary_raw="5 - 10 trieu")
    out = pipe.process_item(item, spider=None)
    assert out["salary_min"] == 5_000_000
    assert out["salary_max"] == 10_000_000
    assert out["salary_currency"] == "VND"


def test_cleaning_missing_salary_returns_none():
    pipe = CleaningPipeline()
    item = make_item(salary_raw=None)
    out = pipe.process_item(item, spider=None)
    assert out["salary_min"] is None
    assert out["salary_max"] is None


def test_cleaning_normalizes_location():
    pipe = CleaningPipeline()
    item = make_item(location="HCM")
    out = pipe.process_item(item, spider=None)
    assert out["location"] == "Ho Chi Minh"


def test_cleaning_strips_whitespace_and_nones():
    pipe = CleaningPipeline()
    item = make_item(title="  Ky su  ", company=None)
    out = pipe.process_item(item, spider=None)
    assert out["title"] == "Ky su"
    assert out["company"] is None


def test_dedup_new_url_passes():
    pipe = DedupPipeline()
    pipe.open_spider(spider=None)
    item = make_item(source_url="https://vieclam.gov.vn/seed/1")
    assert pipe.process_item(item, spider=None) is item


def test_dedup_duplicate_url_raises():
    pipe = DedupPipeline()
    pipe.open_spider(spider=None)
    item = make_item(source_url="https://vieclam.gov.vn/seed/1")
    pipe.process_item(item, spider=None)
    with pytest.raises(DropItem):
        pipe.process_item(
            make_item(source_url="https://vieclam.gov.vn/seed/1"), spider=None
        )


def test_dedup_missing_url_raises():
    pipe = DedupPipeline()
    pipe.open_spider(spider=None)
    with pytest.raises(DropItem):
        pipe.process_item(make_item(source_url=None), spider=None)


def make_pg_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = False
    mock_cursor.fetchone.return_value = [1]
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.closed = False
    return mock_conn


def test_postgres_batch_commit_flushes_every_50():
    mock_conn = make_pg_conn()
    with patch("psycopg2.connect", return_value=mock_conn):
        pipe = PostgresPipeline()
        pipe.open_spider(spider=None)
        for i in range(55):
            pipe.process_item(
                make_item(source_url=f"https://vieclam.gov.vn/seed/{i}"),
                spider=None,
            )
        # open commit + 1 batch commit at 50 items
        assert mock_conn.commit.call_count == 2
        pipe.close_spider(spider=None)
        # close flushes remaining 5 items
        assert mock_conn.commit.call_count == 3
        assert mock_conn.close.called


def test_elasticsearch_bulk_flushes_every_50():
    mock_es = MagicMock()
    mock_es.ping.return_value = True
    mock_es.indices.exists.return_value = True
    with patch("elasticsearch.Elasticsearch", return_value=mock_es):
        pipe = ElasticsearchPipeline()
        pipe.open_spider(spider=None)
        pipe.es = mock_es
        with patch("elasticsearch.helpers.bulk", return_value=(50, [])) as mock_bulk:
            for i in range(55):
                item = make_item(source_url=f"https://vieclam.gov.vn/seed/{i}")
                item["pg_id"] = i
                pipe.process_item(item, spider=None)
            assert mock_bulk.call_count == 1
            assert len(pipe._buffer) == 5
            pipe.close_spider(spider=None)
            assert mock_bulk.call_count == 2
            assert len(pipe._buffer) == 0
            assert mock_es.indices.refresh.called
