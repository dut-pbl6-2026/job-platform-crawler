"""Unit tests for CleaningPipeline and DedupPipeline (CRAWL-01-02, CRAWL-01-03)."""

import pytest
from scrapy.exceptions import DropItem

from crawler.items import JobItem
from crawler.pipelines import CleaningPipeline, DedupPipeline


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
