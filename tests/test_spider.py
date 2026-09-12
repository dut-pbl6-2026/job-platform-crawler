"""Unit tests for VieclamSpider (CRAWL-01-01)."""

import json

from scrapy.http import HtmlResponse, Request, TextResponse

from crawler.spiders.vieclam_spider import VieclamSpider

TARGET = "https://vieclam.gov.vn"
API_BASE = "https://api.vieclam.gov.vn/webhook"


def make_spider(max_pages: int = 10) -> VieclamSpider:
    spider = VieclamSpider()
    spider.target = TARGET
    spider.api_base = API_BASE
    spider.max_pages = max_pages
    spider.page_size = 20
    spider.nhom_tin = None
    spider.page_count = 0
    spider.item_count = 0
    return spider


def fake_api_response(
    spider: VieclamSpider,
    entries: list,
    page_num: int = 1,
    total_pages: int = 3,
    raw_text: str | None = None,
) -> TextResponse:
    body = (
        raw_text
        if raw_text is not None
        else json.dumps(
            {
                "success": True,
                "data": entries,
                "pagination": {"total_pages": total_pages},
            }
        )
    )
    request = Request(
        url=f"{API_BASE}/api/svl/nld/vieclam?page_num={page_num}&page_size=20",
        meta={"page_num": page_num},
    )
    return TextResponse(
        url=request.url,
        body=body.encode("utf-8"),
        encoding="utf-8",
        request=request,
    )


def fake_html_response(
    url: str, body: str, meta: dict | None = None, api_item: dict | None = None
) -> HtmlResponse:
    meta = dict(meta or {})
    if api_item is not None:
        meta["api_item"] = api_item
    request = Request(url=url, meta=meta)
    return HtmlResponse(
        url=url,
        body=body.encode("utf-8"),
        encoding="utf-8",
        request=request,
    )


SAMPLE_ENTRY = {
    "id": 388116,
    "vitri_td": "Ky su phan mem",
    "ten_ct": "Cong ty TNHH ABC",
    "ten_tinh1": "Da Nang",
    "ten_tinh2": None,
    "muc_luong": "10 - 20 trieu",
    "nganh_nghe": "CNTT",
}


def test_api_entry_to_item_mapping():
    spider = make_spider()
    item = spider._api_entry_to_item(SAMPLE_ENTRY)
    assert item["title"] == "Ky su phan mem"
    assert item["company"] == "Cong ty TNHH ABC"
    assert item["location"] == "Da Nang"
    assert item["salary_raw"] == "10 - 20 trieu"
    assert item["category"] == "CNTT"
    assert item["source_url"] == f"{TARGET}/search/job-detail?id=388116"


def test_api_entry_to_item_missing_id():
    spider = make_spider()
    entry = dict(SAMPLE_ENTRY)
    entry.pop("id")
    item = spider._api_entry_to_item(entry)
    assert item["source_url"] is None


def test_api_entry_to_item_two_provinces():
    spider = make_spider()
    entry = dict(SAMPLE_ENTRY)
    entry["ten_tinh1"] = "Tinh Bac Ninh"
    entry["ten_tinh2"] = "Tinh Quang Ninh"
    item = spider._api_entry_to_item(entry)
    assert item["location"] == "Tinh Bac Ninh, Tinh Quang Ninh"


def test_parse_api_yields_items_and_next_page():
    spider = make_spider(max_pages=10)
    entries = [dict(SAMPLE_ENTRY, id=1), dict(SAMPLE_ENTRY, id=2)]
    response = fake_api_response(spider, entries, page_num=1, total_pages=3)
    results = list(spider.parse_api(response))
    items = [r for r in results if not isinstance(r, Request)]
    requests = [r for r in results if isinstance(r, Request)]
    assert len(items) == 2
    assert items[0]["source_url"] != ""
    assert len(requests) == 1
    assert "page_num=2" in requests[0].url


def test_parse_api_stops_at_max_pages():
    spider = make_spider(max_pages=2)
    entries = [dict(SAMPLE_ENTRY, id=1)]
    response = fake_api_response(spider, entries, page_num=2, total_pages=5)
    results = list(spider.parse_api(response))
    requests = [r for r in results if isinstance(r, Request)]
    assert requests == []


def test_parse_api_stops_at_last_page():
    spider = make_spider(max_pages=100)
    entries = [dict(SAMPLE_ENTRY, id=1)]
    response = fake_api_response(spider, entries, page_num=5, total_pages=5)
    results = list(spider.parse_api(response))
    requests = [r for r in results if isinstance(r, Request)]
    assert requests == []


def test_parse_api_non_json_body():
    spider = make_spider()
    response = fake_api_response(spider, [], raw_text="<html>not json</html>")
    assert list(spider.parse_api(response)) == []


def test_parse_api_empty_data():
    spider = make_spider()
    response = fake_api_response(spider, [], page_num=1, total_pages=3)
    assert list(spider.parse_api(response)) == []


def test_parse_listing_follows_cards_and_pagination():
    spider = make_spider(max_pages=10)
    html = """
    <html><body>
      <article class="job-card"><a href="/search/job-detail?id=1">Job 1</a></article>
      <article class="job-card"><a href="/search/job-detail?id=2">Job 2</a></article>
      <a class="next-page" href="/search/?page=2">Next</a>
    </body></html>
    """
    response = fake_html_response(f"{TARGET}/search/", html, meta={"page": 1})
    results = list(spider.parse(response))
    requests = [r for r in results if isinstance(r, Request)]
    # 2 detail requests + 1 next-page request
    assert len(requests) == 3


def test_parse_stops_at_max_pages():
    spider = make_spider(max_pages=1)
    spider.page_count = 1
    html = (
        "<html><body><a class='next-page' href='/search/?page=2'>Next</a></body></html>"
    )
    response = fake_html_response(f"{TARGET}/search/", html, meta={"page": 1})
    assert list(spider.parse(response)) == []


def test_parse_job_returns_job_item():
    spider = make_spider()
    html = """
    <html><body>
      <h1>Ky su phan mem</h1>
      <div class="company-name">Cong ty TNHH ABC</div>
      <div class="job-location">Da Nang</div>
      <div class="job-salary">10 - 20 trieu</div>
      <div class="job-category">CNTT</div>
    </body></html>
    """
    url = f"{TARGET}/search/job-detail?id=388116"
    response = fake_html_response(url, html)
    items = list(spider.parse_job(response))
    assert len(items) == 1
    item = items[0]
    assert item["source_url"] == url
    assert item["title"] == "Ky su phan mem"
    assert item["company"] == "Cong ty TNHH ABC"
    for field in (
        "source_url",
        "title",
        "company",
        "location",
        "salary_raw",
        "description",
        "requirements",
        "category",
    ):
        assert field in item
