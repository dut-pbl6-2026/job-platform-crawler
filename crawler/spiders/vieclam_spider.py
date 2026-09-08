"""VieclamSpider — crawl job listings from vieclam.gov.vn (CRAWL-01-01).

Source strategy (in order):
1. Public JSON API ``/api/svl/trang-chu/tin-tuyen-dung-noi-bat`` observed in the
   site bundle (contract: ``data.data.content`` list, ``page_index``/``page_size``
   params). Works only when the endpoint answers anonymous callers.
2. HTML listing route ``/search/`` (allowed by robots.txt) with card selectors.
3. Detail route ``/search/job-detail/`` for full description/requirements.

NOTE on selectors: vieclam.gov.vn renders listings client-side (SPA shell),
so CSS/XPath below are documented placeholders. Refine them after inspecting
the rendered DOM (browser DevTools) or when the JSON API is reachable with a
token. The spider never raises on missing fields — they stay None (CRAWL-01-02).

Stops after MAX_PAGES pages (CRAWL-01-01). Respects robots.txt via settings.
"""

import json
import logging
from typing import Any, Generator, Optional
from urllib.parse import urljoin

import scrapy
from scrapy.http import JsonRequest, Request, Response

from crawler.items import JobItem

logger = logging.getLogger("crawler.spiders.vieclam")


class VieclamSpider(scrapy.Spider):
    name = "vieclam"
    allowed_domains = ["vieclam.gov.vn"]

    # Placeholder card selectors for /search/ listing (refine vs rendered DOM).
    CARD_SELECTOR = "article.job-card, div.job-item, li.job-listing"
    CARD_LINK_SELECTOR = "a::attr(href)"
    NEXT_PAGE_SELECTOR = "a.next-page::attr(href), li.next a::attr(href)"

    # Placeholder detail selectors for /search/job-detail/ (refine vs rendered DOM).
    SEL_TITLE = "h1::text"
    SEL_COMPANY = ".company-name::text, .employer-name::text"
    SEL_LOCATION = ".job-location::text, .location::text"
    SEL_SALARY = ".job-salary::text, .salary::text"
    SEL_DESCRIPTION = ".job-description ::text, article.job-detail ::text"
    SEL_REQUIREMENTS = ".job-requirements ::text"
    SEL_CATEGORY = ".job-category::text, .industry::text"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.max_pages: int = 10
        self.page_size: int = 20
        self.target: str = "https://vieclam.gov.vn"
        self.page_count: int = 0
        self.item_count: int = 0

    @classmethod
    def from_crawler(cls, crawler: Any, *args: Any, **kwargs: Any) -> "VieclamSpider":
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.max_pages = crawler.settings.getint("MAX_PAGES")
        spider.page_size = crawler.settings.getint("PAGE_SIZE", 20)
        spider.target = str(crawler.settings.get("CRAWLER_TARGET"))
        return spider

    def start_requests(self) -> Generator[Request, None, None]:
        logger.info("Spider opened. MAX_PAGES=%d", self.max_pages)
        # 1. Public JSON API first (anonymous-friendly when available).
        api_url = (
            f"{self.target}/api/svl/trang-chu/tin-tuyen-dung-noi-bat"
            f"?page_index=0&page_size={self.page_size}"
        )
        yield JsonRequest(
            api_url,
            callback=self.parse_api,
            errback=self.err_api,
            meta={"page_index": 0},
        )
        # 2. HTML listing route (robots.txt: Allow /search/).
        yield Request(
            urljoin(self.target, "/search/"),
            callback=self.parse,
            errback=self.err_listing,
            meta={"page": 1},
        )

    # -- JSON API ---------------------------------------------------------
    def parse_api(self, response: Response) -> Generator[Any, None, None]:
        """Parse featured-jobs JSON: data.data.content list + pagination."""
        if response.status in (401, 403):
            logger.warning(
                "JSON API unauthorized (status=%d). Falling back to HTML routes. URL: %s",
                response.status,
                response.url,
            )
            return
        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError:
            logger.warning("JSON API returned non-JSON body. URL: %s", response.url)
            return

        content = payload.get("data", {}).get("content", []) or []
        if not content:
            logger.warning("JSON API returned empty content. URL: %s", response.url)
            return

        for entry in content:
            item = self._api_entry_to_item(entry)
            detail_url = self._api_detail_url(entry)
            if detail_url:
                yield Request(
                    detail_url,
                    callback=self.parse_job,
                    errback=self.err_detail,
                    meta={"api_item": dict(item)},
                )
            else:
                self.item_count += 1
                yield item

        # Pagination over the API while under MAX_PAGES.
        page_index = int(response.meta.get("page_index", 0))
        self.page_count = max(self.page_count, page_index + 1)
        if self.page_count < self.max_pages:
            next_index = page_index + 1
            next_url = (
                f"{self.target}/api/svl/trang-chu/tin-tuyen-dung-noi-bat"
                f"?page_index={next_index}"
                f"&page_size={self.page_size}"
            )
            yield JsonRequest(
                next_url,
                callback=self.parse_api,
                errback=self.err_api,
                meta={"page_index": next_index},
            )
        else:
            logger.info("MAX_PAGES=%d reached, stopping API pagination.", self.max_pages)

    def _api_entry_to_item(self, entry: dict) -> JobItem:
        """Map an API entry to JobItem defensively (keys vary by endpoint)."""
        item = JobItem()
        item["source_url"] = (
            entry.get("source_url")
            or entry.get("sourceUrl")
            or self._api_detail_url(entry)
        )
        item["title"] = entry.get("title") or entry.get("tieuDe")
        item["company"] = entry.get("company") or entry.get("tenDoanhNghiep")
        item["location"] = entry.get("location") or entry.get("diaDiem")
        item["salary_raw"] = entry.get("salary_raw") or entry.get("mucluong")
        item["description"] = entry.get("description") or entry.get("moTa")
        item["requirements"] = entry.get("requirements") or entry.get("yeuCau")
        item["category"] = entry.get("category") or entry.get("nganhNghe")
        return item

    def _api_detail_url(self, entry: dict) -> Optional[str]:
        direct = entry.get("detail_url") or entry.get("detailUrl")
        if direct:
            return urljoin(self.target, str(direct))
        job_id = entry.get("id") or entry.get("jobId")
        if job_id:
            return urljoin(self.target, f"/search/job-detail/{job_id}")
        return None

    def err_api(self, failure: Any) -> None:
        logger.warning("JSON API request failed: %s", failure.getErrorMessage())

    # -- HTML listing ------------------------------------------------------
    def parse(self, response: Response) -> Generator[Any, None, None]:
        """Parse /search/ listing: follow detail cards + next page (< MAX_PAGES)."""
        cards = response.css(self.CARD_SELECTOR)
        logger.debug("Listing page %s: %d cards found.", response.url, len(cards))
        for card in cards:
            href = card.css(self.CARD_LINK_SELECTOR).get()
            if href:
                yield response.follow(
                    href, callback=self.parse_job, errback=self.err_detail
                )

        page = int(response.meta.get("page", 1))
        self.page_count = max(self.page_count, page)
        if self.page_count >= self.max_pages:
            logger.info("MAX_PAGES=%d reached, stopping listing pagination.", self.max_pages)
            return

        next_href = response.css(self.NEXT_PAGE_SELECTOR).get()
        if next_href:
            yield response.follow(
                next_href,
                callback=self.parse,
                errback=self.err_listing,
                meta={"page": page + 1},
            )
        else:
            # Placeholder pagination param (refine vs real site behaviour).
            yield Request(
                urljoin(self.target, f"/search/?page={page + 1}"),
                callback=self.parse,
                errback=self.err_listing,
                meta={"page": page + 1},
            )

    def err_listing(self, failure: Any) -> None:
        logger.warning("Listing request failed: %s", failure.getErrorMessage())

    # -- HTML detail --------------------------------------------------------
    def parse_job(self, response: Response) -> Generator[JobItem, None, None]:
        """Parse /search/job-detail/ page into JobItem (None for missing fields)."""
        base = response.meta.get("api_item", {})
        item = JobItem()
        item["source_url"] = response.url
        item["title"] = base.get("title") or self._first_text(response, self.SEL_TITLE)
        item["company"] = base.get("company") or self._first_text(response, self.SEL_COMPANY)
        item["location"] = base.get("location") or self._first_text(response, self.SEL_LOCATION)
        item["salary_raw"] = base.get("salary_raw") or self._first_text(response, self.SEL_SALARY)
        item["description"] = base.get("description") or self._all_text(response, self.SEL_DESCRIPTION)
        item["requirements"] = base.get("requirements") or self._all_text(response, self.SEL_REQUIREMENTS)
        item["category"] = base.get("category") or self._first_text(response, self.SEL_CATEGORY)
        self.item_count += 1
        logger.debug("Scraped item #%d: %s", self.item_count, item.get("title"))
        yield item

    def err_detail(self, failure: Any) -> None:
        request = failure.request
        logger.warning(
            "Detail request failed. URL: %s Status: %s",
            request.url,
            getattr(failure.value, "status", "exception"),
        )

    # -- helpers -------------------------------------------------------------
    @staticmethod
    def _first_text(response: Response, selector: str) -> Optional[str]:
        for sel in selector.split(","):
            text = response.css(sel.strip()).get()
            if text and text.strip():
                return text.strip()
        return None

    @staticmethod
    def _all_text(response: Response, selector: str) -> Optional[str]:
        for sel in selector.split(","):
            parts = [
                t.strip() for t in response.css(sel.strip()).getall() if t.strip()
            ]
            if parts:
                return " ".join(parts)
        return None

    def closed(self, reason: str) -> None:
        logger.info(
            "Spider closed (%s). Pages visited: %d, items scraped: %d.",
            reason,
            self.page_count,
            self.item_count,
        )
