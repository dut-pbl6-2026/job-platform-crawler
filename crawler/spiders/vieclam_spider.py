"""VieclamSpider — crawl job listings from vieclam.gov.vn (CRAWL-01-01).

Primary source: public JSON API (no auth needed, verified 2026-09-08):
  POST {API_BASE}/api/svl/nld/vieclam?page_num=N&page_size=M
  body: {"tencv": "", ...filter ids/null..., "nhom_tin_tuyen_dung": <int|null>}
  response: {"success": true, "data": [...], "pagination": {"total_pages": N}}

Field mapping mirrors the site frontend mapper (verified in site bundle):
  id -> detail URL, vitri_td -> title, ten_ct -> company,
  ten_tinh1 (+ten_tinh2) -> location, muc_luong -> salary_raw,
  nganh_nghe -> category. The list API carries no description/requirements,
  so those stay None here (cleaned/enriched in later stages).

Fallback: HTML routes /search/ and /search/job-detail/ (allowed by
robots.txt). The site renders listings client-side, so HTML selectors below
are documented placeholders — refine vs rendered DOM if fallback is needed.

Stops after MAX_PAGES API pages (CRAWL-01-01). Respects robots.txt.
"""

import json
import logging
from typing import Any, Generator, Optional
from urllib.parse import urljoin

import scrapy
from scrapy.http import JsonRequest, Request, Response

from crawler.items import JobItem

logger = logging.getLogger("crawler.spiders.vieclam")

# POST body mirroring the site search form: empty keyword, no filters.
# nhom_tin_tuyen_dung is injected per settings (None = all groups).
SEARCH_BODY = {
    "tencv": "",
    "noi_lam_viec": None,
    "nghe_lvid": None,
    "chucvu_id": None,
    "linh_vuc_lvid": None,
    "kinh_nghiem_lvid": None,
    "muc_luongid": None,
    "hinh_thuc_lvid": None,
    "quymo": None,
    "kieu_dn": None,
    "nld_id": None,
    "getDataHistory": False,
}


class VieclamSpider(scrapy.Spider):
    name = "vieclam"
    allowed_domains = ["vieclam.gov.vn", "api.vieclam.gov.vn"]

    # Placeholder card selectors for /search/ fallback (refine vs rendered DOM).
    CARD_SELECTOR = "article.job-card, div.job-item, li.job-listing"
    CARD_LINK_SELECTOR = "a::attr(href)"
    NEXT_PAGE_SELECTOR = "a.next-page::attr(href), li.next a::attr(href)"

    # Placeholder detail selectors for /search/job-detail/ fallback.
    SEL_TITLE = "h1::text"
    SEL_COMPANY = ".company-name::text, .employer-name::text"
    SEL_LOCATION = ".job-location::text, .location::text"
    SEL_SALARY = ".job-salary::text, .salary::text"
    SEL_DESCRIPTION = ".job-description ::text, article.job-detail ::text"
    SEL_REQUIREMENTS = ".job-requirements ::text"
    SEL_CATEGORY = ".job-category::text, .industry::text"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # NOTE: kept as start_requests (not async start()) for Scrapy 2.11 compat.
        super().__init__(*args, **kwargs)
        self.max_pages: int = 10
        self.page_size: int = 20
        self.target: Optional[str] = None
        self.api_base: Optional[str] = None
        self.nhom_tin: Optional[int] = None
        self.page_count: int = 0
        self.item_count: int = 0

    @classmethod
    def from_crawler(cls, crawler: Any, *args: Any, **kwargs: Any) -> "VieclamSpider":
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.max_pages = crawler.settings.getint("MAX_PAGES")
        spider.page_size = crawler.settings.getint("PAGE_SIZE", 20)
        spider.target = crawler.settings.get("CRAWLER_TARGET")
        spider.api_base = crawler.settings.get("CRAWLER_API_BASE")
        nhom = crawler.settings.get("NHOM_TIN_TUYEN_DUNG")
        spider.nhom_tin = int(nhom) if nhom not in (None, "") else None
        if not spider.api_base:
            raise RuntimeError(
                "CRAWLER_API_BASE is not set. "
                "Set it via .env (see .env.example) or -s CRAWLER_API_BASE=..."
            )
        return spider

    def _search_body(self) -> dict:
        body = dict(SEARCH_BODY)
        body["nhom_tin_tuyen_dung"] = self.nhom_tin
        return body

    def _search_url(self, page_num: int) -> str:
        return (
            f"{self.api_base}/api/svl/nld/vieclam"
            f"?page_num={page_num}&page_size={self.page_size}"
        )

    def start_requests(self) -> Generator[Request, None, None]:
        logger.info(
            "Spider opened. MAX_PAGES=%d PAGE_SIZE=%d NHOM=%s",
            self.max_pages,
            self.page_size,
            self.nhom_tin,
        )
        yield JsonRequest(
            self._search_url(1),
            data=self._search_body(),
            callback=self.parse_api,
            errback=self.err_api,
            meta={"page_num": 1},
        )

    # -- JSON API ---------------------------------------------------------
    def parse_api(self, response: Response) -> Generator[Any, None, None]:
        """Parse job-list JSON: data[] + pagination.total_pages, page while < MAX_PAGES.

        NOTE: no spider contract here on purpose — the source API is POST-only
        and built-in contracts can only issue GET (which returns 404). This path
        is verified via manual live runs (`scrapy crawl vieclam -o output/live.json`,
        see README) against tests/fixtures/vieclam_sample.json.
        """
        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError:
            logger.warning("API returned non-JSON body. URL: %s", response.url)
            return

        entries = payload.get("data", []) or []
        if not entries:
            logger.warning("API returned empty data list. URL: %s", response.url)
            return

        for entry in entries:
            self.item_count += 1
            yield self._api_entry_to_item(entry)

        page_num = int(response.meta.get("page_num", 1))
        self.page_count = max(self.page_count, page_num)
        total_pages = ((payload.get("pagination") or {}).get("total_pages") or 0) or 0
        logger.debug(
            "API page %d/%s: %d entries (total items scraped: %d).",
            page_num,
            total_pages or "?",
            len(entries),
            self.item_count,
        )

        if self.page_count >= self.max_pages:
            logger.info("MAX_PAGES=%d reached, stopping.", self.max_pages)
            return
        if total_pages and page_num >= total_pages:
            logger.info("Last API page (%d) reached, stopping.", total_pages)
            return
        yield JsonRequest(
            self._search_url(page_num + 1),
            data=self._search_body(),
            callback=self.parse_api,
            errback=self.err_api,
            meta={"page_num": page_num + 1},
        )

    def _api_entry_to_item(self, entry: dict) -> JobItem:
        """Map API entry to JobItem (frontend mapper parity, None-safe)."""
        item = JobItem()
        job_id = entry.get("id")
        item["source_url"] = (
            f"{self.target}/search/job-detail?id={job_id}" if job_id else None
        )
        item["title"] = entry.get("vitri_td")
        item["company"] = entry.get("ten_ct")
        tinh1 = entry.get("ten_tinh1")
        tinh2 = entry.get("ten_tinh2")
        item["location"] = f"{tinh1}, {tinh2}" if tinh1 and tinh2 else (tinh1 or tinh2)
        item["salary_raw"] = entry.get("muc_luong")
        # List API carries no description/requirements (frontend falls back to
        # title/[]); keep None per CRAWL-01-02 missing-field handling.
        item["description"] = None
        item["requirements"] = None
        item["category"] = entry.get("nganh_nghe")
        return item

    def err_api(self, failure: Any) -> None:
        response = getattr(failure.value, "response", None)
        status = getattr(response, "status", None)
        url = getattr(response, "url", None) or failure.request.url
        if status in (401, 403, 429):
            logger.warning("JSON API blocked (status=%d). URL: %s", status, url)
        else:
            logger.warning(
                "JSON API request failed (status=%s). URL: %s Error: %s",
                status,
                url,
                failure.getErrorMessage(),
            )

    # -- HTML fallbacks (robots-allowed routes) ------------------------------
    def parse(self, response: Response) -> Generator[Any, None, None]:
        """Parse /search/ listing: follow detail cards + next page (< MAX_PAGES).

        @url https://vieclam.gov.vn/search/
        @returns requests 1 1
        @returns items 0 0

        Bounds assume the current server-rendered shell: listings render
        client-side, so no placeholder card matches (0 follows) and only the
        placeholder pagination request is yielded. Update when refining the
        HTML fallback selectors vs the rendered DOM.
        """
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
            logger.info(
                "MAX_PAGES=%d reached, stopping listing pagination.", self.max_pages
            )
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
                urljoin(self.target or "", f"/search/?page={page + 1}"),
                callback=self.parse,
                errback=self.err_listing,
                meta={"page": page + 1},
            )

    def err_listing(self, failure: Any) -> None:
        response = getattr(failure.value, "response", None)
        status = getattr(response, "status", None)
        url = getattr(response, "url", None) or failure.request.url
        logger.warning(
            "Listing request failed (status=%s). URL: %s Error: %s",
            status,
            url,
            failure.getErrorMessage(),
        )

    def parse_job(self, response: Response) -> Generator[JobItem, None, None]:
        """Parse /search/job-detail/ page into JobItem (None for missing fields).

        @url https://vieclam.gov.vn/search/job-detail?id=388116
        @returns items 1 1
        @scrapes source_url title company location salary_raw description requirements category

        Detail pages render client-side, so placeholder selectors find nothing
        and fields stay None — the contract pins the JobItem shape (all keys
        present), not values. Update bounds when refining selectors.
        """
        base = response.meta.get("api_item", {})
        item = JobItem()
        item["source_url"] = response.url
        item["title"] = base.get("title") or self._first_text(response, self.SEL_TITLE)
        item["company"] = base.get("company") or self._first_text(
            response, self.SEL_COMPANY
        )
        item["location"] = base.get("location") or self._first_text(
            response, self.SEL_LOCATION
        )
        item["salary_raw"] = base.get("salary_raw") or self._first_text(
            response, self.SEL_SALARY
        )
        item["description"] = base.get("description") or self._all_text(
            response, self.SEL_DESCRIPTION
        )
        item["requirements"] = base.get("requirements") or self._all_text(
            response, self.SEL_REQUIREMENTS
        )
        item["category"] = base.get("category") or self._first_text(
            response, self.SEL_CATEGORY
        )
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
            parts = [t.strip() for t in response.css(sel.strip()).getall() if t.strip()]
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
