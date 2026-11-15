"""Scrapy middlewares for job-platform-crawler.

BlockDetectionMiddleware (CRAWL-01-06, CRAWL-01-07):
  - Block signal = HTTP 403/429, or a 200 HTML page whose <title> is a known
    block/challenge page. Consecutive block signals above the threshold (5)
    activate the seed fallback and close the spider.
  - Network errors (timeouts, refused or reset connections, DNS) are not block
    signals. They are retried with exponential backoff (RETRY_TIMES attempts,
    NETWORK_RETRY_BACKOFF * 2**n seconds) and then logged; the request errback
    receives the failure. settings.py sets RETRY_EXCEPTIONS = [] so Scrapy's
    RetryMiddleware does not retry them without backoff.
"""

import logging
import os
import re

from scrapy.exceptions import IgnoreRequest
from twisted.internet import reactor
from twisted.internet.task import deferLater

logger = logging.getLogger("crawler.middlewares")

BLOCK_STATUSES = frozenset({403, 429})

# Known block/challenge page titles. Heuristic: matched on <title> only, so
# normal pages that merely mention these words in the body are not counted.
# Not verified against vieclam.gov.vn (no live-site access in tests).
BLOCK_PAGE_TITLE_MARKERS = (
    "access denied",
    "attention required",
    "pardon our interruption",
    "just a moment",
)
_TITLE_RE = re.compile(rb"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_TITLE_SCAN_BYTES = 65536


def block_reason(response) -> str | None:
    """Return why *response* is a block signal, or None if it is not one."""
    if response.status in BLOCK_STATUSES:
        return f"HTTP {response.status}"
    content_type = (response.headers.get("Content-Type") or b"").lower()
    if response.status == 200 and b"html" in content_type:
        match = _TITLE_RE.search(response.body[:_TITLE_SCAN_BYTES])
        if match:
            title = match.group(1).decode("utf-8", "replace").strip().lower()
            if any(marker in title for marker in BLOCK_PAGE_TITLE_MARKERS):
                return "block page"
    return None


class BlockDetectionMiddleware:
    """Count consecutive block signals; retry network errors with backoff (CRAWL-01-07)."""

    def __init__(
        self,
        threshold: int = 5,
        network_retry_times: int = 3,
        network_retry_backoff: float = 2.0,
    ) -> None:
        self.threshold = threshold
        self.network_retry_times = network_retry_times
        self.network_retry_backoff = network_retry_backoff
        self.consecutive_failures = 0

    @classmethod
    def from_crawler(cls, crawler) -> "BlockDetectionMiddleware":  # type: ignore[override]
        return cls(
            threshold=crawler.settings.getint("BLOCK_DETECTION_THRESHOLD", 5),
            network_retry_times=crawler.settings.getint("RETRY_TIMES", 3),
            network_retry_backoff=crawler.settings.getfloat(
                "NETWORK_RETRY_BACKOFF", 2.0
            ),
        )

    def process_response(self, request, response, spider):  # type: ignore[override]
        reason = block_reason(response)
        if reason is not None:
            self.consecutive_failures += 1
            logger.warning(
                "Block signal (%s). Consecutive: %d/%d. URL: %s",
                reason,
                self.consecutive_failures,
                self.threshold,
                response.url,
            )
            if self.consecutive_failures > self.threshold:
                logger.warning(
                    "Consecutive block signals exceeded %d. Activating seed fallback.",
                    self.threshold,
                )
                self._run_seed_fallback(spider)
                # Raising CloseSpider here does not stop the crawl: Scrapy turns it
                # into a download error on this request. Close the engine instead.
                spider.crawler.engine.close_spider(spider, reason="blocked_fallback")
                raise IgnoreRequest("blocked_fallback")
        elif response.status == 200:
            self.consecutive_failures = 0
        return response

    def process_exception(self, request, exception, spider):  # type: ignore[override]
        """Retry a network error with backoff. Never counted as a block signal."""
        attempt = request.meta.get("network_retry_count", 0)
        if attempt >= self.network_retry_times:
            logger.error(
                "Network error, retries exhausted after %d attempts. URL: %s Error: %s",
                attempt,
                request.url,
                exception,
            )
            return None
        delay = self.network_retry_backoff * (2**attempt)
        logger.warning(
            "Network error, retry %d/%d in %.1fs. URL: %s Error: %s",
            attempt + 1,
            self.network_retry_times,
            delay,
            request.url,
            exception,
        )
        retry = request.replace(
            dont_filter=True,
            meta={**request.meta, "network_retry_count": attempt + 1},
        )
        return self._after(delay, retry)

    @staticmethod
    def _after(delay: float, request):
        """Return a Deferred that yields *request* after *delay* seconds (non-blocking)."""
        return deferLater(reactor, delay, lambda: request)

    @staticmethod
    def _run_seed_fallback(spider) -> None:  # type: ignore[override]
        """Load seed data into PG + ES as fallback (CRAWL-01-07)."""
        pg_url = os.environ.get("DATABASE_URL_CRAWLER")
        es_url = os.environ.get("ELASTICSEARCH_URL")
        es_index = os.environ.get("ELASTICSEARCH_INDEX", "jobs")
        seed_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "seed",
            "jobs.json",
        )
        try:
            from scripts.seed_loader import load_seed

            load_seed(
                pg_url=pg_url,
                es_url=es_url,
                es_index=es_index,
                seed_path=seed_path,
            )
        except Exception as exc:
            logger.error("Seed fallback failed: %s", exc)
