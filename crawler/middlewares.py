"""Scrapy middlewares for job-platform-crawler.

BlockDetectionMiddleware (CRAWL-01-07):
  Counts consecutive 403/429 responses.
  If count exceeds threshold (5): activates seed fallback and closes spider.
"""

import logging
import os

from scrapy.exceptions import CloseSpider

logger = logging.getLogger("crawler.middlewares")


class BlockDetectionMiddleware:
    """Detect consecutive 403/429 responses and trigger seed fallback (CRAWL-01-07).

    Enabled only when DATABASE_URL_CRAWLER is set (pipelines are active).
    """

    def __init__(self, threshold: int = 5) -> None:
        self.threshold = threshold
        self.consecutive_failures = 0

    @classmethod
    def from_crawler(cls, crawler) -> "BlockDetectionMiddleware":  # type: ignore[override]
        threshold = crawler.settings.getint("BLOCK_DETECTION_THRESHOLD", 5)
        return cls(threshold=threshold)

    def process_response(self, request, response, spider):  # type: ignore[override]
        if response.status in (403, 429):
            self.consecutive_failures += 1
            logger.warning(
                "Consecutive failures: %d/%d. URL: %s",
                self.consecutive_failures,
                self.threshold,
                response.url,
            )
            if self.consecutive_failures > self.threshold:
                logger.warning(
                    "Consecutive failures exceeded %d. Activating seed fallback.",
                    self.threshold,
                )
                self._run_seed_fallback(spider)
                raise CloseSpider("blocked_fallback")
        elif response.status == 200:
            self.consecutive_failures = 0
        return response

    def process_exception(self, request, exception, spider):  # type: ignore[override]
        self.consecutive_failures += 1
        logger.warning(
            "Request exception (consecutive_failures=%d). URL: %s Error: %s",
            self.consecutive_failures,
            request.url,
            exception,
        )
        if self.consecutive_failures > self.threshold:
            logger.warning(
                "Consecutive failures exceeded %d. Activating seed fallback.",
                self.threshold,
            )
            self._run_seed_fallback(spider)
            raise CloseSpider("blocked_fallback")

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
