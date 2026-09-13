"""Scrapy extensions for job-platform-crawler.

CrawlStatsExtension logs a summary line when a spider closes:
scraped / dropped / errors / pages / elapsed / reason.
"""

import logging

from scrapy import signals

logger = logging.getLogger("crawler.extensions")


class CrawlStatsExtension:
    """Log crawl summary stats at spider close."""

    @classmethod
    def from_crawler(cls, crawler):  # type: ignore[override]
        ext = cls()
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_closed(self, spider, reason: str) -> None:
        stats = spider.crawler.stats.get_stats()
        logger.info(
            "Crawl summary: scraped=%d, dropped=%d, errors=%d, "
            "pages=%d, elapsed=%.1fs, reason=%s",
            stats.get("item_scraped_count", 0),
            stats.get("item_dropped_count", 0),
            stats.get("log_count/ERROR", 0),
            stats.get("response_received_count", 0),
            stats.get("elapsed_time_seconds", 0),
            reason,
        )
