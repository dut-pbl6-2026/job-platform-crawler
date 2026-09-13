"""Unit tests for CrawlStatsExtension (Day Wed scale-up)."""

import logging
from unittest.mock import MagicMock

from crawler.extensions import CrawlStatsExtension


def make_spider(stats: dict) -> MagicMock:
    spider = MagicMock()
    spider.crawler.stats.get_stats.return_value = stats
    return spider


def test_from_crawler_connects_signal():
    crawler = MagicMock()
    ext = CrawlStatsExtension.from_crawler(crawler)
    assert isinstance(ext, CrawlStatsExtension)
    crawler.signals.connect.assert_called_once()


def test_spider_closed_logs_summary(caplog):
    ext = CrawlStatsExtension()
    spider = make_spider(
        {
            "item_scraped_count": 523,
            "item_dropped_count": 12,
            "log_count/ERROR": 0,
            "response_received_count": 52,
            "elapsed_time_seconds": 312.4,
        }
    )
    with caplog.at_level(logging.INFO, logger="crawler.extensions"):
        ext.spider_closed(spider, "finished")
    assert "Crawl summary: scraped=523" in caplog.text
    assert "reason=finished" in caplog.text


def test_spider_closed_missing_stats_defaults_to_zero(caplog):
    ext = CrawlStatsExtension()
    spider = make_spider({})
    with caplog.at_level(logging.INFO, logger="crawler.extensions"):
        ext.spider_closed(spider, "cancelled")
    assert "scraped=0" in caplog.text
    assert "reason=cancelled" in caplog.text
