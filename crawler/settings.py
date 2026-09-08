"""Scrapy settings for job-platform-crawler (CRAWL-01-01, CRAWL-01-06).

All environment-driven values use fail-open defaults for local dev;
pipelines raise on missing config themselves (PR2).
"""

import os

from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "crawler"

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

# CRAWL-01-01: politeness — 1-3s delay, adaptive throttle, obey robots.txt
DOWNLOAD_DELAY = 2
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
ROBOTSTXT_OBEY = True
USER_AGENT = "PBL6-JobPlatform-Crawler/1.0 (+https://github.com/dut-pbl6-2026)"

# CRAWL-01-06: retry 3 times with exponential backoff
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# HTTPCACHE only in development (never in production)
SCRAPY_ENV = os.environ.get("SCRAPY_ENV", "development")
HTTPCACHE_ENABLED = SCRAPY_ENV == "development"
HTTPCACHE_DIR = ".scrapy_cache"

# Page limit (CRAWL-01-01): 10 = dev crawl (~100 jobs), 100 = full crawl (~1000 jobs)
MAX_PAGES = int(os.environ.get("MAX_PAGES", "10"))

# Pipeline chain (classes land in PR2: feature/crawler-pipelines)
ITEM_PIPELINES = {
    "crawler.pipelines.CleaningPipeline": 100,
    "crawler.pipelines.DedupPipeline": 200,
    "crawler.pipelines.PostgresPipeline": 300,
    "crawler.pipelines.ElasticsearchPipeline": 400,
}

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Target site base URL (public routes allowed by robots.txt: /search/, /search/job-detail/)
CRAWLER_TARGET = os.environ.get("CRAWLER_TARGET", "https://vieclam.gov.vn")

# Scrapy 2.13+: request fingerprinter defaults to 2.7, no override needed.
FEED_EXPORT_ENCODING = "utf-8"
