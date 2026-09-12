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

# API page size (items per page_num request)
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "20"))

# Job group filter, mirrors ?nhom_tin_tuyen_dung= on /search/ (None = all groups)
NHOM_TIN_TUYEN_DUNG = os.environ.get("NHOM_TIN_TUYEN_DUNG") or None

# Pipeline chain (classes land in PR2: feature/crawler-pipelines)
ITEM_PIPELINES = {
    "crawler.pipelines.CleaningPipeline": 100,
    "crawler.pipelines.DedupPipeline": 200,
    "crawler.pipelines.PostgresPipeline": 300,
    "crawler.pipelines.ElasticsearchPipeline": 400,
}

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Downloader middlewares — BlockDetectionMiddleware (CRAWL-01-07)
DOWNLOADER_MIDDLEWARES = {
    "crawler.middlewares.BlockDetectionMiddleware": 543,
}

# Number of consecutive 403/429 responses that triggers seed fallback
BLOCK_DETECTION_THRESHOLD = int(os.environ.get("BLOCK_DETECTION_THRESHOLD", "5"))

# Target site + JSON API base (no literal fallback: fail fast in spider
# with a clear message when unset — AGENTS.md no-hard-coding rule).
# Public routes allowed by robots.txt: /search/, /search/job-detail/.
CRAWLER_TARGET = os.environ.get("CRAWLER_TARGET")
CRAWLER_API_BASE = os.environ.get("CRAWLER_API_BASE")

# Scrapy 2.13+: request fingerprinter defaults to 2.7, no override needed.
FEED_EXPORT_ENCODING = "utf-8"
