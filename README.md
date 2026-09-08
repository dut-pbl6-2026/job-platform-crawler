# job-platform-crawler

Web crawler for **Vietnam Job Platform** (`pbl6`) — `dut-pbl6-2026`. Extracts job listings from `vieclam.gov.vn`, cleans and deduplicates data, persists to PostgreSQL and syncs to Elasticsearch.

- Tech: Python 3.12, Scrapy 2.11
- Branch flow: `feature/* → main` (see `job-platform-docs/.github/git-strategy.md`)
- Jira: Epic `PBL6-3` (Crawler & Data Seeding, 5 pts, Day Tue–Wed W4)
- TM: TM1 Hoai, TM2 Thanh (Owner), TM3 Chi Bao, TM4 Khoa

## Overview

- `crawler/spiders/vieclam_spider.py` — `VieclamSpider`: pagination crawl from `vieclam.gov.vn`, stops at `MAX_PAGES`
- `crawler/pipelines.py` — `CleaningPipeline` > `DedupPipeline` > `PostgresPipeline` > `ElasticsearchPipeline`
- `crawler/middlewares.py` — `BlockDetectionMiddleware`: detects 403/429 streaks, triggers seed fallback
- `crawler/items.py` — `JobItem` schema
- `crawler/settings.py` — Scrapy settings (throttle, retry, robots.txt, pipelines)
- `scripts/seed_loader.py` — bulk-loads `seed/jobs.json` into PostgreSQL and Elasticsearch
- `scripts/check_connectivity.py` — verifies PostgreSQL and Elasticsearch connections before crawling
- `seed/jobs.json` — static seed dataset (100 records) for offline demo / blocked fallback
- `tests/` — unit tests for spider parsing, pipelines, seed_loader

## Prerequisites

- `mise` https://mise.jdx.dev
- `docker` + `docker compose v2` (for local PostgreSQL and Elasticsearch)
- `git` + `gh` (`gh auth login`)
- Python 3.12 via `mise` — `mise trust && mise install`
- `pip` packages installed via `mise run install`

See `AGENTS.md` for shell activation (`mise activate`) and agent `mise exec` notes.

## Clone

```bash
mkdir -p ~/projects/personal/job-platform && cd ~/projects/personal/job-platform
for r in infra crawler; do gh repo clone dut-pbl6-2026/job-platform-$r; done
cd job-platform-crawler
```

## Setup

```bash
mise trust && mise install
mise run install      # pip install -r requirements.txt
mise run sync-env     # copy .env from ../job-platform-infra/envs/.env.dev.example
mise run check-connectivity
```

Env single source: `../job-platform-infra/envs/.env.dev.example` -> `.env` via `mise run sync-env`.

Required env vars: `DATABASE_URL_CRAWLER`, `ELASTICSEARCH_URL`, `ELASTICSEARCH_INDEX`.

## Run

### Crawl (dev — 10 pages, ~100 jobs, HTTP cache enabled)

```bash
mise run crawl-dev
```

### Crawl (full — 100 pages, ~1000 jobs)

```bash
mise run crawl
```

### Seed fallback (load static seed/jobs.json into PG + ES)

```bash
mise run seed
```

## Lint, Format & Test

```bash
mise run lint       # ruff check crawler/ scripts/ tests/
mise run format     # ruff format --check crawler/ scripts/ tests/
mise run test       # pytest tests/ -v --tb=short
mise run verify     # lint + format + test
```

## Verify data after crawl

```bash
# Count rows in PostgreSQL
psql $DATABASE_URL_CRAWLER -c "SELECT COUNT(*) FROM crawled_jobs;"

# Confirm no duplicates
psql $DATABASE_URL_CRAWLER -c \
  "SELECT COUNT(*) total, COUNT(DISTINCT source_url) unique_urls FROM crawled_jobs;"

# Count indexed documents in Elasticsearch
curl -s "$ELASTICSEARCH_URL/$ELASTICSEARCH_INDEX/_count"
```

## Troubleshooting

- `python: command not found` -> `mise trust && mise install`
- `PostgreSQL connection refused` -> start infra: `cd ../job-platform-infra && docker compose up -d`
- `Elasticsearch connection refused` -> same docker compose, check `ELASTICSEARCH_URL` in `.env`
- 403/429 rate-limiting from `vieclam.gov.vn` -> spider falls back to `mise run seed` automatically, or run manually
- `mise run verify` fails -> re-run `mise run sync-env`

`feature/* -> main` (see `job-platform-docs/.github/git-strategy.md`).
