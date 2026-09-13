# AGENTS — job-platform-crawler

> Web crawler — Python Scrapy. SRS: `job-platform-docs/docs/master-plan.vi.md:195-196`, `docs/srs/vi/3-must-have-fr.vi.md:CRAWL-01-01..07`. Git: `job-platform-docs/.github/git-strategy.md` (`feature/* -> main`).

## Mise activation

Activate `mise` for bare `python`/`scrapy` without `mise exec`:

| Shell | Add to config file | Activate |
|-------|--------------------|----------|
| `bash` | `~/.bashrc` or `~/.bash_profile` | `eval "$(mise activate bash)"` |
| `zsh` | `~/.zshrc` | `eval "$(mise activate zsh)"` |
| `fish` | `~/.config/fish/config.fish` | `mise activate fish \| source` |
| `PowerShell` | `$PROFILE` | `mise activate pwsh \| Out-String \| Invoke-Expression` |

Agent uses `mise exec -- python ...` / `mise exec -- scrapy ...` due to non-interactive shell without `mise activate`; humans just use `python` / `scrapy` after `mise install`.

## Scope

`PBL6-3` MUST `CRAWL-01` — Extract job listings from `vieclam.gov.vn`, clean and normalize data, deduplicate by `source_url`, persist to PostgreSQL (`crawled_jobs`), sync to Elasticsearch (`jobs` index). Owner TM2 W4 (Sprint Schedule: Day Tue–Wed W4). DB `job_platform_crawler`.

- Day Tue milestone: `COUNT(*) FROM crawled_jobs >= 100` (10 pages, dev mode)
- Day Wed milestone: `COUNT(*) FROM crawled_jobs >= 500` (50 pages, scale-up, dedup)
- Full crawl: `COUNT(*) FROM crawled_jobs >= 1000` (100 pages)

## Architecture — Scrapy pipeline chain

```
crawler/spiders/vieclam_spider.py    -> VieclamSpider: start_urls, parse (pagination), parse_job
crawler/items.py                     -> JobItem (source_url, title, company, location, salary_raw, description, requirements, category)
crawler/pipelines.py                 -> CleaningPipeline (100) -> DedupPipeline (200) -> PostgresPipeline (300) -> ElasticsearchPipeline (400)
crawler/middlewares.py               -> BlockDetectionMiddleware (consecutive 403/429 > 5 -> seed fallback)
crawler/settings.py                  -> BOT_NAME, DOWNLOAD_DELAY, AUTOTHROTTLE, ROBOTSTXT_OBEY, ITEM_PIPELINES, RETRY_*
crawler/extensions.py                -> CrawlStatsExtension (summary stats at spider close)
scripts/seed_loader.py               -> bulk insert seed/jobs.json -> PG + ES
scripts/generate_seed.py             -> expand seed/jobs.json to 550 records (one-time, deterministic)
scripts/verify_500.py                -> verify 500+ dedup milestone in PG + ES
scripts/check_connectivity.py        -> verify PG & ES before crawling
seed/jobs.json                       -> 550 static records for fallback and demo
tests/                               -> pytest unit tests (spider, pipelines, seed_loader, extensions, verify_500)
```

## SRS mapping (CRAWL-01)

- `CRAWL-01-01` — Extract `title`, `company`, `location`, `salary_raw`, `description`, `requirements`, `category` from `vieclam.gov.vn`. Respect `robots.txt`, `DOWNLOAD_DELAY=2`, `AUTOTHROTTLE_ENABLED=True`. User-Agent must self-identify. Stop crawl at `MAX_PAGES`.
- `CRAWL-01-02` — `CleaningPipeline`: strip HTML tags from `description`/`requirements`, normalize location names (`HCM` -> `Ho Chi Minh`, `HN` -> `Ha Noi`), parse `salary_raw` into `salary_min`/`salary_max` (VND integers), handle missing fields with `None` (no exception).
- `CRAWL-01-03` — `DedupPipeline`: in-memory `seen_urls` set per run (raise `DropItem` on duplicate). DB-level UNIQUE constraint on `source_url` as safety net across runs.
- `CRAWL-01-04` — `PostgresPipeline`: `CREATE TABLE IF NOT EXISTS crawled_jobs`, upsert via `ON CONFLICT (source_url) DO UPDATE SET title = EXCLUDED.title, updated_at = NOW()`. Auto-creates table on spider open. Batch commit every 50 items (Day Wed scale-up).
- `CRAWL-01-05` — `ElasticsearchPipeline`: index document to `jobs` index after successful PG upsert. ES failure logs warning but does NOT abort the crawl (best-effort sync). Bulk indexing via `helpers.bulk` every 50 docs (Day Wed scale-up).
- `CRAWL-01-06` — Retry 3 times with exponential backoff on network errors. Log failures with `[WARN]` including URL, HTTP status, and HTML snippet. Continue crawl after individual item failure.
- `CRAWL-01-07` — `BlockDetectionMiddleware` counts consecutive 403/429 responses. If `consecutive_failures > 5`: log `[WARN] blocked, activating seed fallback`, call `seed_loader.load_seed()`, raise `CloseSpider("blocked_fallback")`. Seed data loaded into PG + ES for uninterrupted demo.

## Data schema

### PostgreSQL — `crawled_jobs`

```sql
CREATE TABLE IF NOT EXISTS crawled_jobs (
    id              SERIAL PRIMARY KEY,
    source_url      TEXT         NOT NULL UNIQUE,
    title           VARCHAR(256) NOT NULL,
    company         VARCHAR(256),
    location        VARCHAR(256),
    salary_raw      VARCHAR(256),
    salary_min      BIGINT,
    salary_max      BIGINT,
    salary_currency VARCHAR(10)  DEFAULT 'VND',
    description     TEXT,
    requirements    TEXT,
    category        VARCHAR(128),
    crawled_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);
```

### Elasticsearch — index `jobs`

Fields: `id`, `title`, `company`, `location`, `salary_min`, `salary_max`, `description`, `requirements`, `category`, `source_url`, `crawled_at`.

## Scrapy settings reference

| Setting | Value | Reason |
|:--------|:------|:-------|
| `DOWNLOAD_DELAY` | `2` | Base 2s delay between requests (CRAWL-01-01: 1-3s) |
| `AUTOTHROTTLE_ENABLED` | `True` | Adaptive delay based on server response time |
| `AUTOTHROTTLE_TARGET_CONCURRENCY` | `1.0` | Max 1 concurrent request |
| `ROBOTSTXT_OBEY` | `True` | Respect robots.txt (CRAWL-01-01) |
| `USER_AGENT` | `PBL6-JobPlatform-Crawler/1.0 (+https://github.com/dut-pbl6-2026)` | Self-identifying User-Agent |
| `RETRY_TIMES` | `3` | Retry on network/server errors (CRAWL-01-06) |
| `RETRY_HTTP_CODES` | `[500, 502, 503, 504, 408, 429]` | HTTP codes triggering retry |
| `HTTPCACHE_ENABLED` | `True` (dev only) | Cache responses in dev to avoid re-crawling |
| `MAX_PAGES` | `10` (dev) / `50` (500+) / `100` (full) | Page limit read from env |

## No hard-coding (STRICT — apply to every file you touch)

**NEVER** embed literal values for any of the following in source code (`.py`, `.toml`, `.yaml`, ...):

| Category | Examples of forbidden literals |
|----------|-------------------------------|
| Connection strings | `postgresql://user:pass@localhost:5432/db` |
| URLs / hostnames | `http://localhost:9200` |
| Secrets / passwords | plain-text credentials |
| Database names | `job_platform_crawler` (except in migration scripts) |

**Always** read from environment variables via `os.environ` or `python-dotenv`:

```python
import os
DATABASE_URL = os.environ.get("DATABASE_URL_CRAWLER")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL_CRAWLER is not set.")
```

- `.env` is git-ignored. Never commit credentials.
- Single source of truth: `../job-platform-infra/envs/.env.dev.example` — sync via `mise run sync-env`.
- Required env vars: `DATABASE_URL_CRAWLER`, `ELASTICSEARCH_URL`, `ELASTICSEARCH_INDEX`, `MAX_PAGES`.

## Logging conventions

- Use Python `logging` module. Logger name: `crawler.<module>` (e.g., `crawler.pipelines`, `crawler.spiders.vieclam`).
- Log levels: `DEBUG` for per-item tracing, `INFO` for milestones (spider open/close, item counts), `WARNING` for retries / block detection / ES failures, `ERROR` for unrecoverable failures.
- **No emoji or icons in log messages.** Plain ASCII text only.
- Format: `[LEVEL] <logger_name>: <message>` — Scrapy's default log formatter handles this.

Examples:
```
[INFO] crawler.spiders.vieclam: Spider opened. MAX_PAGES=10
[INFO] crawler.pipelines: PostgresPipeline opened. Table crawled_jobs ready.
[WARNING] crawler.middlewares: Consecutive failures: 3/5. URL: https://vieclam.gov.vn/...
[WARNING] crawler.middlewares: Consecutive failures exceeded 5. Activating seed fallback.
[INFO] crawler.scripts.seed_loader: Loaded 550 seed records into PostgreSQL.
[INFO] crawler.scripts.seed_loader: Indexed 550 seed documents into Elasticsearch.
[WARNING] crawler.pipelines: Elasticsearch unavailable. Skipping index for item: <title>
[ERROR] crawler.pipelines: PostgreSQL upsert failed. source_url=... Error: ...
```

## 2026 best practice

- Python 3.12, Scrapy 2.11, type hints on all public functions.
- `ruff check` + `ruff format --check` zero warnings enforced via `mise run verify`.
- `pytest` coverage > 70% (`MAINT-02`).
- Never commit `.env` (`.gitignore`).
- `requirements.txt` with pinned minor versions: `scrapy==2.11.*`, `psycopg2-binary==2.9.*`, `elasticsearch==8.*`, `python-dotenv==1.*`, `ruff==0.4.*`, `pytest==8.*`.

## Workflow

```bash
mise trust && mise install
mise run sync-env
mise run install             # pip install -r requirements.txt
mise run check-connectivity  # verify PG & ES connections
mise run crawl-dev           # 10 pages, ~100 jobs, HTTP cache on
mise run crawl-500           # 50 pages, ~500+ jobs (Day Wed milestone)
mise run verify-500          # verify 500+ dedup in PG + ES
mise run verify              # lint + format + test before PR
```

## Git convention (git-strategy.md)

Branch: `feature/<description>` | `bugfix/<description>` | `hotfix/v<semver>-<desc>` -> `main`.

Commits — `<type>(crawler): <subject>` (scope always `crawler` for this repo):

| Type | Example |
|------|---------|
| `feat` | `feat(crawler): add VieclamSpider with pagination` |
| `fix` | `fix(crawler): handle missing salary field in CleaningPipeline` |
| `refactor` | `refactor(crawler): extract salary parser to util module` |
| `test` | `test(crawler): add unit tests for DedupPipeline` |
| `docs` | `docs(crawler): update AGENTS.md with logging conventions` |
| `chore` | `chore(crawler): pin scrapy version in requirements.txt` |
| `data` | `data(crawler): add seed/jobs.json with 100 records` |
| `ci` | `ci(crawler): configure GitHub Actions workflow` |

PR checklist: Description / How to verify / Checklist `mise run verify` + DB count check + ES count check.
