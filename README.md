# job-platform-crawler

Web crawler for **Vietnam Job Platform** (`pbl6`) — `dut-pbl6-2026`.
Extracts job listings from `vieclam.gov.vn` (CRAWL-01-01).

- Tech: Python 3.14, Scrapy 2.13
- Branch flow: `feature/* → main` (see `job-platform-docs/.github/git-strategy.md`)
- Jira: Epic `PBL6-3` (Crawler & Data Seeding)

## Spider: `vieclam` (CRAWL-01-01)

Primary source is the public JSON API (no auth), HTML `/search/` routes are
robots-allowed fallbacks (client-side rendered — selectors are placeholders).

Field mapping mirrors the site frontend mapper: `vitri_td`→title,
`ten_ct`→company, `ten_tinh1(+2)`→location, `muc_luong`→salary_raw,
`nganh_nghe`→category,
`source_url` = `https://vieclam.gov.vn/search/job-detail?id={id}`.
The list API carries no description/requirements (stay `None`, CRAWL-01-02).

## Setup

```bash
python -m venv .venv && .venv/Scripts/activate  # Windows
pip install -r requirements.txt
copy .env.example .env  # then fill values; never commit .env
```

Single source of truth for env values:
`../job-platform-infra/envs/.env.dev.example`.
Required: `CRAWLER_TARGET`, `CRAWLER_API_BASE`
(`DATABASE_URL_CRAWLER` / `ELASTICSEARCH_*` land with PR2 pipelines).

## Run

```bash
scrapy crawl vieclam -o output/live.json
# knobs: MAX_PAGES=2 PAGE_SIZE=20 NHOM_TIN_TUYEN_DUNG=4 scrapy crawl vieclam -o output/live.json
```

`output/`, `*.log`, `*.jl` are git-ignored — **never commit live dumps**,
reproduce with the command above. The only committed sample is the golden
fixture `tests/fixtures/vieclam_sample.json` (see `tests/fixtures/README.md`).

## Contracts (`scrapy check`)

`parse` / `parse_job` carry `@url` / `@returns` / `@scrapes` contracts
(HTML GET routes). `parse_api` is intentionally contract-free: the source
API is POST-only and built-in contracts can only issue GET (verified 404) —
it is covered by live runs instead. Pipelines land in PR2, so disable them
for the check:

```bash
scrapy check vieclam -s "ITEM_PIPELINES={}"
```

Needs `CRAWLER_TARGET` + `CRAWLER_API_BASE` (via `.env` or environment).

## Lint & Test

```bash
ruff check crawler/ tests/
pytest tests/ -v --tb=short   # from PR2 (pipelines) onwards
```