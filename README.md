# job-platform-crawler
Python Scrapy — part of **Vietnam Job Platform** (`pbl6`) under [`dut-pbl6-2026`](https://github.com/dut-pbl6-2026).
- Tech: Python Scrapy
- Branch flow: `feature/* → main`
- Jira PBL6 skid.atlassian.net

## Spider: `vieclam` (CRAWL-01-01)

Primary source is the public JSON API (no auth), HTML `/search/` routes are
robots-allowed fallbacks (client-side rendered — selectors are placeholders).

Field mapping mirrors the site frontend mapper: `vitri_td`→title,
`ten_ct`→company, `ten_tinh1(+2)`→location, `muc_luong`→salary_raw,
`nganh_nghe`→category,
`source_url` = `https://vieclam.gov.vn/search/job-detail?id={id}`.
The list API carries no description/requirements (stay `None`, CRAWL-01-02).

## Run

```bash
cp ../job-platform-infra/envs/.env.dev.example .env  # or copy .env.example
scrapy crawl vieclam -o output/live.json
# knobs: MAX_PAGES=2 PAGE_SIZE=20 NHOM_TIN_TUYEN_DUNG=4 scrapy crawl vieclam -o output/live.json
```

`output/`, `*.log`, `*.jl` are git-ignored — **never commit live dumps**,
reproduce with the command above. The only committed sample is the golden
fixture `tests/fixtures/vieclam_sample.json` (see `tests/fixtures/README.md`).

## Contracts (`scrapy check`)

`parse` / `parse_job` carry `@url` / `@returns` / `@scrapes` contracts
(HTML GET routes). `parse_api` is intentionally contract-free: the source
API is POST-only and built-in contracts can only issue GET (404) — it is
covered by live runs instead. Pipelines land in PR2, so disable them for
the check:

```bash
scrapy check vieclam -s "ITEM_PIPELINES={}"
```

Needs `CRAWLER_TARGET` + `CRAWLER_API_BASE` (via `.env` or environment).
