# Test fixtures (intentional, committed)

- `vieclam_sample.json` — 3 golden `JobItem` samples captured from the live
  JSON API on 2026-09-08 (`feature/crawler-setup` manual run: `MAX_PAGES=2`,
  `PAGE_SIZE=20`, `nhom_tin_tuyen_dung=4` → 40 items, 100% title/company/
  location/category, 14 without salary, no description/requirements from the
  list API). Used for eyeball verification of the frontend-mapper parity
  (`vitri_td`→title, `ten_ct`→company, `ten_tinh1(+2)`→location,
  `muc_luong`→salary_raw, `nganh_nghe`→category).

Live crawl dumps (`-o output/*.json`, `*.log`) are NEVER committed —
they grow unbounded and contain real third-party data. Reproduce with:

```bash
scrapy crawl vieclam -o output/live.json
```
