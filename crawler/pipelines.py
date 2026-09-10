"""Scrapy item pipelines for job-platform-crawler (CRAWL-01-02 to CRAWL-01-05).

Pipeline chain (priorities defined in settings.py):
  CleaningPipeline (100)      -> strip HTML, normalize salary/location
  DedupPipeline    (200)      -> in-memory dedup per run by source_url
  PostgresPipeline (300)      -> upsert into crawled_jobs, returns pg_id
  ElasticsearchPipeline (400) -> index doc into ES jobs index (best-effort)
"""

import html
import logging
import re
from html.parser import HTMLParser

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

logger = logging.getLogger("crawler.pipelines")

# ---------------------------------------------------------------------------
# Helper: strip HTML tags
# ---------------------------------------------------------------------------


class _MLStripper(HTMLParser):
    """Simple HTML-tag stripper using stdlib html.parser (no deps)."""

    def __init__(self) -> None:
        super().__init__()
        self.reset()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_data(self) -> str:
        return " ".join(self._parts)


def _strip_html(raw: str | None) -> str | None:
    """Return plain text from *raw* HTML string; None if blank/None."""
    if not raw:
        return None
    s = _MLStripper()
    s.feed(html.unescape(raw))
    text = s.get_data().strip()
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text or None


# ---------------------------------------------------------------------------
# Helper: salary parser
# ---------------------------------------------------------------------------

# Matches patterns like "5 - 10 trieu", "10tr - 15tr", "15.000.000 - 20.000.000"
_SALARY_RE = re.compile(
    r"([\d.,]+)\s*[-\u2013\u2014]\s*([\d.,]+)\s*(trieu|tri\u1ec7u|tri\u1ec7u \u0111\u1ed3ng|tr|million)?",
    re.IGNORECASE,
)

# Also match single value "10 trieu"
_SALARY_SINGLE_RE = re.compile(
    r"([\d.,]+)\s*(trieu|tri\u1ec7u|tri\u1ec7u \u0111\u1ed3ng|tr|million)?",
    re.IGNORECASE,
)

_MILLION = 1_000_000


def _parse_salary(salary_raw: str | None) -> tuple[int | None, int | None]:
    """Parse *salary_raw* into (salary_min, salary_max) VND integers.

    Returns (None, None) when salary_raw is blank or unparseable.
    Converts trieu/tr/million unit to VND by multiplying by 1_000_000.
    """
    if not salary_raw:
        return None, None

    cleaned = salary_raw.strip().replace(",", ".").replace("\xa0", " ")

    m = _SALARY_RE.search(cleaned)
    if m:
        low_str, high_str, unit = m.group(1), m.group(2), m.group(3)
        low = _to_int(low_str)
        high = _to_int(high_str)
        if unit:
            low = low * _MILLION if low is not None else None
            high = high * _MILLION if high is not None else None
        elif low is not None and low < 1_000:
            # Heuristic: bare number < 1000 treated as millions
            low = low * _MILLION
            high = high * _MILLION if high is not None else None
        return low, high

    # Single value (e.g. "10 trieu")
    m2 = _SALARY_SINGLE_RE.search(cleaned)
    if m2:
        val_str, unit = m2.group(1), m2.group(2)
        val = _to_int(val_str)
        if unit:
            val = val * _MILLION if val is not None else None
        elif val is not None and val < 1_000:
            val = val * _MILLION
        return val, val

    return None, None


def _to_int(s: str) -> int | None:
    """Convert a numeric string (may contain dots as thousand-separators) to int."""
    if not s:
        return None
    try:
        return int(float(s.replace(".", "").replace(",", ".")))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Helper: location normalizer
# ---------------------------------------------------------------------------

_LOCATION_MAP: dict[str, str] = {
    "hcm": "Ho Chi Minh",
    "tp.hcm": "Ho Chi Minh",
    "tp hcm": "Ho Chi Minh",
    "ho chi minh": "Ho Chi Minh",
    "hn": "Ha Noi",
    "ha noi": "Ha Noi",
    "hanoi": "Ha Noi",
    "dn": "Da Nang",
    "da nang": "Da Nang",
    "danang": "Da Nang",
    "ct": "Can Tho",
    "can tho": "Can Tho",
    "hp": "Hai Phong",
    "hai phong": "Hai Phong",
    "hue": "Hue",
    "thua thien hue": "Hue",
}


def _normalize_location(loc: str | None) -> str | None:
    """Normalize common Vietnamese location abbreviations to full names."""
    if not loc:
        return None
    stripped = re.sub(r"\s+", " ", loc.strip())
    key = stripped.lower()
    return _LOCATION_MAP.get(key, stripped) or None


# ---------------------------------------------------------------------------
# CleaningPipeline (priority 100)  -- CRAWL-01-02
# ---------------------------------------------------------------------------


class CleaningPipeline:
    """Strip HTML, normalize location and salary fields (CRAWL-01-02)."""

    def process_item(self, item: dict, spider) -> dict:  # type: ignore[override]
        adapter = ItemAdapter(item)

        # Strip HTML from text fields
        for field in ("description", "requirements"):
            raw = adapter.get(field)
            adapter[field] = _strip_html(raw)

        # Strip whitespace from all string fields
        for field in ("title", "company", "location", "salary_raw", "category"):
            val = adapter.get(field)
            if isinstance(val, str):
                adapter[field] = val.strip() or None

        # Normalize location
        adapter["location"] = _normalize_location(adapter.get("location"))

        # Parse salary
        salary_min, salary_max = _parse_salary(adapter.get("salary_raw"))
        adapter["salary_min"] = salary_min
        adapter["salary_max"] = salary_max
        adapter["salary_currency"] = "VND"

        return item


# ---------------------------------------------------------------------------
# DedupPipeline (priority 200)  -- CRAWL-01-03
# ---------------------------------------------------------------------------


class DedupPipeline:
    """In-memory dedup by source_url per spider run (CRAWL-01-03).

    DB-level UNIQUE constraint on source_url is the cross-run safety net.
    """

    def open_spider(self, spider) -> None:  # type: ignore[override]
        self.seen_urls: set[str] = set()

    def process_item(self, item: dict, spider) -> dict:  # type: ignore[override]
        adapter = ItemAdapter(item)
        url: str = adapter.get("source_url") or ""
        if not url:
            raise DropItem("Missing source_url -- cannot dedup.")
        if url in self.seen_urls:
            raise DropItem(f"Duplicate URL: {url}")
        self.seen_urls.add(url)
        return item
