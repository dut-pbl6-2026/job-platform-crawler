"""Unit tests for the salary parser (CRAWL-01-02). Inputs are real listing strings."""

import json
from pathlib import Path

import pytest

from crawler.pipelines import _parse_salary

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Plain ranges and single values, with the common unit spellings
        ("5 - 10 trieu", (5_000_000, 10_000_000)),
        ("8 - 12 trieu", (8_000_000, 12_000_000)),
        ("10tr - 15tr", (10_000_000, 15_000_000)),
        ("10 triệu", (10_000_000, 10_000_000)),
        ("5 triệu - 10 triệu", (5_000_000, 10_000_000)),
        ("1.5 - 2.5 tr", (1_500_000, 2_500_000)),
        ("Up to 15 trieu", (None, 15_000_000)),
        # Decimal salaries: the bug was 5.5 triệu -> 55,000,000 (10x)
        ("5.5 triệu", (5_500_000, 5_500_000)),
        ("5,5 triệu", (5_500_000, 5_500_000)),
        ("5.5 - 7.5 triệu", (5_500_000, 7_500_000)),
        ("5,5 - 7,5 triệu", (5_500_000, 7_500_000)),
        ("1,5 - 2 tỷ", (1_500_000_000, 2_000_000_000)),
        # Range words used by the site ("den" = "đến", "to")
        ("20 den 50 trieu", (20_000_000, 50_000_000)),
        ("10 den 20 trieu", (10_000_000, 20_000_000)),
        ("20 đến 50 triệu", (20_000_000, 50_000_000)),
        ("Từ 10 triệu", (10_000_000, None)),
        ("Trên 10 triệu", (10_000_000, None)),
        ("Tới 15 triệu", (None, 15_000_000)),
        ("Lương 12 - 15 triệu/tháng", (12_000_000, 15_000_000)),
        # Full VND amounts: '.' is a thousands separator
        ("15.000.000 - 20.000.000", (15_000_000, 20_000_000)),
        ("15.000.000 VND", (15_000_000, 15_000_000)),
        # Reversed range is normalized
        ("15 - 10 trieu", (10_000_000, 15_000_000)),
        # No value: negotiable, non-VND, blank, unparseable
        ("Thỏa thuận", (None, None)),
        ("Thương lượng", (None, None)),
        ("Negotiable", (None, None)),
        ("Cạnh tranh", (None, None)),
        ("$1,000 - $2,000", (None, None)),
        ("3 - 5 USD", (None, None)),
        ("", (None, None)),
        ("   ", (None, None)),
        (None, (None, None)),
    ],
)
def test_parse_salary_real_strings(raw, expected):
    assert _parse_salary(raw) == expected


def test_parse_salary_has_no_float_rounding_error():
    # float("2.3") * 1_000_000 is 2299999.9999999995 without Decimal
    assert _parse_salary("2.3 triệu") == (2_300_000, 2_300_000)


def test_golden_fixture_salaries():
    """The committed golden sample (captured from the live list API) parses as expected."""
    items = json.loads((FIXTURES / "vieclam_sample.json").read_text(encoding="utf-8"))
    parsed = {item["salary_raw"]: _parse_salary(item["salary_raw"]) for item in items}
    assert parsed == {
        "20 den 50 trieu": (20_000_000, 50_000_000),
        "10 den 20 trieu": (10_000_000, 20_000_000),
        "10 - 20 trieu": (10_000_000, 20_000_000),
    }


def test_seed_records_round_trip():
    """Parsing each seed salary_raw reproduces the stored salary_min/salary_max."""
    seed = json.loads(
        (Path(__file__).parent.parent / "seed" / "jobs.json").read_text(
            encoding="utf-8"
        )
    )
    mismatches = [
        (
            r["salary_raw"],
            _parse_salary(r["salary_raw"]),
            (r["salary_min"], r["salary_max"]),
        )
        for r in seed
        if _parse_salary(r["salary_raw"]) != (r["salary_min"], r["salary_max"])
    ]
    assert mismatches == []
