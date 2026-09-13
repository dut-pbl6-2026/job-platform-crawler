"""generate_seed.py — Expand seed/jobs.json from 100 to 550 records.

One-time generator (Day Wed scale-up). Keeps existing 100 records,
appends deterministic records 101..550 with diverse locations,
categories, and salary ranges.

Usage:
    python scripts/generate_seed.py
    python scripts/generate_seed.py --count 550 --seed 42
"""

import argparse
import json
import logging
import random
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("crawler.scripts.generate_seed")

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_SEED_PATH = str(_REPO_ROOT / "seed" / "jobs.json")

LOCATIONS = [
    "Ha Noi",
    "Da Nang",
    "Ho Chi Minh",
    "Hue",
    "Can Tho",
    "Hai Phong",
    "Binh Duong",
    "Dong Nai",
    "Bac Ninh",
    "Quang Ninh",
    "Nghe An",
    "Thanh Hoa",
    "Khanh Hoa",
    "Lam Dong",
    "Thua Thien Hue",
    "Hai Duong",
    "Nam Dinh",
    "Binh Dinh",
    "Quang Nam",
    "Long An",
]

CATEGORIES = [
    "CNTT",
    "Ke toan",
    "Ban hang",
    "Ky thuat",
    "Marketing",
    "Nhan su",
    "Giao duc",
    "Y te",
    "Xay dung",
    "Logistics",
    "Du lich",
    "Nong nghiep",
    "Tai chinh",
    "Luat",
    "Truyen thong",
    "Van tai",
]

TITLES_BY_CATEGORY = {
    "CNTT": [
        "Ky su phan mem",
        "Lap trinh vien Python",
        "DevOps Engineer",
        "Kiem thu phan mem",
    ],
    "Ke toan": ["Nhan vien ke toan", "Ke toan truong", "Ke toan tong hop"],
    "Ban hang": ["Nhan vien kinh doanh", "Truong nhom ban hang", "Nhan vien tu van"],
    "Ky thuat": ["Ky su co khi", "Ky su dien", "Ky thuat vien bao tri"],
    "Marketing": ["Nhan vien marketing", "Chuyen vien SEO", "Nhan vien content"],
    "Nhan su": ["Chuyen vien tuyen dung", "Nhan vien hanh chinh nhan su"],
    "Giao duc": ["Giao vien tieng Anh", "Tro giang", "Nhan vien dao tao"],
    "Y te": ["Dieu duong vien", "Duoc si ban le", "Ky thuat vien xet nghiem"],
    "Xay dung": ["Ky su xay dung", "Giam sat cong trinh", "Nhan vien du toan"],
    "Logistics": ["Nhan vien xuat nhap khau", "Nhan vien dieu phoi giao hang"],
    "Du lich": ["Huong dan vien du lich", "Nhan vien dat tour"],
    "Nong nghiep": ["Ky su nong nghiep", "Nhan vien ky thuat trang trai"],
    "Tai chinh": ["Chuyen vien tin dung", "Nhan vien phan tich tai chinh"],
    "Luat": ["Chuyen vien phap che", "Tro ly luat su"],
    "Truyen thong": ["Phong vien", "Bien tap vien noi dung"],
    "Van tai": ["Nhan vien kho", "Tai xe giao hang", "Dieu phoi van tai"],
}

COMPANIES = [
    "Cong ty TNHH ABC",
    "Cong ty CP XYZ",
    "Tap doan DEF",
    "Cong ty TNHH Foogo Viet Nam",
    "Cong ty CP Sai Gon Tech",
    "Tap doan Hoa Phat Mien Trung",
    "Cong ty TNHH Minh Duc",
    "Cong ty CP Dau tu Hoang Long",
    "Tap doan An Phat",
    "Cong ty TNHH Thanh Dat",
    "Cong ty CP Viet Tien",
    "Tap doan Song Hong",
]

SALARY_RANGES = [
    (3_000_000, 5_000_000),
    (5_000_000, 10_000_000),
    (8_000_000, 15_000_000),
    (15_000_000, 25_000_000),
    (25_000_000, 40_000_000),
    (40_000_000, 70_000_000),
]


def _fmt_salary(salary_min: int, salary_max: int) -> str:
    """Format salary pair as '<min> - <max> trieu' with trimmed millions."""
    lo = salary_min // 1_000_000
    hi = salary_max // 1_000_000
    return f"{lo} - {hi} trieu"


def generate_record(index: int, rng: random.Random) -> dict:
    """Generate one deterministic seed record with unique source_url."""
    category = rng.choice(CATEGORIES)
    title = rng.choice(TITLES_BY_CATEGORY[category])
    company = rng.choice(COMPANIES)
    location = rng.choice(LOCATIONS)
    salary_min, salary_max = rng.choice(SALARY_RANGES)
    return {
        "source_url": f"https://vieclam.gov.vn/seed/{index}",
        "title": title,
        "company": company,
        "location": location,
        "salary_raw": _fmt_salary(salary_min, salary_max),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": "VND",
        "description": (
            f"Mo ta cong viec {title} tai {company}, lam viec tai {location}."
        ),
        "requirements": (f"Yeu cau kinh nghiem, ky nang phu hop vi tri {title}."),
        "category": category,
    }


def expand_seed(
    seed_path: str = _DEFAULT_SEED_PATH,
    count: int = 550,
    seed: int = 42,
) -> int:
    """Expand seed file to *count* records. Returns final record count."""
    seed_file = Path(seed_path)
    if not seed_file.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_path}")

    with open(seed_file, encoding="utf-8") as fh:
        records: list[dict] = json.load(fh)

    existing_urls = {r.get("source_url") for r in records}
    rng = random.Random(seed)
    next_index = len(records) + 1
    while len(records) < count:
        record = generate_record(next_index, rng)
        if record["source_url"] in existing_urls:
            next_index += 1
            continue
        records.append(record)
        existing_urls.add(record["source_url"])
        next_index += 1

    with open(seed_file, "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)
        fh.write("\n")

    logger.info("Seed file expanded to %d records: %s", len(records), seed_path)
    return len(records)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Expand seed/jobs.json to 500+ records."
    )
    parser.add_argument("--count", type=int, default=550)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--path", type=str, default=_DEFAULT_SEED_PATH)
    args = parser.parse_args()
    expand_seed(seed_path=args.path, count=args.count, seed=args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
