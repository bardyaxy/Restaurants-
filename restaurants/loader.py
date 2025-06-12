"""
Load a Google-Places CSV into dela.sqlite

Usage:
    python loader.py path/to/file.csv
"""

import csv
import argparse
import sqlite3
import pathlib
import textwrap
import logging
import json
from datetime import datetime, timezone

try:
    from restaurants.utils import setup_logging
except ImportError:  # pragma: no cover - fallback for running as script
    from utils import setup_logging  # type: ignore

DB_PATH = pathlib.Path(__file__).with_name("dela.sqlite")

SCHEMA = textwrap.dedent(
    """
CREATE TABLE IF NOT EXISTS places (
  place_id TEXT PRIMARY KEY,
  name TEXT,
  formatted_address TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  lat REAL,
  lon REAL,
  rating REAL,
  user_ratings_total INTEGER,
  price_level INTEGER,
  business_status TEXT,
  local_phone TEXT,
  intl_phone TEXT,
  website TEXT,
  photo_ref TEXT,
  categories TEXT,
  category TEXT,
  distance_miles REAL,
  source TEXT,
  first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_seen  TIMESTAMP,
  yelp_rating REAL,
  yelp_reviews INTEGER,
  yelp_price_tier TEXT,
  yelp_status TEXT,
  yelp_cuisines TEXT,
  yelp_primary_cuisine TEXT,
  yelp_category_titles TEXT,
  facebook_url TEXT,
  instagram_url TEXT,
  gpv_projection REAL
);
"""
)

RENAMES = {
    "Place ID": "place_id",
    "Name": "name",
    "Formatted Address": "formatted_address",
    "City": "city",
    "State": "state",
    "Zip Code": "zip_code",
    "lat": "lat",
    "lon": "lon",
    "Rating": "rating",
    "User Ratings Total": "user_ratings_total",
    "Price Level": "price_level",
    "Business Status": "business_status",
    "Formatted Phone Number": "local_phone",
    "International Phone Number": "intl_phone",
    "Website": "website",
    "Photo Reference": "photo_ref",
    "Types": "categories",
    "Category": "category",
    "Distance Miles": "distance_miles",
    "source": "source",
    "facebook_url": "facebook_url",
    "instagram_url": "instagram_url",
    "GPV Projection": "gpv_projection",
}

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def ensure_db() -> sqlite3.Connection:
    """Create dela.sqlite and the places table if they don’t exist yet."""
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(places)")
    cols = {row[1] for row in cur.fetchall()}
    if "categories" not in cols:
        cur.execute("ALTER TABLE places ADD COLUMN categories TEXT")
    if "category" not in cols:
        cur.execute("ALTER TABLE places ADD COLUMN category TEXT")
    if "yelp_cuisines" not in cols:
        cur.execute("ALTER TABLE places ADD COLUMN yelp_cuisines TEXT")
    if "yelp_primary_cuisine" not in cols:
        cur.execute("ALTER TABLE places ADD COLUMN yelp_primary_cuisine TEXT")
    if "yelp_category_titles" not in cols:
        cur.execute("ALTER TABLE places ADD COLUMN yelp_category_titles TEXT")
    if "facebook_url" not in cols:
        cur.execute("ALTER TABLE places ADD COLUMN facebook_url TEXT")
    if "instagram_url" not in cols:
        cur.execute("ALTER TABLE places ADD COLUMN instagram_url TEXT")
    if "gpv_projection" not in cols:
        cur.execute("ALTER TABLE places ADD COLUMN gpv_projection REAL")
    conn.commit()
    return conn


def load(csv_file: pathlib.Path) -> None:
    """Insert rows from the CSV into the places table (dedup on place_id)."""
    conn = ensure_db()
    cur = conn.cursor()

    cols = ", ".join(RENAMES.values())
    qs = ", ".join(["?"] * len(RENAMES))
    insert_sql = f"INSERT OR IGNORE INTO places ({cols}) VALUES ({qs})"

    with csv_file.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cur.execute(insert_sql, [row.get(k) for k in RENAMES])

    conn.commit()
    total = cur.execute("SELECT COUNT(*) FROM places").fetchone()[0]
    logging.info("CSV loaded. Rows now in table: %s", total)
    conn.close()


def load_yelp_json(json_file: pathlib.Path) -> None:
    """Insert Yelp-fetch JSON rows into the places table."""
    conn = ensure_db()
    cur = conn.cursor()

    cols = [
        "place_id",
        "name",
        "formatted_address",
        "city",
        "state",
        "zip_code",
        "lat",
        "lon",
        "local_phone",
        "website",
        "yelp_rating",
        "yelp_reviews",
        "yelp_price_tier",
        "yelp_cuisines",
        "yelp_primary_cuisine",
        "yelp_category_titles",
        "source",
        "last_seen",
    ]

    insert_sql = (
        f"INSERT OR IGNORE INTO places ({', '.join(cols)}) "
        f"VALUES ({', '.join(['?'] * len(cols))})"
    )

    with json_file.open(encoding="utf-8") as f:
        data = json.load(f)

    now = datetime.now(timezone.utc).isoformat()

    for item in data:
        business = item.get("business") or {}
        details = item.get("details") or {}
        info = details or business

        location = info.get("location") or business.get("location") or {}
        coords = info.get("coordinates") or business.get("coordinates") or {}
        categories = info.get("categories") or business.get("categories") or []
        aliases = [c.get("alias") for c in categories if c and c.get("alias")]
        titles = [c.get("title") for c in categories if c and c.get("title")]

        addr_parts = [
            location.get("address1"),
            location.get("city"),
            location.get("state"),
            location.get("zip_code"),
        ]
        formatted_address = ", ".join([p for p in addr_parts if p]) or None

        row = {
            "place_id": business.get("id") or details.get("id"),
            "name": business.get("name") or details.get("name"),
            "formatted_address": formatted_address,
            "city": location.get("city"),
            "state": location.get("state"),
            "zip_code": location.get("zip_code"),
            "lat": coords.get("latitude"),
            "lon": coords.get("longitude"),
            "local_phone": info.get("display_phone") or info.get("phone"),
            "website": info.get("url"),
            "yelp_rating": business.get("rating"),
            "yelp_reviews": business.get("review_count"),
            "yelp_price_tier": business.get("price"),
            "yelp_cuisines": ",".join(aliases) if aliases else None,
            "yelp_primary_cuisine": aliases[0] if aliases else None,
            "yelp_category_titles": ",".join(titles) if titles else None,
            "source": "yelp_fetch",
            "last_seen": now,
        }

        cur.execute(insert_sql, [row.get(c) for c in cols])

    conn.commit()
    logging.info(
        "Yelp JSON loaded. Rows now in table: %s",
        cur.execute("SELECT COUNT(*) FROM places").fetchone()[0],
    )
    conn.close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load a Google-Places CSV into dela.sqlite"
    )
    parser.add_argument("csv", help="Path to CSV produced by refresh_restaurants.py")
    args = parser.parse_args()

    csv_path = pathlib.Path(args.csv).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    setup_logging()
    load(csv_path)
