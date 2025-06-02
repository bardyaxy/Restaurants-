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

from utils import setup_logging

DB_PATH = pathlib.Path(__file__).with_name("dela.sqlite")

SCHEMA = textwrap.dedent("""
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
  yelp_status TEXT
);
""")

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
    "source": "source"
}

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def ensure_db() -> sqlite3.Connection:
    """Create dela.sqlite and the places table if they don’t exist yet."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(places)")
    cols = {row[1] for row in cur.fetchall()}
    if not cols:
        conn.executescript(SCHEMA)
    else:
        if "categories" not in cols:
            cur.execute("ALTER TABLE places ADD COLUMN categories TEXT")
        if "category" not in cols:
            cur.execute("ALTER TABLE places ADD COLUMN category TEXT")
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


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load a Google-Places CSV into dela.sqlite")
    parser.add_argument("csv", help="Path to CSV produced by refresh_restaurants.py")
    args = parser.parse_args()

    csv_path = pathlib.Path(args.csv).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    setup_logging()
    load(csv_path)
