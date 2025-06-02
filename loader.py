"""
Load a Google-Places CSV into dela.sqlite
Usage:  python loader.py path/to/file.csv
"""
import csv, sys, sqlite3, pathlib, textwrap

DB_PATH = pathlib.Path(__file__).with_name("dela.sqlite")

SCHEMA = textwrap.dedent("""
CREATE TABLE IF NOT EXISTS places (
  place_id TEXT PRIMARY KEY,
  name TEXT, formatted_address TEXT,
  city TEXT, state TEXT, zip_code TEXT,
  lat REAL, lon REAL,
  rating REAL, user_ratings_total INTEGER,
  price_level INTEGER, business_status TEXT,
  local_phone TEXT, intl_phone TEXT,
  website TEXT, photo_ref TEXT,
  distance_miles REAL, source TEXT,
  first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_seen  TIMESTAMP,
  yelp_rating REAL, yelp_reviews INTEGER, yelp_price_tier TEXT,
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
    "Distance Miles": "distance_miles",
    "source": "source"
}

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn

def load(csv_file: pathlib.Path):
    conn = ensure_db()
    cur  = conn.cursor()
    cols = ", ".join(RENAMES.values())
    qs   = ", ".join(["?"] * len(RENAMES))
    insert = f"INSERT OR IGNORE INTO places ({cols}) VALUES ({qs})"
    with csv_file.open(newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            cur.execute(insert, [row.get(k) for k in RENAMES])
    conn.commit()
    print("Rows now in table:",
          cur.execute("SELECT COUNT(*) FROM places").fetchone()[0])
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python loader.py <csv-file>")
    load(pathlib.Path(sys.argv[1]))
