import csv
import sqlite3
import json
from restaurants import loader


def test_loader_inserts_category(tmp_path, monkeypatch):
    csv_path = tmp_path / "sample.csv"
    fields = list(loader.RENAMES.keys())
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        row = {field: "x" for field in fields}
        row["Place ID"] = "pid1"
        row["Types"] = "bar,cafe"
        row["Category"] = "bar"
        writer.writerow(row)

    tmp_db = tmp_path / "dela.sqlite"
    monkeypatch.setattr(loader, "DB_PATH", tmp_db)
    loader.load(csv_path)

    conn = sqlite3.connect(tmp_db)
    res = conn.execute("SELECT categories, category FROM places").fetchone()
    conn.close()
    assert res == ("bar,cafe", "bar")


def test_ensure_db_adds_yelp_columns_fresh_db(tmp_path, monkeypatch):
    tmp_db = tmp_path / "dela.sqlite"
    monkeypatch.setattr(loader, "DB_PATH", tmp_db)

    conn = loader.ensure_db()
    conn.close()

    conn = sqlite3.connect(tmp_db)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(places)")
    cols = {row[1] for row in cur.fetchall()}
    conn.close()

    assert {
        "yelp_cuisines",
        "yelp_primary_cuisine",
        "yelp_category_titles",
        "facebook_url",
        "instagram_url",
        "gpv_projection",
    } <= cols


def test_ensure_db_adds_yelp_columns_existing_db(tmp_path, monkeypatch):
    tmp_db = tmp_path / "dela.sqlite"
    conn = sqlite3.connect(tmp_db)
    conn.execute("CREATE TABLE places (place_id TEXT PRIMARY KEY)")
    conn.close()

    monkeypatch.setattr(loader, "DB_PATH", tmp_db)
    conn = loader.ensure_db()
    conn.close()

    conn = sqlite3.connect(tmp_db)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(places)")
    cols = {row[1] for row in cur.fetchall()}
    conn.close()

    assert {
        "yelp_cuisines",
        "yelp_primary_cuisine",
        "yelp_category_titles",
        "facebook_url",
        "instagram_url",
        "gpv_projection",
    } <= cols


def test_ensure_db_updates_partial_schema(tmp_path, monkeypatch):
    """Ensure missing Yelp columns are added to an existing DB."""
    tmp_db = tmp_path / "dela.sqlite"
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        "CREATE TABLE places (place_id TEXT PRIMARY KEY, name TEXT, category TEXT)"
    )
    conn.close()

    monkeypatch.setattr(loader, "DB_PATH", tmp_db)
    conn = loader.ensure_db()
    conn.close()

    conn = sqlite3.connect(tmp_db)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(places)")}
    conn.close()
    assert {
        "yelp_cuisines",
        "yelp_primary_cuisine",
        "yelp_category_titles",
        "facebook_url",
        "instagram_url",
        "gpv_projection",
    } <= cols


def test_load_yelp_json(tmp_path, monkeypatch):
    data = [
        {
            "business": {
                "id": "y1",
                "name": "YelpFoo",
                "location": {
                    "address1": "123 A St",
                    "city": "Olympia",
                    "state": "WA",
                    "zip_code": "98501",
                },
                "coordinates": {"latitude": 47.0, "longitude": -122.0},
                "rating": 4.0,
                "review_count": 7,
                "price": "$",
                "phone": "123",
                "url": "http://foo.com",
                "categories": [{"alias": "thai", "title": "Thai"}],
            },
            "details": {},
            "reviews": {},
        }
    ]
    json_path = tmp_path / "data.json"
    json_path.write_text(json.dumps(data))

    tmp_db = tmp_path / "dela.sqlite"
    monkeypatch.setattr(loader, "DB_PATH", tmp_db)
    loader.load_yelp_json(json_path)

    conn = sqlite3.connect(tmp_db)
    row = conn.execute(
        "SELECT name, city, yelp_rating, yelp_cuisines, source FROM places WHERE place_id='y1'"
    ).fetchone()
    conn.close()
    assert row == ("YelpFoo", "Olympia", 4.0, "thai", "yelp_fetch")
