import csv
import sqlite3
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

    assert {"yelp_cuisines", "yelp_primary_cuisine"} <= cols


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

    assert {"yelp_cuisines", "yelp_primary_cuisine"} <= cols
