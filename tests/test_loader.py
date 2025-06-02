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
