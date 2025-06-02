"""Add Yelp ratings and prices to existing entries in dela.sqlite."""

from __future__ import annotations

import os
import sqlite3
import pathlib
from typing import Any

import requests

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*_args: Any, **_kwargs: Any) -> None:
        pass

load_dotenv()

DB_PATH = pathlib.Path(__file__).with_name("dela.sqlite")
YELP_API_KEY = os.getenv("YELP_API_KEY")

if not YELP_API_KEY:
    raise SystemExit("Warning: YELP_API_KEY environment variable not set")

HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
SEARCH_URL = "https://api.yelp.com/v3/businesses/search"


def enrich() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    rows = cur.execute(
        "SELECT place_id, name, city, state, lat, lon FROM places"
    ).fetchall()

    success = 0
    for place_id, name, city, state, lat, lon in rows:
        params: dict[str, Any] = {"term": name, "limit": 1}
        if lat is not None and lon is not None:
            params.update({"latitude": lat, "longitude": lon})
        elif city or state:
            loc = ", ".join(part for part in [city, state] if part)
            params["location"] = loc
        else:
            continue

        try:
            resp = requests.get(SEARCH_URL, headers=HEADERS, params=params, timeout=10)
            resp.raise_for_status()
            businesses = resp.json().get("businesses") or []
        except Exception:
            continue

        if not businesses:
            continue

        biz = businesses[0]
        cur.execute(
            """
            UPDATE places
            SET yelp_rating=?,
                yelp_reviews=?,
                yelp_price_tier=?,
                yelp_status=?
            WHERE place_id=?
            """,
            (
                biz.get("rating"),
                biz.get("review_count"),
                biz.get("price"),
                "closed" if biz.get("is_closed") else "open",
                place_id,
            ),
        )
        success += 1

    conn.commit()
    conn.close()
    print(f"✅ Yelp enrichment done. Success rows: {success}")


if __name__ == "__main__":
    enrich()
