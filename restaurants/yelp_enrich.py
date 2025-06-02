"""Add Yelp ratings, review counts, and price tiers to rows in dela.sqlite."""

from __future__ import annotations

import pathlib
import sqlite3
from typing import Any

import requests
from rapidfuzz import fuzz

try:
    from restaurants.network_utils import check_network
    from restaurants.config import YELP_API_KEY
except Exception:  # pragma: no cover - fallback for running as script
    from network_utils import check_network
    from config import YELP_API_KEY

DB_PATH = pathlib.Path(__file__).with_name("dela.sqlite")

if not YELP_API_KEY:
    raise SystemExit("⚠️  Set YELP_API_KEY first (env var or .env file)")

HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
SEARCH_URL = "https://api.yelp.com/v3/businesses/search"

# --------------------------------------------------------------------------- #
# Core logic
# --------------------------------------------------------------------------- #
def enrich() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")

    if not check_network():
        print("[WARN] Yelp enrichment skipped – network unreachable.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # queue rows that were never touched or are missing cuisines (old DB schema)
    rows = cur.execute(
        """
        SELECT place_id, name, city, state, lat, lon
        FROM   places
        WHERE  yelp_status IS NULL
           OR  yelp_status IN ('open','closed')
           OR  yelp_cuisines IS NULL
        """
    ).fetchall()

    success, fail = 0, 0
    for place_id, name, city, state, lat, lon in rows:
        params: dict[str, Any] = {"term": name, "limit": 5}
        if lat is not None and lon is not None:
            params.update({"latitude": lat, "longitude": lon})
        else:
            params["location"] = f"{city}, {state}"

        try:
            r = requests.get(SEARCH_URL, headers=HEADERS, params=params, timeout=10)
            r.raise_for_status()
            biz_candidates = r.json().get("businesses") or []
        except Exception:
            biz_candidates = []  # network / JSON error → treat as no match

        best, best_score = None, 0
        for cand in biz_candidates:
            cand_name = cand.get("name") or ""
            score = fuzz.ratio(name, cand_name)
            if score > best_score:
                best_score = score
                best = cand

        if not best or best_score < 70:
            cur.execute(
                "UPDATE places SET yelp_status='FAIL' WHERE place_id=?",
                (place_id,),
            )
            fail += 1
            continue

        biz = best

        cats = biz.get("categories") or []
        aliases = [c.get("alias") for c in cats if c and c.get("alias")]
        cuisines = ",".join(aliases) if aliases else None
        primary_cuisine = aliases[0] if aliases else None

        cur.execute(
            """
            UPDATE places SET
                yelp_rating         = ?,
                yelp_reviews        = ?,
                yelp_price_tier     = ?,
                yelp_cuisines       = ?,
                yelp_primary_cuisine= ?,
                yelp_status         = 'SUCCESS'
            WHERE place_id = ?
            """,
            (
                biz.get("rating"),
                biz.get("review_count"),
                biz.get("price"),
                cuisines,
                primary_cuisine,
                place_id,
            ),
        )
        success += 1

    conn.commit()
    conn.close()
    print(f"✅ Yelp enrichment done —  SUCCESS: {success}  |  FAIL: {fail}")


if __name__ == "__main__":
    enrich()
