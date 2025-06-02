"""Add Yelp ratings, review counts, price tiers, and categories to dela.sqlite."""

from __future__ import annotations

import os
import pathlib
import sqlite3
import logging
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
PHONE_SEARCH_URL = "https://api.yelp.com/v3/businesses/search/phone"

MATCH_THRESHOLD = int(os.getenv("YELP_MATCH_THRESHOLD", "70"))
DEBUG = bool(os.getenv("YELP_DEBUG"))
logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO)

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
        SELECT place_id, name, city, state, lat, lon, local_phone
        FROM   places
        WHERE  yelp_status IS NULL
           OR  yelp_status IN ('open','closed')
           OR  yelp_cuisines IS NULL
        """
    ).fetchall()

    success, fail = 0, 0
    for place_id, name, city, state, lat, lon, local_phone in rows:
        params: dict[str, Any] = {"term": f"{name} {city}", "limit": 5}
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
            score = fuzz.token_set_ratio(name, cand_name)
            if score > best_score:
                best_score = score
                best = cand

        # fallback to phone-based search if we didn't get a strong match
        if (not best or best_score < MATCH_THRESHOLD) and local_phone:
            try:
                r2 = requests.get(
                    PHONE_SEARCH_URL,
                    headers=HEADERS,
                    params={"phone": local_phone},
                    timeout=10,
                )
                r2.raise_for_status()
                phone_biz = (r2.json().get("businesses") or [None])[0]
            except Exception:
                phone_biz = None
            if phone_biz:
                best = phone_biz
                best_score = 100

        if not best or best_score < MATCH_THRESHOLD:
            raw_results = biz_candidates
            logging.debug(
                "Yelp search for %r at (%s,%s) returned %d candidates: %s",
                name,
                lat,
                lon,
                len(raw_results),
                [c.get("name") for c in raw_results],
            )
            cur.execute(
                "UPDATE places SET yelp_status='FAIL' WHERE place_id=?",
                (place_id,),
            )
            fail += 1
            continue

        biz = best

        cats = biz.get("categories") or []
        if DEBUG:
            print(f"[DBG] biz categories for {place_id}: {cats}")
        aliases = [c.get("alias") for c in cats if c and c.get("alias")]
        titles = [c.get("title") for c in cats if c and c.get("title")]
        cuisines = ",".join(aliases) if aliases else None
        primary_cuisine = aliases[0] if aliases else None
        category_titles = ",".join(titles) if titles else None
        if DEBUG:
            print(f"[DBG] storing yelp_category_titles: {category_titles!r}")

        cur.execute(
            """
            UPDATE places SET
                yelp_rating         = ?,
                yelp_reviews        = ?,
                yelp_price_tier     = ?,
                yelp_cuisines       = ?,
                yelp_primary_cuisine= ?,
                yelp_category_titles= ?,
                yelp_status         = 'SUCCESS'
            WHERE place_id = ?
            """,
            (
                biz.get("rating"),
                biz.get("review_count"),
                biz.get("price"),
                cuisines,
                primary_cuisine,
                category_titles,
                place_id,
            ),
        )
        success += 1

    conn.commit()
    conn.close()
    print(f"✅ Yelp enrichment done —  SUCCESS: {success}  |  FAIL: {fail}")


if __name__ == "__main__":
    enrich()
