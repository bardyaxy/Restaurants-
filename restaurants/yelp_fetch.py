"""Fetch Yelp restaurant data for a ZIP code."""

from __future__ import annotations

import argparse
import json
import logging
import pathlib
import os
from datetime import datetime
from typing import Any

import requests

try:
    from restaurants.config import YELP_API_KEY, DEFAULT_ZIP
    from restaurants.network_utils import check_network
    from restaurants.utils import setup_logging
except Exception:  # pragma: no cover - fallback for running as script
    from config import YELP_API_KEY, DEFAULT_ZIP
    from network_utils import check_network
    try:
        from utils import setup_logging
    except Exception:  # pragma: no cover - fallback if utils missing
        def setup_logging(level: int = logging.INFO) -> None:
            logging.basicConfig(level=level)


if not YELP_API_KEY:
    raise SystemExit("\u26a0\ufe0f  Set YELP_API_KEY first (env var or .env file)")

HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
DETAILS_URL = "https://api.yelp.com/v3/businesses/{id}"
REVIEWS_URL = "https://api.yelp.com/v3/businesses/{id}/reviews"


def search_yelp_businesses(
    zip_code: str, session: requests.Session, limit: int = 50
) -> list[dict[str, Any]]:
    """Return all Yelp businesses for ``zip_code`` handling pagination."""
    results: list[dict[str, Any]] = []
    offset = 0
    while True:
        params = {
            "location": zip_code,
            "categories": "restaurants",
            "limit": limit,
            "offset": offset,
        }
        try:
            resp = session.get(
                SEARCH_URL, headers=HEADERS, params=params, timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # pragma: no cover - network errors
            logging.error("Search failed for %s offset %s: %s", zip_code, offset, exc)
            break
        page_results = data.get("businesses") or []
        results.extend(page_results)
        total = data.get("total", len(results))
        offset += limit
        if offset >= total or not page_results:
            break
    return results


def get_business_details(business_id: str, session: requests.Session) -> dict:
    """Return Yelp details for ``business_id``."""
    try:
        resp = session.get(
            DETAILS_URL.format(id=business_id), headers=HEADERS, timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:  # pragma: no cover - network errors
        logging.error("Details failed for %s: %s", business_id, exc)
        return {}


def get_business_reviews(business_id: str, session: requests.Session) -> dict:
    """Return Yelp reviews for ``business_id``."""
    try:
        resp = session.get(
            REVIEWS_URL.format(id=business_id), headers=HEADERS, timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:  # pragma: no cover - network errors
        logging.error("Reviews failed for %s: %s", business_id, exc)
        return {}


def enrich_restaurants(zip_code: str) -> list[dict[str, Any]]:
    """Return Yelp business info with details and reviews for ``zip_code``."""
    if not check_network():
        logging.info("Skipping Yelp fetch – network unreachable.")
        return []

    with requests.Session() as session:
        businesses = search_yelp_businesses(zip_code, session)
        enriched: list[dict[str, Any]] = []
        for biz in businesses:
            biz_id = biz.get("id")
            if not biz_id:
                continue
            details = get_business_details(biz_id, session)
            reviews = get_business_reviews(biz_id, session)
            enriched.append({"business": biz, "details": details, "reviews": reviews})
        return enriched


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Fetch Yelp restaurant data")
    parser.add_argument(
        "--zip",
        dest="zip_code",
        default=os.getenv("YELP_ZIP", DEFAULT_ZIP),
        help="ZIP code to query (env YELP_ZIP)",
    )
    parser.add_argument(
        "--out",
        dest="out_path",
        default=os.getenv("YELP_OUT"),
        help="Output JSON file path (env YELP_OUT)",
    )
    args = parser.parse_args(argv)

    setup_logging()
    results = enrich_restaurants(args.zip_code)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = pathlib.Path(
        args.out_path
        or f"yelp_businesses_{args.zip_code}_{timestamp}.json"
    )
    if out_file.is_dir():
        out_file = out_file / f"yelp_businesses_{args.zip_code}_{timestamp}.json"
    if results:
        with out_file.open("w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        logging.info("Saved %s businesses to %s", len(results), out_file)
    else:
        logging.info("No data fetched.")


if __name__ == "__main__":
    main()
