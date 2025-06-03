"""Google & Yelp enrichment utility.

This module searches for a restaurant using the Google Places Text
Search API then enriches the result with Yelp details and reviews.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any, Dict

import requests

from .config import GOOGLE_API_KEY, YELP_API_KEY
from .network_utils import check_network

GOOGLE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
YELP_DETAILS_URL = "https://api.yelp.com/v3/businesses/{id}"
YELP_REVIEWS_URL = "https://api.yelp.com/v3/businesses/{id}/reviews"

HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}


def search_google_place(name: str, location: str, session: requests.Session) -> dict[str, Any]:
    """Return the first Google place result for ``name`` and ``location``."""
    params = {"query": f"{name} {location}", "type": "restaurant", "key": GOOGLE_API_KEY}
    resp = session.get(GOOGLE_SEARCH_URL, params=params, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("results") or []
    return results[0] if results else {}


def search_yelp_business(name: str, lat: float, lon: float, session: requests.Session) -> dict[str, Any]:
    """Return the first Yelp business for the given name and coordinates."""
    params = {"term": name, "latitude": lat, "longitude": lon, "limit": 1}
    resp = session.get(YELP_SEARCH_URL, headers=HEADERS, params=params, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("businesses") or []
    return results[0] if results else {}


def get_yelp_details(business_id: str, session: requests.Session) -> dict[str, Any]:
    resp = session.get(YELP_DETAILS_URL.format(id=business_id), headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_yelp_reviews(business_id: str, session: requests.Session) -> dict[str, Any]:
    resp = session.get(YELP_REVIEWS_URL.format(id=business_id), headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def enrich_restaurant(name: str, location: str) -> dict[str, Any]:
    """Return combined Google and Yelp data for ``name`` in ``location``."""
    if not check_network():
        logging.info("Network unavailable, skipping enrichment")
        return {}

    if not GOOGLE_API_KEY or not YELP_API_KEY:
        raise SystemExit("Missing GOOGLE_API_KEY or YELP_API_KEY")

    with requests.Session() as session:
        g_place = search_google_place(name, location, session)
        if not g_place:
            return {}
        loc = g_place.get("geometry", {}).get("location", {})
        lat, lon = loc.get("lat"), loc.get("lng")
        yelp_biz: Dict[str, Any] = {}
        yelp_details: Dict[str, Any] = {}
        yelp_reviews: Dict[str, Any] = {}
        if lat is not None and lon is not None:
            yelp_biz = search_yelp_business(g_place.get("name", name), lat, lon, session)
            biz_id = yelp_biz.get("id")
            if biz_id:
                yelp_details = get_yelp_details(biz_id, session)
                yelp_reviews = get_yelp_reviews(biz_id, session)

        return {
            "google": g_place,
            "yelp": {
                "business": yelp_biz,
                "details": yelp_details,
                "reviews": yelp_reviews,
            },
        }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Enrich a restaurant via Google and Yelp")
    parser.add_argument("name", help="Restaurant name")
    parser.add_argument("location", help="City/State or address for Google search")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    data = enrich_restaurant(args.name, args.location)
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
