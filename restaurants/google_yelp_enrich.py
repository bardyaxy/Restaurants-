"""Google & Yelp enrichment utility.

This module searches for a restaurant using the Google Places Text
Search API then enriches the result with Yelp details and reviews.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any, Dict, Iterable

import requests

from rapidfuzz import fuzz

from .config import GOOGLE_API_KEY, YELP_API_KEY
from .network_utils import check_network

GOOGLE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
YELP_DETAILS_URL = "https://api.yelp.com/v3/businesses/{id}"
YELP_REVIEWS_URL = "https://api.yelp.com/v3/businesses/{id}/reviews"
YELP_PHONE_SEARCH_URL = "https://api.yelp.com/v3/businesses/search/phone"

# Minimum fuzzy match score required to accept a Yelp business match
YELP_MATCH_THRESHOLD = 60


def search_google_place(
    name: str, location: str, session: requests.Session
) -> dict[str, Any]:
    """Return the first Google place result for ``name`` and ``location``."""
    params = {
        "query": f"{name} {location}",
        "type": "restaurant",
        "key": GOOGLE_API_KEY,
    }
    resp = session.get(GOOGLE_SEARCH_URL, params=params, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("results") or []
    return results[0] if results else {}


def get_google_details(place_id: str, session: requests.Session) -> dict[str, Any]:
    """Return phone details for a Google place."""
    params = {
        "place_id": place_id,
        "key": GOOGLE_API_KEY,
        "fields": "formatted_phone_number,international_phone_number",
    }
    resp = session.get(GOOGLE_DETAILS_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("result", {})


def _pick_best_by_name(
    name: str, businesses: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    """Return the business with the highest fuzzy match to ``name``."""
    best: dict[str, Any] | None = None
    best_score = -1.0
    for biz in businesses:
        score = fuzz.token_set_ratio(name, biz.get("name", ""))
        if score > best_score:
            best = biz
            best_score = score
    if best_score < YELP_MATCH_THRESHOLD:
        return {}
    return best or {}


def search_yelp_business(
    name: str,
    lat: float | None,
    lon: float | None,
    location: str,
    session: requests.Session,
) -> dict[str, Any]:
    """Return the best Yelp business for ``name`` using coords or location."""
    params: dict[str, str | int | float | None] = {"term": name, "limit": 5}
    if lat is not None and lon is not None:
        params.update({"latitude": str(lat), "longitude": str(lon)})
    else:
        params["location"] = location

    resp = session.get(YELP_SEARCH_URL, params=params, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("businesses") or []

    if not results and (lat is not None and lon is not None):
        params.pop("latitude", None)
        params.pop("longitude", None)
        params["location"] = location
        resp = session.get(YELP_SEARCH_URL, params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("businesses") or []

    return _pick_best_by_name(name, results)


def search_yelp_by_phone(phone: str, session: requests.Session) -> dict[str, Any]:
    """Return the first Yelp business for the given phone number."""
    if not phone:
        return {}
    digits = "".join(c for c in phone if c.isdigit() or c == "+")
    resp = session.get(YELP_PHONE_SEARCH_URL, params={"phone": digits}, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("businesses") or []
    return results[0] if results else {}


def get_yelp_details(business_id: str, session: requests.Session) -> dict[str, Any]:
    resp = session.get(YELP_DETAILS_URL.format(id=business_id), timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_yelp_reviews(business_id: str, session: requests.Session) -> dict[str, Any]:
    resp = session.get(YELP_REVIEWS_URL.format(id=business_id), timeout=10)
    resp.raise_for_status()
    return resp.json()


def enrich_restaurant(name: str, location: str) -> dict[str, Any]:
    """Return combined Google and Yelp data for ``name`` in ``location``."""
    if not check_network():
        raise SystemExit("Network unavailable; Yelp enrichment required")

    if not GOOGLE_API_KEY or not YELP_API_KEY:
        raise SystemExit("Missing GOOGLE_API_KEY or YELP_API_KEY")

    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}

    with requests.Session() as session:
        session.headers.update(headers)
        g_place = search_google_place(name, location, session)
        if not g_place:
            return {}
        g_details = get_google_details(g_place.get("place_id", ""), session)
        loc = g_place.get("geometry", {}).get("location", {})
        lat, lon = loc.get("lat"), loc.get("lng")
        yelp_biz: Dict[str, Any] = {}
        yelp_details: Dict[str, Any] = {}
        yelp_reviews: Dict[str, Any] = {}

        yelp_biz = search_yelp_business(
            g_place.get("name", name), lat, lon, location, session
        )
        if not yelp_biz:
            phone = g_details.get("formatted_phone_number") or g_details.get(
                "international_phone_number"
            )
            yelp_biz = search_yelp_by_phone(phone or "", session)
        biz_id = yelp_biz.get("id")
        if biz_id:
            yelp_details = get_yelp_details(biz_id, session)
            yelp_reviews = get_yelp_reviews(biz_id, session)

        cuisines = [
            c.get("alias")
            for c in (yelp_details.get("categories") or [])
            if c.get("alias")
        ]
        summary = {
            "cuisines": cuisines,
            "primary_cuisine": cuisines[0] if cuisines else None,
            "website": yelp_details.get("url"),
            "delivery": "delivery" in (yelp_details.get("transactions") or []),
            "review_count": yelp_details.get("review_count"),
            "rating": yelp_details.get("rating"),
            "price": yelp_details.get("price"),
            "phone": yelp_details.get("display_phone"),
            "is_closed": yelp_details.get("is_closed"),
        }

        return {
            "google": g_place,
            "yelp": {
                "business": yelp_biz,
                "details": yelp_details,
                "reviews": yelp_reviews,
                "summary": summary,
            },
        }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Enrich a restaurant via Google and Yelp"
    )
    parser.add_argument("name", help="Restaurant name")
    parser.add_argument("location", help="City/State or address for Google search")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    data = enrich_restaurant(args.name, args.location)
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
