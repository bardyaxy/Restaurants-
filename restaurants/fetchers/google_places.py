from __future__ import annotations
import time
import json
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm.auto import tqdm

import pandas as pd

from restaurants.utils import normalize_hours, haversine_miles
from restaurants.config import GOOGLE_API_KEY, OLYMPIA_LAT, OLYMPIA_LON
from restaurants.chain_blocklist import CHAIN_BLOCKLIST
from restaurants.network_utils import check_network

from .base import BaseFetcher

MAX_PAGES = 15
GOOGLE_TEXT_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


class GooglePlacesFetcher(BaseFetcher):
    """Fetch restaurant data from Google Places."""

    @staticmethod
    def _fetch_details(
        session: requests.Session, params: dict, place_name: str
    ) -> dict:
        """Helper to fetch place details with retries."""
        for attempt in range(3):
            try:
                resp = session.get(GOOGLE_DETAILS_URL, params=params, timeout=15)
                resp.raise_for_status()
                return resp.json().get("result", {})
            except Exception as exc:  # pragma: no cover - network errors
                if attempt == 2:
                    logging.error("Details failed for %s: %s", place_name, exc)
                    raise SystemExit(1)
                time.sleep(1)
        return {}

    def fetch(self, zip_codes: list[str], **opts) -> list[dict]:
        if not check_network():
            logging.error("Network unavailable; cannot fetch Google Places data.")
            raise SystemExit(1)

        results: list[dict] = []
        with requests.Session() as session, ThreadPoolExecutor(
            max_workers=8
        ) as executor:
            for zip_code in tqdm(zip_codes, desc="ZIP codes"):
                logging.info("Fetching Google Places data for ZIP %s…", zip_code)
                params = {
                    "key": GOOGLE_API_KEY,
                    "query": f"restaurants in {zip_code} WA",
                }
                page = 1
                while True:
                    try:
                        resp = session.get(GOOGLE_TEXT_URL, params=params, timeout=15)
                        logging.info(
                            "%s page %s -> %s / %s",
                            zip_code,
                            page,
                            resp.status_code,
                            resp.json().get("status"),
                        )
                        resp.raise_for_status()
                        data = resp.json()
                    except (requests.RequestException, json.JSONDecodeError) as exc:
                        logging.error(
                            "Error during Text Search for %s: %s", zip_code, exc
                        )
                        raise SystemExit(1)

                    page_rows = []
                    for result in data.get("results", []):
                        name = result.get("name", "")
                        if any(block in name.lower() for block in CHAIN_BLOCKLIST):
                            continue

                        basic_row = {
                            "Name": name,
                            "Formatted Address": result.get("formatted_address")
                            or result.get("vicinity"),
                            "Place ID": result.get("place_id"),
                            "Rating": result.get("rating"),
                            "User Ratings Total": result.get("user_ratings_total"),
                            "Business Status": result.get("business_status"),
                            "lat": result["geometry"]["location"].get("lat"),
                            "lon": result["geometry"]["location"].get("lng"),
                        }

                        det_params = {
                            "key": GOOGLE_API_KEY,
                            "place_id": basic_row["Place ID"],
                            "fields": (
                                "formatted_phone_number,international_phone_number,website,opening_hours,"
                                "price_level,types,address_components,photo"
                            ),
                        }
                        page_rows.append((basic_row, det_params, name))

                    future_map = {
                        executor.submit(self._fetch_details, session, dp, nm): br
                        for br, dp, nm in page_rows
                    }
                    for fut in as_completed(list(future_map)):
                        basic_row = future_map[fut]
                        details = fut.result()

                        opening_hours_raw = details.get("opening_hours", {}).get(
                            "weekday_text", []
                        )
                        photos = details.get("photos", [])
                        addr_comps = details.get("address_components", [])

                        def _parse_hours(items: list[str]) -> dict:
                            out: dict[str, str] = {}
                            for seg in items:
                                if ":" not in seg:
                                    continue
                                day, times = seg.split(":", 1)
                                out[day.strip()] = times.strip()
                            return out

                        hours_dict = (
                            normalize_hours(_parse_hours(opening_hours_raw))
                            if opening_hours_raw
                            else {}
                        )

                        def _ac(key: str):
                            for comp in addr_comps:
                                if key in comp.get("types", []):
                                    return comp.get("long_name")
                            return ""

                        street = f"{_ac('street_number')} {_ac('route')}".strip()

                        enriched = {
                            "Formatted Phone Number": details.get(
                                "formatted_phone_number"
                            ),
                            "International Phone Number": details.get(
                                "international_phone_number"
                            ),
                            "Website": details.get("website"),
                            "Opening Hours": (
                                "; ".join(f"{d}: {t}" for d, t in hours_dict.items())
                                if hours_dict
                                else None
                            ),
                            "Price Level": details.get("price_level"),
                            "Types": ",".join(details.get("types", [])),
                            "Category": (details.get("types") or [None])[0],
                            "Photo Reference": (
                                photos[0].get("photo_reference") if photos else None
                            ),
                            "Street Address": street,
                            "City": _ac("locality"),
                            "State": _ac("administrative_area_level_1"),
                            "Zip Code": _ac("postal_code") or zip_code,
                        }

                        dist = haversine_miles(
                            OLYMPIA_LAT,
                            OLYMPIA_LON,
                            basic_row["lat"],
                            basic_row["lon"],
                        )
                        enriched["Distance Miles"] = (
                            round(dist, 2) if dist is not None else None
                        )

                        results.append(
                            {
                                **basic_row,
                                **enriched,
                                "source": "google_places_smb",
                                "last_seen": datetime.now(timezone.utc).isoformat(),
                            }
                        )

                    next_token = data.get("next_page_token")
                    if not next_token or page >= MAX_PAGES:
                        break
                    time.sleep(2)
                    params = {"key": GOOGLE_API_KEY, "pagetoken": next_token}
                    page += 1

        logging.info("Collected %s SMB rows with enrichment.", len(results))
        return results
