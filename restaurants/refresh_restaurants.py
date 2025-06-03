import time
import json
import requests
import pandas as pd
from datetime import datetime, timezone
import pathlib
import sqlite3
import logging
import argparse

try:
    from restaurants.utils import setup_logging, normalize_hours, haversine_miles
    from restaurants import loader, yelp_enrich, yelp_fetch
    from restaurants.config import (
        GOOGLE_API_KEY,
        TARGET_OLYMPIA_ZIPS,
        OLYMPIA_LAT,
        OLYMPIA_LON,
    )
    from restaurants.chain_blocklist import CHAIN_BLOCKLIST
    from restaurants.network_utils import check_network
except Exception:  # pragma: no cover - fallback for running as script
    from utils import setup_logging, normalize_hours, haversine_miles
    import loader
    import yelp_enrich
    import yelp_fetch
    from config import (
        GOOGLE_API_KEY,
        TARGET_OLYMPIA_ZIPS,
        OLYMPIA_LAT,
        OLYMPIA_LON,
    )
    from chain_blocklist import CHAIN_BLOCKLIST  # list of substrings that ID big chains
    from network_utils import check_network
MAX_PAGES = 15   # safety cap; tweak per need

# -----------------------------------------------------------------------------
# OPTIONAL DEPENDENCIES --------------------------------------------------------
# -----------------------------------------------------------------------------
try:
    import geocoder  # fallback geocoding for gov CSVs
except ImportError:
    geocoder = None


# -----------------------------------------------------------------------------
# LOCAL MODULES ----------------------------------------------------------------
# -----------------------------------------------------------------------------


# Data store for Google Places results
smb_restaurants_data: list[dict] = []

# -----------------------------------------------------------------------------
# CONFIGURATION ----------------------------------------------------------------
# -----------------------------------------------------------------------------
GOV_CSV_FILES = {
    "wa_health": "wa_food_establishments.csv",
    "thurston_county": "thurston_business_licenses.csv",
}
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"

# -----------------------------------------------------------------------------
# UTILS ------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# 1) GOOGLE PLACES FETCHER -----------------------------------------------------
# -----------------------------------------------------------------------------

def fetch_google_places() -> None:
    """Populate smb_restaurants_data with enriched Google Places SMB rows."""

    if not check_network():
        logging.info("Skipping Google Places fetch due to no network connectivity.")
        return

    text_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    details_url = "https://maps.googleapis.com/maps/api/place/details/json"

    with requests.Session() as session:
        for zip_code in TARGET_OLYMPIA_ZIPS:
            logging.info("Fetching Google Places data for ZIP %s…", zip_code)
            params = {"key": GOOGLE_API_KEY, "query": f"restaurants in {zip_code} WA"}
            page = 1
            while True:
                try:
                    resp = session.get(text_url, params=params, timeout=15)
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
                    logging.error("Error during Text Search for %s: %s", zip_code, exc)
                    break

                for result in data.get("results", []):
                    name = result.get("name", "")
                    if any(block in name.lower() for block in CHAIN_BLOCKLIST):
                        continue  # skip chains

                    basic_row = {
                        "Name": name,
                        "Formatted Address": result.get("formatted_address") or result.get("vicinity"),
                        "Place ID": result.get("place_id"),
                        "Rating": result.get("rating"),
                        "User Ratings Total": result.get("user_ratings_total"),
                        "Business Status": result.get("business_status"),
                        "lat": result["geometry"]["location"].get("lat"),
                        "lon": result["geometry"]["location"].get("lng"),
                    }

                    # ---------- Place Details enrichment ----------
                    det_params = {
                        "key": GOOGLE_API_KEY,
                        "place_id": basic_row["Place ID"],
                        "fields": (
                            "formatted_phone_number,international_phone_number,website,opening_hours,"  # essentials
                            "price_level,types,address_components,photo"
                        ),
                    }
                    details = {}
                    for attempt in range(3):
                        try:
                            d_resp = session.get(details_url, params=det_params, timeout=15)
                            d_resp.raise_for_status()
                            details = d_resp.json().get("result", {})
                            break
                        except Exception as exc:
                            if attempt == 2:
                                logging.error("Details failed for %s: %s", name, exc)
                            else:
                                time.sleep(1)

                    # ----- Parse extra fields -----
                    opening_hours_raw = details.get("opening_hours", {}).get("weekday_text", [])
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
                        "Formatted Phone Number": details.get("formatted_phone_number"),
                        "International Phone Number": details.get("international_phone_number"),
                        "Website": details.get("website"),
                        "Opening Hours": (
                            "; ".join(f"{d}: {t}" for d, t in hours_dict.items())
                            if hours_dict
                            else None
                        ),
                        "Price Level": details.get("price_level"),
                        "Types": ",".join(details.get("types", [])),
                        "Category": (details.get("types") or [None])[0],
                        "Photo Reference": photos[0].get("photo_reference") if photos else None,
                        "Street Address": street,
                        "City": _ac("locality"),
                        "State": _ac("administrative_area_level_1"),
                        "Zip Code": _ac("postal_code") or zip_code,
                    }

                    # distance from Olympia center
                    dist = haversine_miles(
                        OLYMPIA_LAT,
                        OLYMPIA_LON,
                        basic_row["lat"],
                        basic_row["lon"],
                    )
                    enriched["Distance Miles"] = round(dist, 2) if dist is not None else None

                    # final row
                    smb_restaurants_data.append(
                        {
                            **basic_row,
                            **enriched,
                            "source": "google_places_smb",
                            "last_seen": datetime.now(timezone.utc).isoformat(),
                        }
                    )

                # ----- paging (run once per Google response) -----
                next_token = data.get("next_page_token")
                if not next_token or page >= MAX_PAGES:
                    break                    # no more pages or hit our safety cap
                time.sleep(2)                # Google requires a short pause
                params = {"key": GOOGLE_API_KEY, "pagetoken": next_token}
                page += 1

    logging.info("Collected %s SMB rows with enrichment.", len(smb_restaurants_data))


# -----------------------------------------------------------------------------
# 2) GOVERNMENT CSV IMPORTER ----------------------------------------------------
# (unchanged except for timezone fix) -----------------------------------------
# -----------------------------------------------------------------------------

def fetch_gov_csvs():
    logging.info("Government CSV import disabled in this trimmed script.")
    return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])


# -----------------------------------------------------------------------------
# 3) OPENSTREETMAP FETCHER ------------------------------------------------------
# (kept but network‑guarded) ----------------------------------------------------
# -----------------------------------------------------------------------------

def fetch_osm():
    if not check_network():
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])
    # … (same as before) – omitted here for brevity
    return pd.DataFrame()


# -----------------------------------------------------------------------------
# 4) MAIN ----------------------------------------------------------------------
# -----------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Refresh restaurant data")
    parser.add_argument(
        "--yelp-json",
        dest="yelp_json",
        help="Path to write Yelp fetch JSON and import into the DB",
    )
    args = parser.parse_args(argv)

    setup_logging()
    smb_restaurants_data.clear()
    fetch_google_places()

    if not smb_restaurants_data:
        logging.info("No SMB restaurants found – nothing to write.")
        return

    df = pd.DataFrame(smb_restaurants_data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"olympia_smb_google_restaurants_{timestamp}.csv"
    df.to_csv(out_csv, index=False)
    logging.info("Saved %s rows to %s", len(df), out_csv)

    csv_path = pathlib.Path(out_csv)
    loader.load(csv_path)

    if args.yelp_json:
        results = yelp_fetch.enrich_restaurants(TARGET_OLYMPIA_ZIPS[0])
        yelp_path = pathlib.Path(args.yelp_json)
        if results:
            if yelp_path.is_dir():
                yelp_path = yelp_path / f"yelp_businesses_{TARGET_OLYMPIA_ZIPS[0]}_{timestamp}.json"
            with yelp_path.open("w", encoding="utf-8") as f:
                json.dump(results, f, indent=2)
            loader.load_yelp_json(yelp_path)

    yelp_enrich.enrich()

    conn = sqlite3.connect(loader.DB_PATH)
    df_db = pd.read_sql_query("SELECT * FROM places", conn)
    final_csv = f"olympia_smb_google_restaurants_enriched_{timestamp}.csv"
    df_db.to_csv(final_csv, index=False)
    conn.close()
    logging.info("Saved enriched data to %s", final_csv)


if __name__ == "__main__":
    main()
