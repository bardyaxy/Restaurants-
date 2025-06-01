import os
import time
import json
import math
import requests
import pandas as pd
from datetime import datetime, timezone
import overpy

# -----------------------------------------------------------------------------
# OPTIONAL DEPENDENCIES --------------------------------------------------------
# -----------------------------------------------------------------------------
try:
    import geocoder  # fallback geocoding for gov CSVs
except ImportError:
    geocoder = None

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_args, **_kwargs):
        pass

# -----------------------------------------------------------------------------
# LOCAL MODULES ----------------------------------------------------------------
# -----------------------------------------------------------------------------
from chain_blocklist import CHAIN_BLOCKLIST  # list of substrings that ID big chains
from network_utils import check_network

# Data store for Google Places results
smb_restaurants_data: list[dict] = []

load_dotenv()

# -----------------------------------------------------------------------------
# CONFIGURATION ----------------------------------------------------------------
# -----------------------------------------------------------------------------
GOOGLE_API_KEY: str | None = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise SystemExit("Error: GOOGLE_API_KEY not found. Add it to your .env or export it before running.")

GOV_CSV_FILES = {
    "wa_health": "wa_food_establishments.csv",
    "thurston_county": "thurston_business_licenses.csv",
}
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
TARGET_OLYMPIA_ZIPS = ["98501"]  # extend this list as needed
OLYMPIA_LAT, OLYMPIA_LON = 47.0379, -122.9007  # used for distance calculation

# -----------------------------------------------------------------------------
# UTILS ------------------------------------------------------------------------
# -----------------------------------------------------------------------------



def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great‑circle distance in miles between two lat/lon points."""
    R = 3958.8  # Earth radius in miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# -----------------------------------------------------------------------------
# 1) GOOGLE PLACES FETCHER ------------------------------------------------------
# -----------------------------------------------------------------------------

def fetch_google_places() -> None:
    """Populate smb_restaurants_data with enriched Google Places SMB rows."""

    if not check_network():
        print("[INFO] Skipping Google Places fetch due to no network connectivity.")
        return

    text_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    details_url = "https://maps.googleapis.com/maps/api/place/details/json"

    with requests.Session() as session:
        for zip_code in TARGET_OLYMPIA_ZIPS:
            print(f"Fetching Google Places data for ZIP {zip_code}…")
            params = {"key": GOOGLE_API_KEY, "query": f"restaurants in {zip_code} WA"}
            while True:
                try:
                    resp = session.get(text_url, params=params, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                except (requests.RequestException, json.JSONDecodeError) as exc:
                    print(f"Error during Text Search for {zip_code}: {exc}")
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
                try:
                    d_resp = session.get(details_url, params=det_params, timeout=15)
                    d_resp.raise_for_status()
                    details = d_resp.json().get("result", {})
                except Exception as exc:
                    print(f"  → Details failed for {name}: {exc}")

                # ----- Parse extra fields -----
                opening_hours = details.get("opening_hours", {}).get("weekday_text", [])
                photos = details.get("photos", [])
                addr_comps = details.get("address_components", [])

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
                    "Opening Hours": "; ".join(opening_hours) if opening_hours else None,
                    "Price Level": details.get("price_level"),
                    "Types": ",".join(details.get("types", [])),
                    "Photo Reference": photos[0].get("photo_reference") if photos else None,
                    "Street Address": street,
                    "City": _ac("locality"),
                    "State": _ac("administrative_area_level_1"),
                    "Zip Code": _ac("postal_code") or zip_code,
                }

                # distance from Olympia center
                if basic_row["lat"] is not None and basic_row["lon"] is not None:
                    enriched["Distance Miles"] = round(
                        haversine(OLYMPIA_LAT, OLYMPIA_LON, basic_row["lat"], basic_row["lon"]), 2
                    )
                else:
                    enriched["Distance Miles"] = None

                # final row
                smb_restaurants_data.append(
                    {
                        **basic_row,
                        **enriched,
                        "source": "google_places_smb",
                        "last_seen": datetime.now(timezone.utc).isoformat(),
                    }
                )

                # ----- paging -----
                next_token = data.get("next_page_token")
                if not next_token:
                    break
                time.sleep(2)  # Google requirement before using next_page_token
                params = {"key": GOOGLE_API_KEY, "pagetoken": next_token}

    print(f"Collected {len(smb_restaurants_data)} SMB rows with enrichment.")


# -----------------------------------------------------------------------------
# 2) GOVERNMENT CSV IMPORTER ----------------------------------------------------
# (unchanged except for timezone fix) -----------------------------------------
# -----------------------------------------------------------------------------

def fetch_gov_csvs():
    print("[INFO] Government CSV import disabled in this trimmed script.")
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

def main() -> None:
    smb_restaurants_data.clear()
    fetch_google_places()

    if not smb_restaurants_data:
        print("No SMB restaurants found – nothing to write.")
        return

    df = pd.DataFrame(smb_restaurants_data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"olympia_smb_google_restaurants_{timestamp}.csv"
    df.to_csv(out_csv, index=False)
    print(f"Saved {len(df)} rows to {out_csv}")


if __name__ == "__main__":
    main()
