import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
import overpy
import geocoder  # for address normalization
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------------------
# CONFIGURATION: fill in your API keys and file locations here
# ------------------------------------------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")  # for Google Places
DOORDASH_API_KEY = os.getenv("DOORDASH_API_KEY")  # if you have one
UBER_EATS_API_KEY = os.getenv("UBER_EATS_API_KEY")  # if available
OUTPUT_CSV = "master_restaurants.csv"
GOV_CSV_FILES = {
    # paths to local copies of government CSVs
    "wa_health": "wa_food_establishments.csv",
    "thurston_county": "thurston_business_licenses.csv",
}
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
SEARCH_ZIP_CODES = ["98501", "98502", "98506", "98512", "98516"]  # adjust for your territory

# ------------------------------------------------------------------------------
# 1) GOOGLE PLACES FETCHER
# ------------------------------------------------------------------------------
def fetch_google_places(radius=50000, types_list=None, keyword_list=None):
    """
    Query Google Places API for restaurant, cafe, bakery, etc. within each ZIP’s area.
    Returns a DataFrame with name, address, lat, lon, place_id, phone, source, last_seen.
    """
    if types_list is None:
        types_list = ["restaurant", "bar", "cafe", "bakery", "food"]
    if keyword_list is None:
        keyword_list = ["taco", "pizza", "deli", "pub", "bbq"]

    all_rows = []
    for z in SEARCH_ZIP_CODES:
        g = geocoder.osm(z + ", WA, USA")
        if g.ok:
            center_lat, center_lng = g.latlng
        else:
            print(f"Failed to geocode ZIP {z}")
            continue

        for place_type in types_list:
            params = {
                "key": GOOGLE_API_KEY,
                "location": f"{center_lat},{center_lng}",
                "radius": radius,
                "type": place_type,
            }
            url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
            while True:
                resp = requests.get(url, params=params)
                data = resp.json()
                for result in data.get("results", []):
                    row = {
                        "name": result.get("name"),
                        "address": result.get("vicinity") or result.get("formatted_address"),
                        "lat": result["geometry"]["location"]["lat"],
                        "lon": result["geometry"]["location"]["lng"],
                        "place_id": result.get("place_id"),
                        "phone": None,
                        "source": "google_places",
                        "last_seen": datetime.utcnow(),
                    }
                    all_rows.append(row)

                if "next_page_token" in data:
                    next_token = data["next_page_token"]
                    time.sleep(2)
                    params["pagetoken"] = next_token
                    continue
                else:
                    break

    return pd.DataFrame(all_rows)


# ------------------------------------------------------------------------------
# 2) GOVERNMENT CSV IMPORTER
# ------------------------------------------------------------------------------
def fetch_gov_csvs():
    """
    Read in government CSVs. Each must have at least: Address, City, State, ZIP.
    Outputs name, address, lat, lon, phone, source, last_seen.
    """
    frames = []
    for key, filepath in GOV_CSV_FILES.items():
        if not os.path.exists(filepath):
            print(f"[WARNING] {filepath} not found; skipping")
            continue
        df = pd.read_csv(filepath, dtype=str)
        df = df.rename(columns=lambda c: c.strip())

        if set(["Address", "City", "State", "ZIP"]).issubset(df.columns):
            df["address_full"] = (
                df["Address"].str.strip()
                + ", "
                + df["City"].str.strip()
                + ", "
                + df["State"].str.strip()
                + " "
                + df["ZIP"].str.strip()
            )
        else:
            df["address_full"] = df.get("Full Address", df.get("address", "")).str.strip()

        latlon_cols = set(df.columns) & set(["Latitude", "Longitude", "Lat", "Lon"])
        if not latlon_cols:
            df["geocode"] = df["address_full"].apply(lambda a: geocoder.osm(a).latlng if a else (None, None))
            df["lat"] = df["geocode"].apply(lambda x: x[0] if isinstance(x, (list, tuple)) else None)
            df["lon"] = df["geocode"].apply(lambda x: x[1] if isinstance(x, (list, tuple)) else None)
        else:
            df["lat"] = df.get("Latitude") or df.get("Lat")
            df["lon"] = df.get("Longitude") or df.get("Lon")

        subset = df[["name", "address_full", "lat", "lon"]].copy()
        subset.columns = ["name", "address", "lat", "lon"]
        subset["phone"] = df.get("Phone", None)
        subset["source"] = f"gov_{key}"
        subset["last_seen"] = datetime.utcnow()
        frames.append(subset)

    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])


# ------------------------------------------------------------------------------
# 3) DELIVERY-APP SCRAPERS
# ------------------------------------------------------------------------------
def fetch_doordash(zip_code_list=None):
    """
    Skeleton for DoorDash scraping. Adjust the URL/payload after inspecting network calls.
    Returns name, address, lat, lon, phone, source, last_seen.
    """
    all_rows = []
    if zip_code_list is None:
        zip_code_list = SEARCH_ZIP_CODES

    for z in zip_code_list:
        url = f"https://api.doordash.com/v2/search/store/?location={z},WA&limit=100"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {DOORDASH_API_KEY}",
        }
        resp = requests.get(url, headers=headers)
        data = resp.json()
        for shop in data.get("stores", []):
            row = {
                "name": shop.get("name"),
                "address": shop.get("address"),
                "lat": shop.get("latitude"),
                "lon": shop.get("longitude"),
                "phone": shop.get("phone_number"),
                "source": "doordash",
                "last_seen": datetime.utcnow(),
            }
            all_rows.append(row)
        time.sleep(1)

    return pd.DataFrame(all_rows)


def fetch_uber_eats(zip_code_list=None):
    """
    Skeleton for Uber Eats scraping. Adjust URL/payload after inspecting network calls.
    Returns name, address, lat, lon, phone, source, last_seen.
    """
    all_rows = []
    if zip_code_list is None:
        zip_code_list = SEARCH_ZIP_CODES

    for z in zip_code_list:
        url = f"https://www.ubereats.com/api/getFeed?obtainBy=restaurant&address={z}%2C%20WA"
        headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers)
        data = resp.json()
        for item in data.get("restaurants", []):
            row = {
                "name": item.get("name"),
                "address": item.get("address", {}).get("formatted"),
                "lat": item.get("address", {}).get("latitude"),
                "lon": item.get("address", {}).get("longitude"),
                "phone": item.get("phone"),
                "source": "ubereats",
                "last_seen": datetime.utcnow(),
            }
            all_rows.append(row)
        time.sleep(1)

    return pd.DataFrame(all_rows)


# ------------------------------------------------------------------------------
# 4) OPENSTREETMAP (OVERPASS) FETCHER
# ------------------------------------------------------------------------------
def fetch_osm():
    """
    Fetch all nodes/ways tagged amenity=restaurant, cafe, fast_food within Thurston County.
    Returns name, address, lat, lon, phone, source, last_seen.
    """
    api = overpy.Overpass(url=OVERPASS_ENDPOINT)

    south, west, north, east = 46.8, -123.1, 47.2, -122.6

    query = f"""
    [out:json][timeout:60];
    (
      node["amenity"="restaurant"]({south},{west},{north},{east});
      way["amenity"="restaurant"]({south},{west},{north},{east});
      node["amenity"="cafe"]({south},{west},{north},{east});
      way["amenity"="cafe"]({south},{west},{north},{east});
      node["amenity"="fast_food"]({south},{west},{north},{east});
      way["amenity"="fast_food"]({south},{west},{north},{east});
    );
    out center;
    """
    result = api.query(query)

    rows = []
    for node in result.nodes:
        rows.append({
            "name": node.tags.get("name"),
            "address": node.tags.get("addr:full") or node.tags.get("addr:street", ""),
            "lat": node.lat,
            "lon": node.lon,
            "phone": node.tags.get("phone"),
            "source": "osm",
            "last_seen": datetime.utcnow(),
        })
    for way in result.ways:
        rows.append({
            "name": way.tags.get("name"),
            "address": way.tags.get("addr:full") or way.tags.get("addr:street", ""),
            "lat": way.center_lat,
            "lon": way.center_lon,
            "phone": way.tags.get("phone"),
            "source": "osm",
            "last_seen": datetime.utcnow(),
        })

    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------
# 5) MERGE & DEDUPE
# ------------------------------------------------------------------------------
def normalize_address(addr: str) -> str:
    """
    Lowercase, strip punctuation, expand abbreviations if you like.
    For production, use Google Geocoder or USPS API for true standardization.
    """
    if not isinstance(addr, str):
        return ""
    return addr.lower().replace(".", "").replace(",", "").strip()


def dedupe_master(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate on a key made from normalized name and street part.
    Adds columns: appeared_in (list of sources), needs_verification (bool).
    """
    df = df.copy()
    df["name_norm"] = df["name"].fillna("").str.lower().str.replace(r"[^a-z0-9 ]", "", regex=True)
    df["street_part"] = df["address"].fillna("").str.lower().apply(lambda s: s.split(",")[0].strip())
    df["dedupe_key"] = df["street_part"] + "|" + df["name_norm"].str[:20]

    deduped_rows = []
    for key, group in df.groupby("dedupe_key"):
        if len(group) == 1:
            row = group.iloc[0].copy()
            row["appeared_in"] = group["source"].tolist()
            row["needs_verification"] = True
            deduped_rows.append(row)
        else:
            best = group.sort_values("last_seen", ascending=False).iloc[0].copy()
            best["appeared_in"] = list(set(group["source"].tolist()))
            best["needs_verification"] = False
            deduped_rows.append(best)

    deduped_df = pd.DataFrame(deduped_rows).reset_index(drop=True)
    return deduped_df


# ------------------------------------------------------------------------------
# 6) MAIN EXECUTION
# ------------------------------------------------------------------------------
def main():
    print("Fetching Google Places…")
    df_google = fetch_google_places()
    print(f"→ {len(df_google)} rows from Google Places")

    print("Reading government CSVs…")
    df_gov = fetch_gov_csvs()
    print(f"→ {len(df_gov)} rows from government sources")

    print("Scraping DoorDash…")
    df_dd = fetch_doordash()
    print(f"→ {len(df_dd)} rows from DoorDash")

    print("Scraping Uber Eats…")
    df_ue = fetch_uber_eats()
    print(f"→ {len(df_ue)} rows from Uber Eats")

    print("Querying OpenStreetMap…")
    df_osm = fetch_osm()
    print(f"→ {len(df_osm)} rows from OSM")

    master = pd.concat([df_google, df_gov, df_dd, df_ue, df_osm], ignore_index=True, sort=False)
    master["address"] = master["address"].fillna("")
    master["name"] = master["name"].fillna("")

    print("Deduplicating…")
    master_clean = dedupe_master(master)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"{os.path.splitext(OUTPUT_CSV)[0]}_{timestamp}.csv"
    master_clean.to_csv(out_csv, index=False)
    print(f"Saved merged list to {out_csv}")

    try:
        from shapely.geometry import Point, mapping

        features = []
        for _, row in master_clean.iterrows():
            if row["lat"] and row["lon"]:
                pt = Point(float(row["lon"]), float(row["lat"]))
                prop = {
                    "name": row["name"],
                    "address": row["address"],
                    "phone": row.get("phone"),
                    "source": ",".join(row.get("appeared_in", [])),
                    "needs_verification": row["needs_verification"],
                }
                features.append({"type": "Feature", "geometry": mapping(pt), "properties": prop})

        geojson = {"type": "FeatureCollection", "features": features}
        geo_out = f"{os.path.splitext(OUTPUT_CSV)[0]}_{timestamp}.geojson"
        with open(geo_out, "w") as f:
            json.dump(geojson, f)
        print(f"Saved GeoJSON to {geo_out}")
    except Exception as e:
        print("Skipping GeoJSON export:", e)


if __name__ == "__main__":
    main()

