import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
import overpy
# import geocoder # No longer strictly needed if fetch_google_places is fixed
                 # and if fetch_gov_csvs also stops using geocoder.osm
                 # For now, let's keep it in case fetch_gov_csvs still uses it.
from dotenv import load_dotenv
from chain_blocklist import CHAIN_BLOCKLIST
from smb_restaurants_data import smb_restaurants_data

load_dotenv()

# ------------------------------------------------------------------------------
# CONFIGURATION: fill in your API keys and file locations here
# ------------------------------------------------------------------------------
google_api_key = os.getenv("GOOGLE_API_KEY")
if not google_api_key:
    print("Error: GOOGLE_API_KEY not found. Please set the environment variable.")
    exit(1)
GOOGLE_API_KEY = google_api_key  # for Google Places
# TODO: Delivery app API integration deferred. Focusing on core data and free methods.
# DOORDASH_API_KEY = os.getenv("DOORDASH_API_KEY")  # if you have one
# UBER_EATS_API_KEY = os.getenv("UBER_EATS_API_KEY")  # if available
OUTPUT_CSV = "master_restaurants.csv"
GOV_CSV_FILES = {
    # paths to local copies of government CSVs
    "wa_health": "wa_food_establishments.csv",
    "thurston_county": "thurston_business_licenses.csv",
}
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
TARGET_OLYMPIA_ZIPS = [ # Using a short list for testing, as you had it.
    "98501",
    "98502",
    "98506",
] # You can expand this list later when testing is successful.

# ------------------------------------------------------------------------------
# 1) GOOGLE PLACES FETCHER
# ------------------------------------------------------------------------------
def fetch_google_places():
    """Fetch restaurant data using Google Places Text Search.

    For each ZIP in ``TARGET_OLYMPIA_ZIPS`` the query
    "restaurants in {ZIP} WA" is sent and results are paged until all
    pages are retrieved.
    """
    all_rows = []
    for z in TARGET_OLYMPIA_ZIPS:
        print(f"Fetching Google Places data for ZIP code: {z}...") # Added a print statement here
        params = {
            "key": GOOGLE_API_KEY,
            "query": f"restaurants in {z} WA",
        }
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"

        while True:
            try: # Added error handling for requests
                resp = requests.get(url, params=params, timeout=10) # Added timeout
                resp.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
                data = resp.json()
            except requests.exceptions.RequestException as e:
                print(f"Error during Google Places API request for ZIP {z}: {e}")
                break # Break from while loop for this ZIP if network error
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON response for ZIP {z}: {e}")
                break # Break from while loop

            for result in data.get("results", []):
                row = {
                    "name": result.get("name"),
                    "address": result.get("formatted_address") or result.get("vicinity"),
                    "lat": result.get("geometry", {}).get("location", {}).get("lat"), # Safer access
                    "lon": result.get("geometry", {}).get("location", {}).get("lng"), # Safer access
                    "place_id": result.get("place_id"),
                    "phone": None, # Placeholder, Details API needed for phone usually
                    "source": "google_places",
                    "last_seen": datetime.utcnow().isoformat(), # Using ISO format
                }
                all_rows.append(row)

                name_lower = (row["name"] or "").lower()
                if not any(block in name_lower for block in CHAIN_BLOCKLIST):
                    smb_restaurants_data.append({
                        "Name": row["name"],
                        "Formatted Address": row["address"],
                        "Place ID": row["place_id"],
                        "Rating": result.get("rating"),
                        "User Ratings Total": result.get("user_ratings_total"),
                        "Business Status": result.get("business_status"),
                        "Zip Code": z,
                    })

            if "next_page_token" in data:
                next_token = data["next_page_token"]
                # print(f"Fetching next page for ZIP {z}...") # Optional: for debugging pagination
                time.sleep(2)  # Important delay before requesting next page
                # For subsequent page requests, the 'pagetoken' parameter is primary.
                # Other original parameters like 'query' might not be needed or allowed.
                # Let's simplify to just key and pagetoken for subsequent page requests.
                params = {"key": GOOGLE_API_KEY, "pagetoken": next_token}
                # The URL remains the same for paginated requests.
            else:
                break
        print(f"Finished fetching for ZIP {z}. Total raw results from Google for this ZIP: {len(data.get('results', [])) if 'data' in locals() else 0}")


    if not all_rows: # Check if all_rows is empty
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "place_id", "phone", "source", "last_seen"])
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
        try:
            df = pd.read_csv(filepath, dtype=str)
            df = df.rename(columns=lambda c: c.strip())

            if set(["Address", "City", "State", "ZIP"]).issubset(df.columns):
                df["address_full"] = (
                    df["Address"].fillna("").str.strip()
                    + ", "
                    + df["City"].fillna("").str.strip()
                    + ", "
                    + df["State"].fillna("").str.strip()
                    + " "
                    + df["ZIP"].fillna("").str.strip()
                )
            else:
                df["address_full"] = df.get("Full Address", df.get("address", pd.Series(dtype=str))).fillna("").str.strip()


            # Geocoding logic for government CSVs - keep as is for now, but note it uses geocoder.osm
            # This could be a future point of failure if Nominatim is unreachable
            # For now, the focus is on fixing Google Places fetching.
            latlon_cols = set(df.columns) & set(["Latitude", "Longitude", "Lat", "Lon"])
            if not latlon_cols:
                print(f"Attempting to geocode addresses for {key} using geocoder.osm...")
                # Add a small delay and user-agent for geocoder.osm if kept
                # For now, let's make sure it handles potential errors
                latlng_list = []
                for addr in df["address_full"]:
                    if addr:
                        try:
                            time.sleep(1) # Rate limit for Nominatim
                            g = geocoder.osm(addr) # Consider adding a user_agent
                            if g.ok:
                                latlng_list.append(g.latlng)
                            else:
                                latlng_list.append((None, None))
                        except Exception as e_geo:
                            print(f"Error geocoding address '{addr}' with geocoder.osm: {e_geo}")
                            latlng_list.append((None, None))
                    else:
                        latlng_list.append((None, None))
                df["lat"] = [ll[0] if ll else None for ll in latlng_list]
                df["lon"] = [ll[1] if ll else None for ll in latlng_list]

            else: # if lat/lon columns already exist
                df["lat"] = df.get("Latitude", df.get("Lat"))
                df["lon"] = df.get("Longitude", df.get("Lon"))

            # Ensure 'name' column exists, provide a default if not
            if 'name' not in df.columns:
                 df['name'] = "Unknown Name from " + key


            subset_cols = ["name", "address_full", "lat", "lon"]
            # Check if all required columns are present before trying to create subset
            missing_cols = [col for col in subset_cols if col not in df.columns]
            if missing_cols:
                print(f"[WARNING] Missing columns in {key} for subset: {missing_cols}. Skipping this part for {key}.")
            else:
                subset = df[subset_cols].copy()
                subset.columns = ["name", "address", "lat", "lon"] # Rename after subsetting
                subset["phone"] = df.get("Phone", pd.Series(dtype=str)) # Ensure it's a Series
                subset["source"] = f"gov_{key}"
                subset["last_seen"] = datetime.utcnow().isoformat()
                frames.append(subset)

        except Exception as e_csv:
            print(f"Error processing CSV file {filepath}: {e_csv}")
            continue


    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])


# ------------------------------------------------------------------------------
# 3) DELIVERY-APP SCRAPERS
# ------------------------------------------------------------------------------
# TODO: Delivery app API integration deferred. Focusing on core data and free methods.
def fetch_doordash(zip_code_list=None):
    """DoorDash integration disabled."""
    return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])


def fetch_uber_eats(zip_code_list=None):
    """Uber Eats integration disabled."""
    return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])


# ------------------------------------------------------------------------------
# 4) OPENSTREETMAP (OVERPASS) FETCHER
# ------------------------------------------------------------------------------
def fetch_osm():
    """
    Fetch all nodes/ways tagged amenity=restaurant, cafe, fast_food within Thurston County.
    Returns name, address, lat, lon, phone, source, last_seen.
    """
    api = overpy.Overpass(url=OVERPASS_ENDPOINT)
    south, west, north, east = 46.8, -123.1, 47.2, -122.6 # Thurston County approx. bounds

    query = f"""
    [out:json][timeout:60];
    (
      node["amenity"~"restaurant|cafe|fast_food|bar|pub|bakery"]({south},{west},{north},{east});
      way["amenity"~"restaurant|cafe|fast_food|bar|pub|bakery"]({south},{west},{north},{east});
      relation["amenity"~"restaurant|cafe|fast_food|bar|pub|bakery"]({south},{west},{north},{east});
    );
    out center;
    """
    try:
        result = api.query(query)
    except Exception as e_osm_query:
        print(f"Error querying Overpass API: {e_osm_query}")
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])

    rows = []
    def extract_tags(element):
        return {
            "name": element.tags.get("name"),
            "address": element.tags.get("addr:full") or \
                       f"{element.tags.get('addr:housenumber', '')} {element.tags.get('addr:street', '')}".strip() or \
                       element.tags.get("addr:street", ""),
            "phone": element.tags.get("phone") or element.tags.get("contact:phone"),
            "source": "osm",
            "last_seen": datetime.utcnow().isoformat(),
        }

    for node in result.nodes:
        if node.lat is not None and node.lon is not None:
            row = extract_tags(node)
            row["lat"] = node.lat
            row["lon"] = node.lon
            rows.append(row)

    for way in result.ways:
        if way.center_lat is not None and way.center_lon is not None:
            row = extract_tags(way)
            row["lat"] = way.center_lat
            row["lon"] = way.center_lon
            rows.append(row)
            
    for relation in result.relations: # Relations might also have center lat/lon
        if hasattr(relation, 'center_lat') and hasattr(relation, 'center_lon'):
            if relation.center_lat is not None and relation.center_lon is not None:
                row = extract_tags(relation)
                row["lat"] = relation.center_lat
                row["lon"] = relation.center_lon
                rows.append(row)


    if not rows:
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])
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
    # More robust normalization
    addr = addr.lower()
    addr = addr.replace(".", "").replace(",", "")
    # Add more specific normalizations if needed, e.g., st -> street
    return " ".join(addr.split()) # Normalize whitespace


def dedupe_master(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate on a key made from normalized name and street part.
    Adds columns: appeared_in (list of sources), needs_verification (bool).
    """
    if df.empty:
        return pd.DataFrame(columns=list(df.columns) + ["appeared_in", "needs_verification", "dedupe_key", "name_norm", "street_part"])


    df = df.copy()
    df["name_norm"] = df["name"].fillna("").astype(str).str.lower().str.replace(r"[^a-z0-9\s]", "", regex=True).str.strip()
    df["name_norm"] = df["name_norm"].apply(lambda x: " ".join(x.split()))


    df["address_norm"] = df["address"].fillna("").astype(str).apply(normalize_address)
    # Try to extract a more consistent street part, e.g., before first comma or number sequence then street type
    df["street_part"] = df["address_norm"].apply(lambda s: s.split(",")[0].strip()) # Simplified for now

    df["dedupe_key"] = df["street_part"] + "|" + df["name_norm"].str.slice(0, 25) # Max 25 chars from name

    # Ensure 'source' column exists and is list-like if it's from a single source before groupby
    if 'source' in df.columns and not isinstance(df['source'].iloc[0] if not df.empty else None, list):
        df['source'] = df['source'].apply(lambda x: [x] if pd.notna(x) else [])


    deduped_rows = []
    for key, group in df.groupby("dedupe_key", sort=False):
        # Prioritize Google Places data if available
        google_entries = group[group['source'].apply(lambda x: 'google_places' in x if isinstance(x, list) else x == 'google_places')]
        
        if not google_entries.empty:
            # If Google entries exist, choose the one with the most user ratings, or newest if ratings are equal/absent
            # This assumes 'User Ratings Total' might be added to the main df later, or we use 'last_seen'
            best = google_entries.sort_values("last_seen", ascending=False).iloc[0].copy()
        else:
            # If no Google entries, just take the most recently seen entry from other sources
            best = group.sort_values("last_seen", ascending=False).iloc[0].copy()
        
        # Consolidate all sources
        all_sources_in_group = []
        for sources_list in group['source']:
            if isinstance(sources_list, list):
                all_sources_in_group.extend(sources_list)
            elif pd.notna(sources_list): # handle single string source
                all_sources_in_group.append(sources_list)

        best["appeared_in"] = sorted(list(set(all_sources_in_group))) # Unique, sorted list of sources
        best["needs_verification"] = len(best["appeared_in"]) == 1 # Mark for verification if only from one source type
        
        deduped_rows.append(best)

    if not deduped_rows:
         return pd.DataFrame(columns=list(df.columns) + ["appeared_in", "needs_verification"]) # Ensure columns match

    deduped_df = pd.DataFrame(deduped_rows).reset_index(drop=True)
    # Drop intermediate normalization columns if they were added to df directly
    cols_to_drop = [col for col in ["name_norm", "address_norm", "street_part", "dedupe_key"] if col in deduped_df.columns]
    if cols_to_drop:
        deduped_df = deduped_df.drop(columns=cols_to_drop)
        
    return deduped_df


# ------------------------------------------------------------------------------
# 6) MAIN EXECUTION
# ------------------------------------------------------------------------------
def main():
    print("Fetching Google Places…")
    # This now directly uses the filtered smb_restaurants_data
    # Ensure smb_restaurants_data (the list) is converted to a DataFrame
    # with the same columns as other DataFrames if it's to be used directly
    # OR fetch_google_places should return the filtered DataFrame.
    
    # For now, let's assume fetch_google_places returns the raw df_google,
    # and smb_restaurants_data is populated globally.
    # We need to decide if df_google used by main should be the filtered one.

    df_google_raw = fetch_google_places() # This gets ALL Google results
    print(f"→ {len(df_google_raw)} raw rows from Google Places before chain filtering by main.")
    print(f"→ {len(smb_restaurants_data)} SMB restaurants collected from Google Places (in smb_restaurants_data list).")

    # To use the FILTERED data in the main merge, we should convert smb_restaurants_data to a DataFrame
    # that matches the structure of df_google_raw, or adapt df_google_raw.
    # For now, let's create df_google_filtered from smb_restaurants_data
    # The columns in smb_restaurants_data are: "Name", "Formatted Address", "Place ID", "Rating", "User Ratings Total", "Business Status", "Zip Code"
    # The columns expected by dedupe_master (from Google) are: "name", "address", "lat", "lon", "place_id", "phone", "source", "last_seen"

    # We need to map smb_restaurants_data to the expected columns for the merge.
    # This requires lat/lon which are in `df_google_raw` but not explicitly in `smb_restaurants_data` dict structure.
    # Let's modify main to use the filtered data by Place ID.

    # Create a set of Place IDs for the SMB restaurants
    smb_place_ids = {item["Place ID"] for item in smb_restaurants_data if item.get("Place ID")}
    
    # Filter df_google_raw to keep only those rows whose place_id is in smb_place_ids
    if not df_google_raw.empty and 'place_id' in df_google_raw.columns:
        df_google_filtered = df_google_raw[df_google_raw['place_id'].isin(smb_place_ids)].copy()
        # Add source, as it's expected by dedupe and merge
        df_google_filtered.loc[:, 'source'] = 'google_places_smb' 
        print(f"→ {len(df_google_filtered)} filtered SMB rows from Google Places to be used in merge.")
    else:
        print("→ No Google Places data or no place_id column in raw Google data; using empty DataFrame for Google SMBs.")
        df_google_filtered = pd.DataFrame(columns=["name", "address", "lat", "lon", "place_id", "phone", "source", "last_seen"])


    print("Reading government CSVs…")
    df_gov = fetch_gov_csvs()
    print(f"→ {len(df_gov)} rows from government sources")

    print("Querying OpenStreetMap…")
    df_osm = fetch_osm()
    print(f"→ {len(df_osm)} rows from OSM")
    
    # Ensure all DataFrames have a 'source' column if it was potentially missing
    # For df_gov and df_osm, 'source' is already added in their respective functions.

    # Use the filtered Google data for the master list
    all_dfs = []
    if not df_google_filtered.empty:
        all_dfs.append(df_google_filtered)
    if not df_gov.empty:
        all_dfs.append(df_gov)
    if not df_osm.empty:
        all_dfs.append(df_osm)

    if not all_dfs:
        print("No data from any source to merge. Exiting.")
        return

    master = pd.concat(all_dfs, ignore_index=True, sort=False)
    # Ensure essential columns exist, fill with appropriate NAs if not
    for col in ["address", "name", "lat", "lon", "place_id", "phone", "source", "last_seen"]:
        if col not in master.columns:
            if col in ["lat", "lon"]:
                master[col] = pd.NA # Or 0.0, depending on downstream use
            else:
                master[col] = pd.NA


    master["address"] = master["address"].fillna("").astype(str)
    master["name"] = master["name"].fillna("").astype(str)


    print("Deduplicating…")
    master_clean = dedupe_master(master)

    if master_clean.empty:
        print("No data after deduplication. Skipping file save.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"{os.path.splitext(OUTPUT_CSV)[0]}_{timestamp}.csv"
    master_clean.to_csv(out_csv, index=False)
    print(f"Saved merged list to {out_csv}")

    # GeoJSON export
    if 'shapely' in globals() and 'Point' in globals() and 'mapping' in globals(): # Check if shapely was imported
        try:
            features = []
            for _, row in master_clean.iterrows():
                # Ensure lat/lon are valid numbers before trying to create Point
                try:
                    lon = float(row["lon"])
                    lat = float(row["lat"])
                    if pd.notna(lon) and pd.notna(lat):
                        pt = Point(lon, lat)
                        prop = {
                            "name": row["name"],
                            "address": row["address"],
                            "phone": row.get("phone"),
                            "source": ",".join(row.get("appeared_in", []) if isinstance(row.get("appeared_in"), list) else [str(row.get("appeared_in"))]),
                            "needs_verification": row.get("needs_verification", True),
                        }
                        features.append({"type": "Feature", "geometry": mapping(pt), "properties": prop})
                except (ValueError, TypeError):
                    # print(f"Skipping GeoJSON feature for row due to invalid lat/lon: {row['name']}")
                    pass # Skip if lat/lon are not valid floats


            if features:
                geojson = {"type": "FeatureCollection", "features": features}
                geo_out = f"{os.path.splitext(OUTPUT_CSV)[0]}_{timestamp}.geojson"
                with open(geo_out, "w") as f:
                    json.dump(geojson, f, indent=2) # Added indent for readability
                print(f"Saved GeoJSON to {geo_out}")
            else:
                print("No valid features with lat/lon to save in GeoJSON.")

        except NameError: # Handle if shapely components were not imported
            print("Shapely library not available. Skipping GeoJSON export.")
        except Exception as e:
            print(f"Skipping GeoJSON export due to an error: {e}")
    else:
        print("Shapely library components (Point, mapping) not available. Skipping GeoJSON export.")


if __name__ == "__main__":
    main()