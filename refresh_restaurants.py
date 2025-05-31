import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
import overpy
try:
    import geocoder  # Used in fetch_gov_csvs for geocoding fallback
except ImportError:
    geocoder = None
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        pass
from chain_blocklist import CHAIN_BLOCKLIST # Make sure chain_blocklist.py exists and defines CHAIN_BLOCKLIST = [...]
from smb_restaurants_data import smb_restaurants_data # Make sure smb_restaurants_data.py exists and defines smb_restaurants_data = []

load_dotenv()

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
google_api_key = os.getenv("GOOGLE_API_KEY")
if not google_api_key:
    print("Error: GOOGLE_API_KEY not found. Please set the environment variable.")
    exit(1)
# No need to reassign to GOOGLE_API_KEY, use google_api_key directly.

OUTPUT_CSV = "master_restaurants.csv"
GOV_CSV_FILES = {
    "wa_health": "wa_food_establishments.csv",
    "thurston_county": "thurston_business_licenses.csv",
}
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
TARGET_OLYMPIA_ZIPS = ["98501", "98502", "98506"] # Kept short for testing

# ------------------------------------------------------------------------------
# NETWORK CHECK
# ------------------------------------------------------------------------------
def check_network(url: str = "https://www.google.com", timeout: int = 5) -> bool:
    """Return ``True`` if network is reachable, otherwise ``False``."""
    try:
        requests.head(url, timeout=timeout)
        return True
    except requests.RequestException:
        return False

NETWORK_AVAILABLE = check_network()
if not NETWORK_AVAILABLE:
    print("[WARNING] Network unreachable. Online data sources will be skipped.")

# ------------------------------------------------------------------------------
# 1) GOOGLE PLACES FETCHER
# ------------------------------------------------------------------------------
def fetch_google_places():
    """Fetch restaurant data using Google Places Text Search.

    For each ZIP in ``TARGET_OLYMPIA_ZIPS`` the query
    "restaurants in {ZIP} WA" is sent and results are paged until all
    pages are retrieved.
    """
    if not NETWORK_AVAILABLE:
        print("[INFO] Skipping Google Places fetch due to no network connectivity.")
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "place_id", "phone", "source", "last_seen", "rating_google", "user_ratings_total_google", "business_status_google"])

    all_rows_for_df = []  # Stores all raw results for the DataFrame returned by this function
    # smb_restaurants_data list (imported) will be populated with filtered results

    for z in TARGET_OLYMPIA_ZIPS:
        print(f"Fetching Google Places data for ZIP code: {z}...")
        params = {
            "key": google_api_key, # Use the variable defined above
            "query": f"restaurants in {z} WA",
        }
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"

        page_count = 0
        while True:
            page_count += 1
            # print(f"Fetching page {page_count} for ZIP {z} with params: {params}") # Uncomment for deep debugging
            try:
                resp = requests.get(url, params=params, timeout=15) # Increased timeout
                resp.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
                data = resp.json()
            except requests.exceptions.Timeout:
                print(f"Timeout during Google Places API request for ZIP {z}, params: {params}")
                break 
            except requests.exceptions.RequestException as e:
                print(f"Error during Google Places API request for ZIP {z}, params: {params}. Error: {e}")
                break 
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON response for ZIP {z} (params: {params}): {e}. Response text: {resp.text[:200]}") # Log part of response
                break

            results_this_page = data.get("results", [])
            if not results_this_page and page_count == 1: # No results on the first page for this query
                print(f"No Google Places results found for query: restaurants in {z} WA")

            for result in results_this_page:
                # Ensure geometry and location exist before trying to access lat/lng
                geometry = result.get("geometry", {})
                location = geometry.get("location", {})
                
                row = {
                    "name": result.get("name"),
                    "address": result.get("formatted_address") or result.get("vicinity"),
                    "lat": location.get("lat"), # Safer access
                    "lon": location.get("lng"), # Safer access
                    "place_id": result.get("place_id"),
                    "phone": None,  # Phone number usually requires a Details request
                    "source": "google_places",
                    "last_seen": datetime.utcnow().isoformat(),
                    # Adding fields needed for smb_restaurants_data that are available here
                    "rating_google": result.get("rating"),
                    "user_ratings_total_google": result.get("user_ratings_total"),
                    "business_status_google": result.get("business_status"),
                }
                all_rows_for_df.append(row)

                name_lower = (row["name"] or "").lower()
                if not any(block in name_lower for block in CHAIN_BLOCKLIST):
                    smb_restaurants_data.append({
                        "Name": row["name"],
                        "Formatted Address": row["address"],
                        "Place ID": row["place_id"],
                        "Rating": row["rating_google"],
                        "User Ratings Total": row["user_ratings_total_google"],
                        "Business Status": row["business_status_google"],
                        "Zip Code": z,
                        # You might want to add lat/lon here too if needed elsewhere for smb_restaurants_data
                        "lat": row["lat"],
                        "lon": row["lon"]
                    })
            
            next_page_token = data.get("next_page_token")
            if next_page_token:
                # print(f"Got next_page_token for ZIP {z}, preparing to fetch next page...") # Uncomment for deep debugging
                time.sleep(2)  # Crucial delay before requesting the next page
                params = {"key": google_api_key, "pagetoken": next_page_token}
                # The URL stays the same for pagetoken requests
            else:
                # print(f"No more pages for ZIP {z}.") # Uncomment for deep debugging
                break
        # print(f"Finished all pages for ZIP {z}.") # Uncomment for deep debugging

    if not all_rows_for_df:
        print("No rows collected from Google Places overall.")
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "place_id", "phone", "source", "last_seen", "rating_google", "user_ratings_total_google", "business_status_google"])
    return pd.DataFrame(all_rows_for_df)


# ------------------------------------------------------------------------------
# 2) GOVERNMENT CSV IMPORTER
# ------------------------------------------------------------------------------
def fetch_gov_csvs():
    """
    Read in government CSVs. Each must have at least: Address, City, State, ZIP.
    Outputs name, address, lat, lon, phone, source, last_seen.
    """
    if not NETWORK_AVAILABLE:
        print("[INFO] Network unavailable. Government CSVs will be read without geocoding.")
    frames = []
    expected_cols = ["name", "address", "lat", "lon", "phone", "source", "last_seen"]
    for key, filepath in GOV_CSV_FILES.items():
        if not os.path.exists(filepath):
            print(f"[WARNING] {filepath} not found; skipping")
            continue
        try:
            df = pd.read_csv(filepath, dtype=str, on_bad_lines='warn')
            df = df.rename(columns=lambda c: c.strip())

            # Ensure 'name' column exists, provide a default if not
            if 'name' not in df.columns and 'Name' in df.columns:
                 df.rename(columns={'Name': 'name'}, inplace=True) # Common alternative
            if 'name' not in df.columns:
                 df['name'] = "Unknown from " + key
            
            df["address_full"] = "" # Initialize
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
            elif "Full Address" in df.columns:
                df["address_full"] = df["Full Address"].fillna("").str.strip()
            elif "address" in df.columns: # fallback
                df["address_full"] = df["address"].fillna("").str.strip()
            else:
                print(f"[WARNING] Could not construct full address for {key}. Skipping this source or expect missing addresses.")


            # Geocoding for government CSVs
            # This part still uses geocoder.osm and might fail if Nominatim is unreachable.
            # Consider replacing with Google Geocoding API if issues persist or for consistency.
            df["lat"] = df.get("Latitude", df.get("Lat"))
            df["lon"] = df.get("Longitude", df.get("Lon"))
            
            # Attempt geocoding only if lat/lon are missing
            needs_geocoding = df['lat'].isna() | df['lon'].isna()
            if needs_geocoding.any() and NETWORK_AVAILABLE and geocoder:
                print(f"Attempting to geocode {needs_geocoding.sum()} missing lat/lon for {key} using geocoder.osm...")
                for index, row_to_geocode in df[needs_geocoding].iterrows():
                    addr_to_geocode = row_to_geocode["address_full"]
                    if addr_to_geocode and isinstance(addr_to_geocode, str) and addr_to_geocode.strip():
                        try:
                            time.sleep(1.1)  # Nominatim rate limit
                            g = geocoder.osm(addr_to_geocode, timeout=10)
                            if g.ok and g.latlng:
                                df.loc[index, 'lat'] = g.latlng[0]
                                df.loc[index, 'lon'] = g.latlng[1]
                            else:
                                print(f"Failed to geocode (or no result for) '{addr_to_geocode}' from {key}")
                        except Exception as e_geo:
                            print(f"Error geocoding address '{addr_to_geocode}' from {key}: {e_geo}")
            elif needs_geocoding.any() and (not NETWORK_AVAILABLE or not geocoder):
                print(f"[INFO] Skipping geocoding for {needs_geocoding.sum()} addresses in {key} due to no network or missing geocoder module.")


            # Select and rename columns for the standardized DataFrame
            df_renamed = df.rename(columns={"address_full": "address"})
            
            current_subset_cols = []
            for col in ["name", "address", "lat", "lon"]: # Essential
                if col in df_renamed.columns:
                    current_subset_cols.append(col)
                else: # Add as empty if missing, to prevent concat issues
                    print(f"[WARNING] Essential column '{col}' missing in {key}, will be added as empty.")
                    df_renamed[col] = pd.NA

            subset = df_renamed[current_subset_cols].copy() # Ensure only existing columns are selected
            
            subset["phone"] = df_renamed.get("Phone", df_renamed.get("phone", pd.Series(dtype=str))).fillna("")
            subset["source"] = f"gov_{key}"
            subset["last_seen"] = datetime.utcnow().isoformat()
            
            # Ensure all expected columns are present before appending
            for col in expected_cols:
                if col not in subset.columns:
                    subset[col] = pd.NA if col in ["lat", "lon"] else ""
            
            frames.append(subset[expected_cols]) # Select in defined order

        except Exception as e_csv_proc:
            print(f"Error processing government CSV file {filepath}: {e_csv_proc}")
            continue

    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        return pd.DataFrame(columns=expected_cols)


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
    if not NETWORK_AVAILABLE:
        print("[INFO] Skipping OpenStreetMap fetch due to no network connectivity.")
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])

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
    def extract_tags_osm(element): # Renamed to avoid conflict if any
        addr_street = element.tags.get('addr:street', '')
        addr_housenumber = element.tags.get('addr:housenumber', '')
        full_address_parts = []
        if addr_housenumber: full_address_parts.append(addr_housenumber)
        if addr_street: full_address_parts.append(addr_street)
        
        return {
            "name": element.tags.get("name"),
            "address": element.tags.get("addr:full") or " ".join(full_address_parts) or element.tags.get("addr:city"), # Fallback to city
            "phone": element.tags.get("phone") or element.tags.get("contact:phone"),
            "source": "osm",
            "last_seen": datetime.utcnow().isoformat(),
        }

    for node in result.nodes:
        if node.lat is not None and node.lon is not None:
            row = extract_tags_osm(node)
            row["lat"] = float(node.lat)
            row["lon"] = float(node.lon)
            rows.append(row)

    for way in result.ways:
        if hasattr(way, 'center_lat') and way.center_lat is not None and hasattr(way, 'center_lon') and way.center_lon is not None:
            row = extract_tags_osm(way)
            row["lat"] = float(way.center_lat)
            row["lon"] = float(way.center_lon)
            rows.append(row)
            
    for relation in result.relations:
        if hasattr(relation, 'center_lat') and relation.center_lat is not None and \
           hasattr(relation, 'center_lon') and relation.center_lon is not None:
            row = extract_tags_osm(relation)
            row["lat"] = float(relation.center_lat)
            row["lon"] = float(relation.center_lon)
            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["name", "address", "lat", "lon", "phone", "source", "last_seen"])
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------
# 5) MERGE & DEDUPE
# ------------------------------------------------------------------------------
def normalize_address(addr: str) -> str:
    if not isinstance(addr, str): return ""
    addr = addr.lower()
    addr = addr.replace(".", "").replace(",", "")
    # Further normalization can be added here
    return " ".join(addr.split()) # Consolidate multiple spaces

def dedupe_master(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        # Define all columns that would be created or expected by subsequent GeoJSON part
        expected_cols = list(df.columns) + ["appeared_in", "needs_verification"] 
        # Remove potential temp columns if they were to be added
        for col in ["name_norm", "address_norm", "street_part", "dedupe_key"]:
            if col in expected_cols: expected_cols.remove(col)
        return pd.DataFrame(columns=expected_cols)

    df_copy = df.copy() # Work on a copy

    # Ensure required columns for deduplication exist, fill if not
    for col in ["name", "address"]:
        if col not in df_copy.columns: df_copy[col] = ""
        df_copy[col] = df_copy[col].fillna("").astype(str)

    df_copy["name_norm"] = df_copy["name"].str.lower().str.replace(r"[^a-z0-9\s]", "", regex=True).str.strip()
    df_copy["name_norm"] = df_copy["name_norm"].apply(lambda x: " ".join(x.split()))
    df_copy["address_norm"] = df_copy["address"].apply(normalize_address)
    df_copy["street_part"] = df_copy["address_norm"].apply(lambda s: s.split(",")[0].strip() if s else "")
    df_copy["dedupe_key"] = df_copy["street_part"] + "|" + df_copy["name_norm"].str.slice(0, 25)

    # Ensure 'source' column exists and handle its type
    if 'source' not in df_copy.columns: df_copy['source'] = "unknown"
    df_copy['source'] = df_copy['source'].fillna("unknown")
    # Convert to list if it's a string, for consistent processing
    df_copy['source_list'] = df_copy['source'].apply(lambda x: [x] if isinstance(x, str) else (x if isinstance(x, list) else ["unknown"]))


    deduped_rows = []
    for _, group in df_copy.groupby("dedupe_key", sort=False):
        # Prioritize Google Places SMB data if available
        google_smb_entries = group[group['source_list'].apply(lambda x: 'google_places_smb' in x)]
        
        if not google_smb_entries.empty:
            best_row = google_smb_entries.sort_values("last_seen", ascending=False).iloc[0].copy()
        else:
            # Fallback to any Google Places entry
            google_any_entries = group[group['source_list'].apply(lambda x: 'google_places' in x)]
            if not google_any_entries.empty:
                best_row = google_any_entries.sort_values("last_seen", ascending=False).iloc[0].copy()
            else:
                # If no Google entries, just take the most recently seen from other sources
                best_row = group.sort_values("last_seen", ascending=False).iloc[0].copy()
        
        all_sources_in_group = []
        for sources in group['source_list']:
            all_sources_in_group.extend(sources)
        
        best_row["appeared_in"] = sorted(list(set(all_sources_in_group)))
        best_row["needs_verification"] = not ('google_places_smb' in best_row["appeared_in"] or 'google_places' in best_row["appeared_in"]) # Verify if no Google source
        
        deduped_rows.append(best_row)

    if not deduped_rows:
        return pd.DataFrame(columns=list(df.columns) + ["appeared_in", "needs_verification"])

    deduped_df = pd.DataFrame(deduped_rows).reset_index(drop=True)
    
    # Drop intermediate columns used for deduplication
    cols_to_drop_dedupe = [col for col in ["name_norm", "address_norm", "street_part", "dedupe_key", "source_list"] if col in deduped_df.columns]
    if cols_to_drop_dedupe:
        deduped_df = deduped_df.drop(columns=cols_to_drop_dedupe)
        
    return deduped_df

# ------------------------------------------------------------------------------
# 6) MAIN EXECUTION
# ------------------------------------------------------------------------------
def main():
    print("Initializing SMB restaurants list...")
    # Ensure smb_restaurants_data is a global list that fetch_google_places can append to
    # Or, modify fetch_google_places to return it, and assign it here.
    # For now, assuming smb_restaurants_data is populated globally by fetch_google_places.
    global smb_restaurants_data # Make sure we are referring to the global list from the import
    smb_restaurants_data = [] # Explicitly reset it here for each run of main()

    print("Fetching Google Places…")
    df_google_raw = fetch_google_places() # This populates smb_restaurants_data globally AND returns all_rows_for_df
    
    # The following print statements are for clarity on what fetch_google_places did
    print(f"→ {len(df_google_raw)} raw rows collected by fetch_google_places function.")
    print(f"→ {len(smb_restaurants_data)} SMB restaurants (chains filtered) collected in smb_restaurants_data list.")

    # Now, create a DataFrame from the filtered smb_restaurants_data for the merge
    if smb_restaurants_data:
        df_google_smb_filtered = pd.DataFrame(smb_restaurants_data)
        # Standardize column names to match what dedupe_master expects from Google source
        df_google_smb_filtered = df_google_smb_filtered.rename(columns={
            "Name": "name",
            "Formatted Address": "address",
            "Place ID": "place_id",
            # Add other mappings if needed, ensure 'lat', 'lon', 'source', 'last_seen' are present
        })
        # Ensure all necessary columns are present for the merge, adding NAs if some are missing from smb_restaurants_data structure
        expected_google_cols = ["name", "address", "lat", "lon", "place_id", "phone", "source", "last_seen", "rating_google", "user_ratings_total_google", "business_status_google"]
        for col in expected_google_cols:
            if col not in df_google_smb_filtered.columns:
                if col in ["lat", "lon", "rating_google", "user_ratings_total_google"]:
                    df_google_smb_filtered[col] = pd.NA 
                else:
                    df_google_smb_filtered[col] = None # or "" for strings
        
        df_google_smb_filtered['source'] = 'google_places_smb' # Mark source clearly
        # last_seen should already be in isoformat from fetch_google_places if added to smb_restaurants_data items
        # For simplicity, if it's missing, let's add it here too.
        if 'last_seen' not in df_google_smb_filtered.columns:
             df_google_smb_filtered['last_seen'] = datetime.utcnow().isoformat()

        print(f"→ Using {len(df_google_smb_filtered)} filtered SMB rows from Google Places for the main merge.")
    else:
        print("→ No SMB restaurants found by Google Places, or smb_restaurants_data is empty. Using empty DataFrame for Google source.")
        df_google_smb_filtered = pd.DataFrame(columns=["name", "address", "lat", "lon", "place_id", "phone", "source", "last_seen"])


    print("Reading government CSVs…")
    df_gov = fetch_gov_csvs()
    print(f"→ {len(df_gov)} rows from government sources")

    print("Querying OpenStreetMap…")
    df_osm = fetch_osm()
    print(f"→ {len(df_osm)} rows from OSM")
    
    all_dfs = []
    if not df_google_smb_filtered.empty: all_dfs.append(df_google_smb_filtered)
    if not df_gov.empty: all_dfs.append(df_gov)
    if not df_osm.empty: all_dfs.append(df_osm)

    if not all_dfs:
        print("No data from any source to merge. Exiting.")
        return

    master = pd.concat(all_dfs, ignore_index=True)
    
    # Ensure essential columns for dedupe_master exist, fill if necessary
    for col in ["name", "address", "source", "last_seen"]: # lat,lon are also important but handled differently
        if col not in master.columns: master[col] = "" if col != "last_seen" else datetime.utcnow().isoformat()
        master[col] = master[col].fillna("" if col != "last_seen" else datetime.utcnow().isoformat())
    
    # Ensure lat/lon are float or can be converted, handle errors
    for col in ["lat", "lon"]:
        if col in master.columns:
            master[col] = pd.to_numeric(master[col], errors='coerce')
        else:
            master[col] = pd.NA


    print("Deduplicating…")
    master_clean = dedupe_master(master)

    if master_clean.empty:
        print("No data after deduplication. Skipping file save.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"{os.path.splitext(OUTPUT_CSV)[0]}_{timestamp}.csv"
    master_clean.to_csv(out_csv, index=False)
    print(f"Saved merged list to {out_csv}")

    # GeoJSON export - ensure shapely is imported at the top or checked before use
    try:
        from shapely.geometry import Point, mapping # Moved import here to be conditional

        features = []
        # Check if 'appeared_in' column exists, if not, create an empty list for sources
        if 'appeared_in' not in master_clean.columns:
            master_clean['appeared_in'] = [[] for _ in range(len(master_clean))]
        if 'needs_verification' not in master_clean.columns:
            master_clean['needs_verification'] = True


        for _, row in master_clean.iterrows():
            try:
                lon_val = row.get("lon")
                lat_val = row.get("lat")
                if pd.notna(lon_val) and pd.notna(lat_val):
                    pt = Point(float(lon_val), float(lat_val))
                    
                    appeared_in_val = row.get("appeared_in")
                    if isinstance(appeared_in_val, list):
                        source_str = ",".join(appeared_in_val)
                    elif pd.notna(appeared_in_val):
                        source_str = str(appeared_in_val)
                    else:
                        source_str = "unknown"

                    prop = {
                        "name": row.get("name", ""),
                        "address": row.get("address", ""),
                        "phone": row.get("phone", ""),
                        "source": source_str,
                        "needs_verification": row.get("needs_verification", True),
                    }
                    features.append({"type": "Feature", "geometry": mapping(pt), "properties": prop})
            except (ValueError, TypeError, AttributeError) as e_point:
                # print(f"Skipping GeoJSON feature for row '{row.get('name', 'N/A')}' due to invalid lat/lon or data: {e_point}")
                pass

        if features:
            geojson = {"type": "FeatureCollection", "features": features}
            geo_out = f"{os.path.splitext(OUTPUT_CSV)[0]}_{timestamp}.geojson"
            with open(geo_out, "w") as f:
                json.dump(geojson, f, indent=2)
            print(f"Saved GeoJSON to {geo_out}")
        else:
            print("No valid features with lat/lon to save in GeoJSON.")

    except ImportError:
        print("Shapely library not found. Skipping GeoJSON export. Please install it if needed (pip install Shapely).")
    except Exception as e_geojson:
        print(f"Skipping GeoJSON export due to an error: {e_geojson}")


if __name__ == "__main__":
    main()
