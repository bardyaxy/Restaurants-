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

GOV_CSV_FILES = {
    "wa_health": "wa_food_establishments.csv",
    "thurston_county": "thurston_business_licenses.csv",
}
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
TARGET_OLYMPIA_ZIPS = ["98501"]  # Simplified to a single ZIP

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
    """Populate ``smb_restaurants_data`` using Google Places Text Search.

    For each ZIP in ``TARGET_OLYMPIA_ZIPS`` the query
    "restaurants in {ZIP} WA" is sent and results are paged until all
    pages are retrieved.
    """
    if not NETWORK_AVAILABLE:
        print("[INFO] Skipping Google Places fetch due to no network connectivity.")
        return

    # ``smb_restaurants_data`` will be populated with filtered results

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

                name_lower = (row["name"] or "").lower()
                if any(block in name_lower for block in CHAIN_BLOCKLIST):
                    continue

                # Fetch additional details for this place
                details_url = "https://maps.googleapis.com/maps/api/place/details/json"
                details_params = {
                    "key": google_api_key,
                    "place_id": row["place_id"],
                    "fields": (
                        "formatted_phone_number,international_phone_number,"
                        "website,opening_hours,price_level,types," 
                        "address_components,photo"
                    ),
                }
                details_resp = {}
                try:
                    d_resp = requests.get(details_url, params=details_params, timeout=15)
                    d_resp.raise_for_status()
                    details_resp = d_resp.json().get("result", {})
                except Exception as e_det:
                    print(f"Error fetching details for {row['place_id']}: {e_det}")

                phone_fmt = details_resp.get("formatted_phone_number")
                phone_intl = details_resp.get("international_phone_number")
                website = details_resp.get("website")
                weekday_text = details_resp.get("opening_hours", {}).get("weekday_text", [])
                opening_hours = ";".join(weekday_text) if weekday_text else None
                price_level = details_resp.get("price_level")
                types = ",".join(details_resp.get("types", []))
                photo_ref = None
                photos = details_resp.get("photos", [])
                if photos:
                    photo_ref = photos[0].get("photo_reference")

                # Address components parsing
                comps = details_resp.get("address_components", [])
                street_no = route = city = state = zip_code = ""
                for comp in comps:
                    t = comp.get("types", [])
                    if "street_number" in t:
                        street_no = comp.get("long_name", "")
                    if "route" in t:
                        route = comp.get("long_name", "")
                    if "locality" in t:
                        city = comp.get("long_name", "")
                    if "administrative_area_level_1" in t:
                        state = comp.get("short_name", "")
                    if "postal_code" in t:
                        zip_code = comp.get("long_name", "")
                street_address = f"{street_no} {route}".strip()
                zip_final = zip_code or z

                smb_restaurants_data.append({
                    "Name": row["name"],
                    "Formatted Address": row["address"],
                    "Place ID": row["place_id"],
                    "Rating": row["rating_google"],
                    "User Ratings Total": row["user_ratings_total_google"],
                    "Business Status": row["business_status_google"],
                    "Formatted Phone Number": phone_fmt,
                    "International Phone Number": phone_intl,
                    "Website": website,
                    "Opening Hours": opening_hours,
                    "Price Level": price_level,
                    "Types": types,
                    "Photo Reference": photo_ref,
                    "Street Address": street_address,
                    "City": city,
                    "State": state,
                    "Zip Code": zip_final,
                    "source": "google_places_smb",
                    "lat": row["lat"],
                    "lon": row["lon"],
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

    if not smb_restaurants_data:
        print("No rows collected from Google Places overall.")
    return


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
    """Fetch SMB restaurants from Google Places for a single ZIP and save them."""
    smb_restaurants_data.clear()  # reset on each run

    # Fetch Google Places data; results populate smb_restaurants_data
    fetch_google_places()

    print(
        f"→ {len(smb_restaurants_data)} SMB restaurants collected from Google Places for ZIP {TARGET_OLYMPIA_ZIPS[0]}."
    )

    if not smb_restaurants_data:
        print("No SMB restaurants found. Nothing to save.")
        return

    df = pd.DataFrame(smb_restaurants_data)
    out_csv = "olympia_smb_google_restaurants_single_zip.csv"
    df.to_csv(out_csv, index=False)
    print(f"Saved filtered Google SMB list to {out_csv}")


if __name__ == "__main__":
    main()
