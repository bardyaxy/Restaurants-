import os
import json
import csv
import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_args, **_kwargs):
        pass
from network_utils import check_network

load_dotenv()


try:
    from chain_blocklist import CHAIN_BLOCKLIST
except Exception:
    CHAIN_BLOCKLIST = []

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise SystemExit("Error: GOOGLE_API_KEY environment variable not set")

SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

TARGET_OLYMPIA_ZIPS = ["98501"]

logging.basicConfig(
    filename="error.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s: %(message)s",
)

def load_seen_ids(path: str = "seen_place_ids.json") -> set[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_seen_ids(ids: set[str], path: str = "seen_place_ids.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(ids), f, indent=2)


def fetch_details(place_id: str, session: requests.Session) -> dict:
    params = {
        "key": GOOGLE_API_KEY,
        "place_id": place_id,
        "fields": (
            "name,formatted_address,formatted_phone_number,international_phone_number,"
            "website,geometry,price_level,rating,user_ratings_total,business_status"
        ),
    }
    try:
        resp = session.get(DETAILS_URL, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("result", {})
    except Exception as exc:
        logging.error("Details fetch failed for %s: %s", place_id, exc)
        return {}


def main() -> None:
    if not check_network():
        print("[WARN] Skipping Toast leads fetch - network unreachable.")
        return
    seen_ids = load_seen_ids()
    new_rows = []

    with requests.Session() as session:
        for zip_code in TARGET_OLYMPIA_ZIPS:
            params = {"key": GOOGLE_API_KEY, "query": f"restaurants in {zip_code} WA"}
            page = 1
            while True:
                try:
                    resp = session.get(SEARCH_URL, params=params, timeout=15)
                    print(
                        f"{zip_code} page {page} -> {resp.status_code} / {resp.json().get('status')}",
                        flush=True,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as exc:
                    logging.error("Search failed for %s: %s", zip_code, exc)
                    break

                futures = {}
                with ThreadPoolExecutor(max_workers=8) as excpool:
                    for result in data.get("results", []):
                        name = result.get("name", "")
                        if any(block in name.lower() for block in CHAIN_BLOCKLIST):
                            continue
                        pid = result.get("place_id")
                        if not pid or pid in seen_ids:
                            continue
                        futures[excpool.submit(fetch_details, pid, session)] = pid

                    for fut in as_completed(futures):
                        pid = futures[fut]
                        details = fut.result()
                        if not details:
                            continue
                        seen_ids.add(pid)
                        row = {
                            "Business Name": details.get("name"),
                            "Formatted Address": details.get("formatted_address"),
                            "Place ID": pid,
                            "Formatted Phone Number": details.get(
                                "formatted_phone_number"
                            ),
                            "International Phone Number": details.get(
                                "international_phone_number"
                            ),
                            "Website": details.get("website"),
                            "Rating": details.get("rating"),
                            "User Ratings Total": details.get("user_ratings_total"),
                            "Business Status": details.get("business_status"),
                            "Price Level": details.get("price_level"),
                            "lat": details.get("geometry", {}).get("location", {}).get("lat"),
                            "lon": details.get("geometry", {}).get("location", {}).get("lng"),
                            "last_seen": datetime.now(timezone.utc).isoformat(),
                        }
                        new_rows.append(row)

                next_tok = data.get("next_page_token")
                if not next_tok:
                    break
                time.sleep(2)
                params = {"key": GOOGLE_API_KEY, "pagetoken": next_tok}
                page += 1

    if not new_rows:
        print("No new leads found.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"olympia_toast_smb_{timestamp}.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(new_rows[0].keys()))
        writer.writeheader()
        writer.writerows(new_rows)
    save_seen_ids(seen_ids)
    print(f"Saved {len(new_rows)} leads to {out_csv}")


if __name__ == "__main__":
    main()
