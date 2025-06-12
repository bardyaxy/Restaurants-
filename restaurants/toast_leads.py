import json
import csv
import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm.auto import tqdm

# ---------------------------------------------------------------------------
# 0.  Setup
# ---------------------------------------------------------------------------
try:
    from restaurants.config import GOOGLE_API_KEY
    from restaurants.chain_blocklist import CHAIN_BLOCKLIST  # names to skip
    from restaurants.network_utils import check_network  # simple ping check
    from restaurants.utils import setup_logging, is_valid_zip
except ImportError:  # pragma: no cover - fallback when running as script
    from config import GOOGLE_API_KEY  # type: ignore

    try:
        from chain_blocklist import CHAIN_BLOCKLIST  # type: ignore
    except ImportError:
        CHAIN_BLOCKLIST = []
    try:
        from network_utils import check_network  # type: ignore
    except ImportError:

        def check_network() -> bool:  # type: ignore[misc]
            return True

    from utils import setup_logging, is_valid_zip  # type: ignore

SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


# ---------------------------------------------------------------------------
# 1.  Helpers
# ---------------------------------------------------------------------------
def load_seen_ids(path: str = "seen_place_ids.json") -> set[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except (OSError, json.JSONDecodeError):
        return set()


def save_seen_ids(ids: set[str], path: str = "seen_place_ids.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(ids), f, indent=2)


def fetch_details(place_id: str, session: requests.Session) -> dict:
    # Concurrent workers share ``session`` and only issue GET requests,
    # so it is safe to reuse the same session across threads.
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


# ---------------------------------------------------------------------------
# 2.  Main workflow
# ---------------------------------------------------------------------------
ZIP_FILE = "toast_zips.txt"


def load_zip_codes(path: str = ZIP_FILE) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            codes: list[str] = []
            for line in f:
                code = line.strip()
                if not code:
                    continue
                if is_valid_zip(code):
                    codes.append(code)
                else:
                    logging.warning("Invalid ZIP code ignored: %s", code)
            return codes
    except FileNotFoundError:
        return []


def main() -> None:
    setup_logging()
    zip_list = load_zip_codes()
    if not zip_list:
        print(f"No ZIP codes found in {ZIP_FILE}.")
        return

    if not check_network():
        print("[WARN] Skipping Toast leads fetch – network unreachable.")
        return

    seen_ids = load_seen_ids()
    new_rows: list[dict] = []

    with requests.Session() as session:
        session.trust_env = False  # ignore any HTTP(S)_PROXY env vars

        for zip_code in tqdm(zip_list, desc="ZIP codes"):
            zip_start_count = len(new_rows)
            params = {"key": GOOGLE_API_KEY, "query": f"restaurants in {zip_code} WA"}
            page = 1
            while True:
                print(f"→ {zip_code} page {page} requesting", flush=True)
                try:
                    # (connect timeout, read timeout)
                    resp = session.get(SEARCH_URL, params=params, timeout=(5, 10))
                    resp.raise_for_status()
                    data = resp.json()
                    print(
                        f"{zip_code} page {page} -> {resp.status_code} / "
                        f"{data.get('status')}",
                        flush=True,
                    )
                except (requests.Timeout, requests.ConnectionError) as exc:
                    logging.error("Search timeout for %s: %s", zip_code, exc)
                    print(f"⚠️  {zip_code} timed out, skipping", flush=True)
                    break
                except Exception as exc:
                    logging.error("Search failed for %s: %s", zip_code, exc)
                    break

                # ---------- process this page ----------
                futures: dict = {}
                with ThreadPoolExecutor(max_workers=8) as pool:
                    for result in data.get("results", []):
                        name = result.get("name", "")
                        if any(block in name.lower() for block in CHAIN_BLOCKLIST):
                            continue
                        pid = result.get("place_id")
                        if not pid or pid in seen_ids:
                            continue
                        futures[pool.submit(fetch_details, pid, session)] = pid

                    for fut in as_completed(futures):
                        pid = futures[fut]
                        details = fut.result()
                        if not details:
                            continue
                        seen_ids.add(pid)
                        new_rows.append(
                            {
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
                                "lat": details.get("geometry", {})
                                .get("location", {})
                                .get("lat"),
                                "lon": details.get("geometry", {})
                                .get("location", {})
                                .get("lng"),
                                "last_seen": datetime.now(timezone.utc).isoformat(),
                            }
                        )

                # ---------- pagination ----------
                next_tok = data.get("next_page_token")
                if not next_tok:
                    break
                time.sleep(2)  # Google recommends ~2 s wait
                params = {"key": GOOGLE_API_KEY, "pagetoken": next_tok}
                page += 1

            zip_leads = len(new_rows) - zip_start_count
            print(f"{zip_code}: {zip_leads} new leads", flush=True)

    # -----------------------------------------------------------------------
    # 3.  Write results
    # -----------------------------------------------------------------------
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
    print(f"✅ Saved {len(new_rows)} leads to {out_csv}")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
