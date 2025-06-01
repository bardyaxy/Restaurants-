# Olympia Restaurant Aggregator

This project collects restaurant information for ZIP codes around Olympia, Washington using the Google Places API and optional public data sources.

## Features

- **Google Places fetcher** with a chain blocklist to keep only local small businesses. Records include rating, user review count, business status and coordinates.
- **Government CSV importer** for Washington health and Thurston County license data with optional geocoding when the network is available.
- **OpenStreetMap fetcher** (Overpass API) for additional restaurant listings.
- **Deduplication routine** that merges results from all sources while prioritizing Google Places SMB entries.
- **Network check** to gracefully skip online fetchers when offline.
- Output saved as `olympia_smb_google_restaurants_single_zip.csv`.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Copy `.env.template` to `.env` and fill in your `GOOGLE_API_KEY`. Other keys are optional.
3. Run the script:
   ```bash
   python refresh_restaurants.py
   ```

The script currently targets a single ZIP code (`98501`). Adjust `TARGET_OLYMPIA_ZIPS` in `refresh_restaurants.py` if you need additional areas.

## Toast lead enrichment

1. Run `toast_leads.py` to gather additional restaurant leads. On the first execution the script creates a `config.yaml` file. Edit this file and provide your `google_api_key` before running again.
2. Subsequent runs output an `olympia_toast_smb_<timestamp>.csv` and cache processed place IDs in `seen_place_ids.json` so only new results are fetched.
3. The script requires the `pyyaml` package which is included in `requirements.txt`.
