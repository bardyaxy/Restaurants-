# Olympia Restaurant Aggregator

This project collects restaurant information for ZIP codes around Olympia, Washington using the Google Places API and optional public data sources.

## Features

- **Google Places SMB fetcher** with a chain blocklist. This is the only active data source and records rating, review count, business status and coordinates.
- **Government CSV importer** *(disabled)* for Washington health and Thurston County license data.
- **OpenStreetMap fetcher** *(disabled)* for additional restaurant listings.
- **Deduplication routine** that merges results from all sources while prioritizing Google Places SMB entries.
- **Network check** using a lightweight GET request to gracefully skip online
  fetchers when offline. Some corporate networks block HEAD requests, so the
  check avoids them by default.
- Output saved as `olympia_smb_google_restaurants_<timestamp>.csv`.
- Run `prep_restaurants.py` to clean the latest CSV and write
  `restaurants_prepped.csv` and `restaurants_prepped.xlsx`.

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

1. Run `toast_leads.py` to gather additional restaurant leads. Ensure the `GOOGLE_API_KEY` environment variable is set before running.
2. The script outputs an `olympia_toast_smb_<timestamp>.csv` and caches processed place IDs in `seen_place_ids.json` so only new results are fetched.

## Tests

To run the unit tests, install pytest and execute:
```bash
pip install pytest
pytest
```
