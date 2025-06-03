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

1. Install the project in editable mode (the `requests` dependency is
   required for network operations):
   ```bash
   pip install -e .
   ```
2. Copy `.env.template` to `.env` and fill in your `GOOGLE_API_KEY`. Other keys
   like `YELP_API_KEY` are optional but required for Yelp utilities. Configuration
   is loaded via `config.py` which also exposes `DEFAULT_ZIP` (set to `98501`).
3. Run the refresh command:
   ```bash
   refresh-restaurants
   ```
   or
   ```bash
   python -m restaurants.refresh_restaurants
   ```

The script currently targets a single ZIP code (`DEFAULT_ZIP`). Adjust
`TARGET_OLYMPIA_ZIPS` in `config.py` if you need additional areas.

## Toast lead enrichment

1. Run `toast_leads.py` to gather additional restaurant leads. Ensure the `GOOGLE_API_KEY` environment variable is set before running.
2. The script outputs an `olympia_toast_smb_<timestamp>.csv` and caches processed place IDs in `seen_place_ids.json` so only new results are fetched.

## Yelp enrichment

Run `yelp_enrich.py` to supplement Google Places rows with Yelp ratings and
categories. The script searches Yelp by the restaurant name and city and scans
 up to five candidates. `rapidfuzz.fuzz.token_set_ratio` picks the best match and
 only applies it when the score is at least 60 (configurable via `YELP_MATCH_THRESHOLD`). If no strong match is found and a
phone number is available, the script falls back to a phone-based Yelp search.
Rows without a valid match are left unchanged and marked as `FAIL`.

Set `YELP_DEBUG=1` to print debug information about failed lookups, including
all Yelp candidate names returned for each query.

Each matched Yelp business includes a list of food categories. Their aliases
(e.g. `pizza`, `italian`) are written to the `yelp_cuisines` column as a
comma‑separated string, with the first alias stored separately as
`yelp_primary_cuisine`.

To fetch Yelp business data directly for a given ZIP code run:

```bash
python -m restaurants.yelp_fetch --zip 98501 --out output.json
```
`yelp_fetch.py` also reads the `YELP_ZIP` and `YELP_OUT` environment variables if
present. When neither a command line argument nor `YELP_ZIP` is provided it
falls back to `DEFAULT_ZIP`. The script writes
`yelp_businesses_<zip>_<timestamp>.json` by default.

## Tests

Ensure the `restaurants` package is importable before running the tests. The
simplest approach is to install the project in editable mode:

```bash
pip install -e .
pip install -r requirements-dev.txt
pytest
```

Alternatively you can adjust `PYTHONPATH` so the `restaurants` imports in the
tests resolve.
