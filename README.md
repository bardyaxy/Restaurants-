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

By default `refresh_restaurants.py` iterates over the `TARGET_OLYMPIA_ZIPS`
list in `config.py`.  You can pass `--zips 98501,98502` or enter a list when
prompted to restrict the fetch to specific ZIP codes.

## Optional GUI

A minimal Tkinter interface is available for users who prefer not to run
commands in the terminal. Launch it with:

```bash
python -m restaurants.gui
```

The window exposes buttons for the two main workflows: refreshing restaurant
data and fetching Toast leads.

## Toast lead enrichment

1. Run `toast_leads.py` to gather additional restaurant leads. Ensure the `GOOGLE_API_KEY` environment variable is set before running.
2. Edit `toast_zips.txt` with the ZIP codes you want to query. Each line should
   contain a single ZIP code. The script only reads ZIP codes from this file.
3. The script outputs an `olympia_toast_smb_<timestamp>.csv` and caches processed place IDs in `seen_place_ids.json` so only new results are fetched.

## Yelp enrichment

Run `google_yelp_enrich.py` to supplement Google Places rows with Yelp ratings and
categories. The script searches Yelp by the restaurant name and city and scans
up to five candidates. `rapidfuzz.fuzz.token_set_ratio` picks the best match and
 only applies it when the score meets the `YELP_MATCH_THRESHOLD` (60 by default). If no strong match is found and a
phone number is available, the script falls back to a phone-based Yelp search.
Rows without a valid match are left unchanged and marked as `FAIL`.

Ensure the `dela.sqlite` database exists (created by `refresh_restaurants.py`)
and that `YELP_API_KEY` is set before running. If either is missing the script
exits with a message explaining what is required.

Set `YELP_DEBUG=1` to print debug information about failed lookups, including
all Yelp candidate names returned for each query.

Each matched Yelp business includes a list of food categories. Their aliases
(e.g. `pizza`, `italian`) are written to the `yelp_cuisines` column as a
comma‑separated string, with the first alias stored separately as
`yelp_primary_cuisine`.

## Tests

Ensure the `restaurants` package is importable before running the tests.
All runtime dependencies live in `requirements.txt` while the testing tools
are listed in `requirements-dev.txt`:

```bash
pip install -e .
pip install -r requirements-dev.txt
pytest
```

The dev file includes `pytest`, `mypy` and type stubs. It also pulls in the
packages from `requirements.txt` so a single install command prepares the
environment. For convenience you can run `./setup_tests.sh` to perform these
steps automatically.

Alternatively you can adjust `PYTHONPATH` so the `restaurants` imports in the
tests resolve.

## Type checking

Install the development requirements to enable `mypy` type checking. The file
`requirements-dev.txt` now includes stub packages such as `types-requests` and
`pandas-stubs` so `mypy` can analyze the project without missing-import errors:

```bash
pip install -r requirements-dev.txt
mypy restaurants
```
