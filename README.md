# Olympia Restaurant Aggregator

This project collects restaurant information for ZIP codes around Olympia, Washington using the Google Places API and optional public data sources.

## Features

- **Google Places SMB fetcher** with a chain blocklist that skips major
  franchises to focus on local spots. Filtered chains include Starbucks,
  McDonald's, Subway, Burger King, Taco Bell, Wendy's, KFC, Dunkin', Pizza Hut,
  Domino's Pizza, Little Caesars, Chipotle Mexican Grill, Panera Bread,
  Chick-fil-A, Panda Express, Arby's, Dairy Queen, Applebee's, Olive Garden,
  Red Lobster, Outback Steakhouse, Buffalo Wild Wings, Denny's (Dennys), IHOP,
  Red Robin, Cheesecake Factory, Papa John's (Papa Johns), TGI Friday's,
  Five Guys and Dave & Buster's.
- **Government CSV importer** *(disabled)* for Washington health and Thurston County license data.
- **OpenStreetMap fetcher** *(disabled)* for additional restaurant listings.
- **GPV projection fetcher** *(optional)* reads projected visitor volume from a
  CSV and adds a `GPV Projection` column.
- **Deduplication routine** that merges results from all sources while prioritizing Google Places SMB entries.
- **Automatic social link extraction** scrapes each website for Facebook and Instagram URLs.
- **Network check** using a lightweight GET request to gracefully skip online
  fetchers when offline. Some corporate networks block HEAD requests, so the
  check avoids them by default.
- Output saved as `olympia_smb_google_restaurants_<timestamp>.csv`.
- Run `prep_restaurants.py` to clean the latest CSV and write
  `restaurants_prepped.csv` and `restaurants_prepped.xlsx`.

## Setup

1. Install the project in editable mode. The `requirements.txt` file pins
   specific versions of each dependency for reproducible installs:
   ```bash
   pip install -e .
   pip install -r requirements.txt
   ```
2. Copy `.env.template` to `.env` and add your `GOOGLE_API_KEY` and
   `MAPBOX_TOKEN`. Other keys like `YELP_API_KEY` are optional but required for
   Yelp utilities. The `.env` file is listed in `.gitignore` and should remain
   untracked. You can also set `WA_APP_TOKEN` for the Washington owner lookup.
   Configuration is loaded via `config.py` which also exposes
   `DEFAULT_ZIP` (set to `98501`).
3. Run the refresh command:
   ```bash
   refresh-restaurants
   ```
   or
   ```bash
   python -m restaurants.refresh_restaurants
   ```

By default `refresh_restaurants.py` loads ZIP codes from `toast_zips.txt` using
`restaurants.config.load_zip_codes`. The file includes many ZIP codes across the
Olympia area. You can pass `--zips` with any additional ZIP codes (e.g.
`--zips 98502`). The script displays a progress bar via `tqdm` as it fetches
Google results. After loading the results it automatically enriches each row
with Yelp data when `YELP_API_KEY` is set. Pass `--no-yelp` to skip this step.
Use `--strict-zips` to drop any fetched rows whose `Zip Code` isn't in the
provided list. This is useful when Google returns nearby results outside the
desired ZIP codes.
The refresh step now also scrapes each restaurant's website to detect Facebook
and Instagram links, adding `facebook_url` and `instagram_url` columns to the
output CSV.

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

`refresh-restaurants` now enriches Google Places rows with Yelp ratings and
categories automatically when `YELP_API_KEY` is set. You can still run
`google_yelp_enrich.py` manually if desired. The script searches Yelp by the
restaurant name and city and scans
up to five candidates. `rapidfuzz.fuzz.token_set_ratio` picks the best match and
only applies it when the score meets the `YELP_MATCH_THRESHOLD` (60 by default).
When that fails the utility fetches the place's phone number from Google and
performs a Yelp phone search. Rows without a valid match are left unchanged and
marked as `FAIL`. The summary section now includes rating, price tier, phone and
closed status in addition to the list of cuisines.

Ensure the `dela.sqlite` database exists (created by `refresh_restaurants.py`)
and that `YELP_API_KEY` is set before running. Without the key the refresh
command will skip enrichment unless you pass `--no-yelp`.

The refresh step also enriches each record with the business owner's name from
Washington's Department of Revenue and participating city license rolls. Set
`WA_APP_TOKEN` and optional `CITY_APP_TOKEN` in your environment to raise the
request limit. Use `--no-wa` to skip this lookup.

Set `YELP_DEBUG=1` to print debug information about failed lookups, including
all Yelp candidate names returned for each query.

Each matched Yelp business includes a list of food categories. Their aliases
(e.g. `pizza`, `italian`) are written to the `yelp_cuisines` column as a
comma‑separated string, with the first alias stored separately as
`yelp_primary_cuisine`.

## Tests

Ensure the `restaurants` package is importable before running the tests.
Running `pytest` without installing dependencies will fail with missing-module
errors. Be sure to run `pip install -r requirements-dev.txt` (or
`./setup_tests.sh`) before executing the test suite. All runtime
dependencies live in `requirements.txt` while the testing tools are listed in
`requirements-dev.txt`:

```bash
pip install -e .
pip install -r requirements-dev.txt
pytest
```

Run `pytest -s` to see logging and progress messages during the suite. The
`-s` flag disables output capturing so `tqdm` progress bars and other prints are
visible.

The dev file includes `pytest`, `mypy` and type stubs. It also pulls in the
packages from `requirements.txt` so a single install command prepares the
environment. For convenience you can run `./setup_tests.sh` which installs the
requirements and runs the test suite automatically.

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

## GeoJSON export

Run the helper script after cleaning the data to create `restaurants.geojson` in
`backend/static`:

```bash
python -m restaurants.export_geojson
```

## React development server (disabled)

The React frontend lives in `frontend/`, but it is currently disabled. If you
want to work on the UI later install dependencies and start the dev server
manually with:

```bash
cd frontend
npm install
npm run dev
```

The map fetches `/static/restaurants.geojson` from the backend and displays the
locations using Mapbox GL JS. Create `frontend/.env.local` with your Mapbox
token:

```bash
VITE_MAPBOX_TOKEN=<your token>
```

`src/App.jsx` reads this value via `import.meta.env.VITE_MAPBOX_TOKEN` when the
dev server runs.

Running `npm start` now only refreshes restaurant data because the frontend is
disabled.

## Makefile

The `Makefile` streamlines the workflow of refreshing data and exporting GeoJSON. Run:

```bash
make all
```

This command refreshes restaurant data and exports `restaurants.geojson`.
