#!/usr/bin/env bash
set -e
python refresh_restaurants.py

# Find the newest Google restaurants CSV if it exists
latest=$(ls -t *_google_restaurants_*.csv 2>/dev/null | head -1)

# Exit early if no new files were created
if [[ -z "$latest" ]]; then
  echo "No *_google_restaurants_*.csv files produced by refresh_restaurants.py" >&2
  exit 1
fi

python loader.py "$latest"
